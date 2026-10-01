"""Máscara de dados pessoais (LGPD, RNF-08) e de números de cartão no ``detail`` do evento.

Aplicada em tudo que sai do Oracle: logs, API, export e mensagem da fila (ARCHITECTURE §6, §11).

Regras (padrões tirados dos ``elementName`` do guia Oracle, até a resposta da Q-9):
1. Módulos "negar por padrão" (``PROFILE``): todo valor é mascarado, exceto elementos de uma
   lista de seguros (indicadores, tipos, códigos).
2. Demais módulos: valores mascarados quando o nome casa com um padrão de dado pessoal
   (nomes, endereço, documentos, contato, nascimento, campos livres, UDF de texto) ou de cartão.
3. Em qualquer valor, um número de cartão que passa no Luhn é mascarado e marca
   ``card_data_detected`` (alerta: nunca deveríamos receber número de cartão). Formatos
   reconhecidos: 13 a 19 dígitos seguidos (12 em elementos de cartão, por causa do Maestro), ou
   grupos com o mesmo separador (1 a 3 caracteres entre espaço, NBSP, quebra de linha, ponto,
   hífen e barra) no padrão de cartão: 4-4-4-4, 4-4-4-4-1..3, 4-6-4, 4-6-5. Números vizinhos
   (CVV, validade), separados ou colados (17 a 23 dígitos cujo prefixo de 16 ou 15 passa no
   Luhn), não impedem a detecção. Datas e outros agrupamentos (ex.: ``2026-10-10``) não casam.
4. Elementos de **número estruturado** que não é cartão (fidelidade, telefone, documento,
   confirmação, ids: ``_STRUCTURED_NUMBER_ELEMENTS``) nunca são examinados: cartões de
   fidelidade usam Luhn de propósito, e o falso positivo alteraria o bruto e geraria alerta
   falso. Esses valores são mascarados nas saídas pelo nome quando são pessoais.

``scrub_card_numbers_*`` remove números de cartão do que é **gravado** no Oracle (payload do
evento e mensagem da DLQ): o PRD proíbe armazenar dado de cartão (ADR-0011). A limpeza é
estrutural sempre que o texto é JSON: só valores são examinados (strings, números e estruturas
aninhadas), nunca os campos de identificação (offset, ids, chaves).
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from itertools import pairwise
from typing import Final, TypeAlias

from ohip_streaming.domain.events import Event, EventDetail
from ohip_streaming.domain.identifiers import normalize_event_name

MASK: Final = "***"

# Nomes normalizados (maiúsculas, espaços simples). Padrões casam o nome inteiro.
_PERSONAL_PATTERNS: Final = tuple(
    re.compile(pattern)
    for pattern in (
        r"X?(FIRST |LAST |MIDDLE |ALTERNATE |FULL |GUEST |MAIDEN )?NAME\d*",  # NAME, NAME2, XNAME
        r"MIDDLE",
        r"INCOGNITO( .*)?",
        r"ADDRESS( LINE)? ?\d*",  # ADDRESS, ADDRESS1, ADDRESS LINE 2
        r"STREET.*",
        r"CITY",
        r"POSTAL CODE|ZIP( CODE)?",
        r"TAX (NUMBER|ID)\d*|VAT( ID| NUMBER)?\d*",
        r"ID (NUMBER|PLACE|DATE|EXPIRATION DATE|COUNTRY)",
        r"(IDENTIFICATION|DOCUMENT|PASSPORT)( NUMBER)?",
        r"EMAIL( ADDRESS)?",
        r"(PHONE|MOBILE|FAX)( NUMBER)?|PAGER",
        r"BIRTH (DATE|PLACE|COUNTRY)|DATE OF BIRTH",
        r"NATIONALITY|GENDER",
        r"MEMBERSHIP (NUMBER|CARD NUMBER)",
        r"COMMENTS?|NOTES?|REMARKS?",
        r"UDF CHAR ?\d*",
        r"WEBPAGE|VIRTUAL NUMBER",
        r"X?(TITLE|SALUTATION)|BUSINESS TITLE|PROFESSION",
        r"CREDIT CARD( .*)?|CARD NUMBER|CC NUMBER|CVV|CVC",  # mascarado, mas não é alerta
    )
)

# Em PROFILE, só estes passam em claro (não identificam a pessoa).
DEFAULT_PROFILE_SAFE_ELEMENTS: Final = frozenset(
    {
        "ACCOUNT TYPE",
        "ADDRESS LANGUAGE",
        "ADDRESS PRIMARY YN",
        "ADDRESS TYPE",
        "ANONYMIZATION DATE",
        "ANONYMIZATION STATUS",
        "AUTO ENROLL MEMBERSHIP OPT IN FLG",
        "AUTO ENROLL MEMBERSHIP YN",
        "BIRTH DATE CHANGED YN",
        "CHAIN CODE",
        "COMPANY TYPE",
        "CONTACT YN",
        "CREDIT RATING",
        "EMAIL LANGUAGE",
        "EMAIL OPT IN FLG",
        "EMAIL YN",
        "GUEST PRIVACY OPT IN FLG",
        "GUEST PRIVACY YN",
        "IATA TYPE",
        "ID TYPE",
        "INDUSTRY CODE",
        "KEYWORD TYPE",
        "LANGUAGE",
        "MAILING LIST OPT IN FLG",
        "MAILING LIST YN",
        "MARKET RESEARCH OPT IN FLG",
        "MARKET RESEARCH YN",
        "MEMBERSHIP LEVEL",
        "MEMBERSHIP TYPE",
        "NAME TYPE",
        "PHONE OPT IN FLG",
        "PHONE PRIMARY YN",
        "PHONE TYPE",
        "PHONE YN",
        "PREFERENCE GROUP",
        "PREFERENCE TYPE",
        "PREFERENCE TYPE DESCRIPTION",
        "PRIORITY",
        "RATE CODE",
        "RESORT LOGGED",
        "RESORT REGISTERED",
        "SCOPE",
        "SMS OPT IN FLG",
        "SMS YN",
        "STATUS",
        "THIRD PARTY OPT IN FLG",
        "THIRD PARTY YN",
        "VIP STATUS",
    }
)

# Sequência de grupos de dígitos com separadores curtos (``\s`` cobre NBSP e quebra de linha).
_DIGIT_GROUPS_RE: Final = re.compile(r"\d+(?:[\s./-]{1,3}\d+)*")
_DIGIT_RE: Final = re.compile(r"\d+")
# Atalho: todo cartão tem pelo menos 12 dígitos com, no máximo, 3 separadores entre eles.
_MAYBE_CARD_RE: Final = re.compile(r"\d(?:[\s./-]{0,3}\d){11}")
_GLUED_PREFIXES: Final = (16, 15)  # PAN colado ao CVV ou à validade
_GLUED_MAX_DIGITS: Final = 23
_GROUPED_CARD_SHAPES: Final = (
    (4, 4, 4, 4),
    (4, 4, 4, 4, 1),
    (4, 4, 4, 4, 2),
    (4, 4, 4, 4, 3),
    (4, 6, 4),
    (4, 6, 5),
)
CARD_MIN_DIGITS: Final = 13
CARD_ELEMENT_MIN_DIGITS: Final = 12  # Maestro; só em elementos com nome de cartão
_CARD_ELEMENT_RE: Final = re.compile(r"CREDIT CARD( .*)?|CARD NUMBER|CC NUMBER")
# Números estruturados que não são cartão (nomes do guia Oracle; ver ADR-0011 §6). Fora da
# lista de propósito: campos que podem receber cartão digitado (ACCOUNT NUMBER em pagamento e
# roteamento, EXTERNAL REFERENCE, que é texto livre) continuam examinados.
_STRUCTURED_NUMBER_ELEMENTS: Final = re.compile(
    "|".join(
        (
            r"MEMBERSHIP( CARD)? (NUMBER|NO)|MEMBERSHIP DEVICE CODE",
            r"BARCODE",
            r"(DATABASE|NAME|PROFILE|RESV NAME|RESERVATION|CONTACT|ACCOUNT) ID",
            r"(CONFIRMATION|CANCELLATION)( NO| NUMBER)",
            r"(.* )?A/?R NUMBER",
            r"(COMPANY|IATA|TRAVEL AGENT|SOURCE) (NUMBER|NO|CODE)",
            r"(PHONE|MOBILE|FAX)( NUMBER)?|PAGER|VIRTUAL NUMBER",
            r"TAX (NUMBER|ID)\d*|VAT( ID| NUMBER)?\d*",
            r"ID NUMBER|PASSPORT( NUMBER)?|DOCUMENT( NUMBER)?",
        )
    )
)

# Campos de identificação do frame/evento: nunca examinados (podem ter 13+ dígitos).
_IDENTIFIER_KEYS: Final = frozenset(
    {
        "id",
        "type",
        "offset",
        "uniqueEventId",
        "primaryKey",
        "chainCode",
        "hotelId",
        "publisherId",
        "actionInstanceId",
        "timestamp",
        "moduleName",
        "eventName",
        "elementName",
    }
)

JsonValue: TypeAlias = bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"] | None


def _luhn_ok(digits: str) -> bool:
    total = 0
    for index, char in enumerate(reversed(digits)):
        value = int(char)
        if index % 2 == 1:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return total % 10 == 0


def _card_end(
    groups: list[re.Match[str]], separators: list[str], start: int, min_digits: int
) -> int | None:
    """Índice (exclusivo) do último grupo do cartão que começa em ``start``, ou None.

    Tenta do candidato mais longo para o mais curto: um CVV logo depois de um 4-4-4-4 não
    impede a detecção (o 4-4-4-4-3 falha no Luhn e o 4-4-4-4 é testado em seguida). Num grupo
    contínuo longo demais, testa os prefixos de 16 e 15 dígitos (PAN colado ao CVV/validade);
    o grupo inteiro é mascarado.
    """
    first = groups[start].group(0)
    candidates: list[tuple[str, int]] = []  # (dígitos testados no Luhn, fim)
    if min_digits <= len(first) <= 19:
        candidates.append((first, start + 1))
    if 16 < len(first) <= _GLUED_MAX_DIGITS:
        candidates.extend((first[:size], start + 1) for size in _GLUED_PREFIXES)
    for shape in _GROUPED_CARD_SHAPES:
        end = start + len(shape)
        if end > len(groups) or len(set(separators[start : end - 1])) != 1:
            continue
        if all(len(groups[start + i].group(0)) == size for i, size in enumerate(shape)):
            candidates.append(("".join(g.group(0) for g in groups[start:end]), end))
    for digits, end in sorted(candidates, key=lambda c: (len(c[0]), c[1]), reverse=True):
        if _luhn_ok(digits):
            return end
    return None


def scrub_card_numbers_in_text(
    value: str, *, min_digits: int = CARD_MIN_DIGITS
) -> tuple[str, bool]:
    """Troca por ``***`` todo número de cartão (formato reconhecido + Luhn) do texto livre."""
    if not _MAYBE_CARD_RE.search(value):
        return value, False
    spans: list[tuple[int, int]] = []
    for run in _DIGIT_GROUPS_RE.finditer(value):
        groups = list(_DIGIT_RE.finditer(value, run.start(), run.end()))
        separators = [value[a.end() : b.start()] for a, b in pairwise(groups)]
        index = 0
        while index < len(groups):
            end = _card_end(groups, separators, index, min_digits)
            if end is None:
                index += 1
                continue
            spans.append((groups[index].start(), groups[end - 1].end()))
            index = end
    if not spans:
        return value, False
    parts: list[str] = []
    cursor = 0
    for start, end in spans:
        parts.extend((value[cursor:start], MASK))
        cursor = end
    parts.append(value[cursor:])
    return "".join(parts), True


def _element_min_digits(element_name: object, default: int | None) -> int | None:
    """Tamanho mínimo de cartão a procurar nos valores de um elemento; None = não examinar."""
    if not isinstance(element_name, str):
        return default
    name = normalize_event_name(element_name)
    if _STRUCTURED_NUMBER_ELEMENTS.fullmatch(name):
        return None
    if _CARD_ELEMENT_RE.fullmatch(name):
        return CARD_ELEMENT_MIN_DIGITS
    return default


def _scrub_json(
    value: JsonValue, *, skip_identifiers: bool, min_digits: int = CARD_MIN_DIGITS
) -> tuple[JsonValue, bool]:
    """Limpa recursivamente os valores de uma estrutura JSON.

    Número JSON que é um cartão vira a string ``***``. Com ``skip_identifiers``, as chaves de
    ``_IDENTIFIER_KEYS`` são mantidas como vieram. Dentro de um item do ``detail``, o
    ``elementName`` decide: cartão → a partir de 12 dígitos; número estruturado → não examina.
    """
    if isinstance(value, str):
        return scrub_card_numbers_in_text(value, min_digits=min_digits)
    if isinstance(value, bool) or value is None:
        return value, False
    if isinstance(value, int | float):
        text, found = scrub_card_numbers_in_text(json.dumps(value), min_digits=min_digits)
        return (text if found else value), found
    found = False
    if isinstance(value, list):
        items: list[JsonValue] = []
        for item in value:
            clean, item_found = _scrub_json(
                item, skip_identifiers=skip_identifiers, min_digits=min_digits
            )
            items.append(clean)
            found |= item_found
        return items, found
    if "elementName" in value:
        element_min = _element_min_digits(value["elementName"], min_digits)
        if element_min is None:
            return value, False
        min_digits = element_min
    mapping: dict[str, JsonValue] = {}
    for key, item in value.items():
        if skip_identifiers and key in _IDENTIFIER_KEYS:
            mapping[key] = item
            continue
        mapping[key], item_found = _scrub_json(
            item, skip_identifiers=skip_identifiers, min_digits=min_digits
        )
        found |= item_found
    return mapping, found


def _dumps(value: JsonValue) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def scrub_card_numbers_in_message(raw: str) -> tuple[str, bool]:
    """Limpa uma mensagem bruta que vai para a DLQ (frame ``next``, ``newEvent`` ou lixo).

    JSON válido: limpeza estrutural, sem tocar nos campos de identificação, e o texto só é
    reescrito se algo foi trocado. Texto que não é JSON: limpeza textual.
    """
    if not _MAYBE_CARD_RE.search(raw):  # atalho: nenhum candidato no texto inteiro
        return raw, False
    try:
        data: JsonValue = json.loads(raw)
    except ValueError:
        return scrub_card_numbers_in_text(raw)
    clean, found = _scrub_json(data, skip_identifiers=True)
    return (_dumps(clean) if found else raw), found


def _scrub_optional(
    value: str | None, min_digits: int | None = CARD_MIN_DIGITS
) -> tuple[str | None, bool]:
    if not value or min_digits is None:
        return value, False
    return scrub_card_numbers_in_text(value, min_digits=min_digits)


def scrub_card_numbers_in_event(event: Event) -> tuple[Event, bool]:
    """Remove números de cartão dos valores do ``detail`` e do ``payload_json`` gravado.

    O ``newEvent`` inteiro é examinado, exceto os campos de identificação (offset, ids e
    chaves, que podem ter 13+ dígitos), em qualquer formato de valor: string, número JSON ou
    estrutura aninhada; campos novos que o schema ganhar também são cobertos. O payload
    continua sendo o JSON recebido, exceto pelos valores trocados.
    """
    found = False
    items: list[EventDetail] = []
    for item in event.detail:
        size = _element_min_digits(item.element_name, CARD_MIN_DIGITS)
        old, old_found = _scrub_optional(item.old_value, size)
        new, new_found = _scrub_optional(item.new_value, size)
        scope_from, from_found = _scrub_optional(item.scope_from, size)
        scope_to, to_found = _scrub_optional(item.scope_to, size)
        found |= old_found or new_found or from_found or to_found
        items.append(
            replace(item, old_value=old, new_value=new, scope_from=scope_from, scope_to=scope_to)
        )

    payload_json, payload_found = scrub_card_numbers_in_message(event.payload_json)
    found |= payload_found
    if not found:
        return event, False
    return replace(event, detail=tuple(items), payload_json=payload_json), True


@dataclass(frozen=True, slots=True)
class MaskedDetail:
    items: tuple[EventDetail, ...]
    card_data_detected: bool


@dataclass(frozen=True, slots=True)
class MaskingPolicy:
    extra_personal_elements: frozenset[str] = frozenset()
    deny_by_default_modules: frozenset[str] = frozenset({"PROFILE"})
    profile_safe_elements: frozenset[str] = field(default=DEFAULT_PROFILE_SAFE_ELEMENTS)

    @classmethod
    def with_extra_elements(cls, names: Iterable[str]) -> MaskingPolicy:
        return cls(extra_personal_elements=frozenset(normalize_event_name(n) for n in names))

    def is_sensitive(self, element_name: str, module_name: str) -> bool:
        name = normalize_event_name(element_name)
        if normalize_event_name(module_name) in self.deny_by_default_modules:
            return name not in self.profile_safe_elements
        if name in self.extra_personal_elements:
            return True
        return any(pattern.fullmatch(name) for pattern in _PERSONAL_PATTERNS)

    def mask_detail(self, detail: Iterable[EventDetail], module_name: str) -> MaskedDetail:
        """``module_name`` é obrigatório: sem ele a regra "negar por padrão" (PROFILE) se perde."""
        card_detected = False
        items: list[EventDetail] = []
        for item in detail:
            old: str | None
            new: str | None
            size = _element_min_digits(item.element_name, CARD_MIN_DIGITS)
            old, old_card = _scrub_optional(item.old_value, size)
            new, new_card = _scrub_optional(item.new_value, size)
            # Sinaliza só número completo de cartão (Luhn), não o nome do campo.
            card_detected |= old_card or new_card
            if self.is_sensitive(item.element_name, module_name):
                old = MASK if item.old_value else item.old_value
                new = MASK if item.new_value else item.new_value
            items.append(replace(item, old_value=old, new_value=new))
        return MaskedDetail(items=tuple(items), card_data_detected=card_detected)
