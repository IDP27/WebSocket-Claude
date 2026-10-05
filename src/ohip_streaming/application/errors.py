"""Erros que os adapters levantam e os casos de uso tratam. Independentes de tecnologia."""

from __future__ import annotations


class ApplicationError(Exception):
    code = "APPLICATION_ERROR"


class LeaseLostError(ApplicationError):
    """A barreira de epoch falhou: outro processo assumiu (ADR-0008). Parar de gravar e sair."""

    code = "LEASE_LOST"


class StoreUnavailableError(ApplicationError):
    """Banco inacessível (rede, sessão, instância fora), sem recursos (tablespace, undo) ou
    falha sistêmica detectada pelo disjuntor do lote. Nada foi gravado."""

    code = "DEPENDENCY_UNAVAILABLE"


class BatchFailedError(ApplicationError):
    """O banco respondeu, mas o lote inteiro falhou (erro que não é por linha). Nada foi gravado."""

    code = "BATCH_FAILED"


class BrokerMisconfiguredError(ApplicationError):
    """A topologia no broker diverge da nossa (argumentos diferentes numa declaração).
    Exige ação humana; tentar de novo não resolve."""

    code = "BROKER_MISCONFIGURED"


class StoreOperationError(ApplicationError):
    """O banco respondeu e recusou uma operação que não é o lote do consumer (ex.: marcação da
    outbox, pedido de replay). Nada foi gravado."""

    code = "STORE_ERROR"


class BrokerUnavailableError(ApplicationError):
    """Conexão com o broker perdida (broker fora, rede). Nunca conta tentativa (ADR-0002)."""

    code = "BROKER_UNAVAILABLE"


class ChannelClosedError(BrokerUnavailableError):
    """O broker fechou o canal ao publicar esta mensagem, com a conexão de pé (ex.:
    PRECONDITION_FAILED). Repetido na mesma linha, conta como falha da mensagem (ADR-0002)."""

    code = "BROKER_CHANNEL_CLOSED"


class NotFoundError(ApplicationError):
    code = "NOT_FOUND"


class UnknownChainError(NotFoundError):
    code = "CHAIN_NOT_FOUND"


class ReplayAlreadyPendingError(ApplicationError):
    code = "REPLAY_ALREADY_PENDING"


class ReplayNotPendingError(ApplicationError):
    code = "REPLAY_NOT_PENDING"


class InvalidOperationError(ApplicationError):
    """O estado atual não permite a ação (item já resolvido, retry já pedido, evento IGNORED).
    Na API vira 409."""

    code = "INVALID_STATE"


class AuthRejectedError(ApplicationError):
    """O OAuth recusou as credenciais (400/401/403). Não repetir em seguida: alerta e espera
    a correção da configuração ou da assinatura no Developer Portal."""

    code = "AUTH_REJECTED"


class AuthUnavailableError(ApplicationError):
    """Gateway do OAuth fora, lento, 429 ou 5xx. Repetir com backoff."""

    code = "AUTH_UNAVAILABLE"


class LeaseNotProvisionedError(ApplicationError):
    """Não existe linha em OHIP_LEASE para o recurso: erro de provisionamento (ADR-0008)."""

    code = "LEASE_NOT_PROVISIONED"


class ConnectionClosedError(ApplicationError):
    """O WebSocket fechou. ``code`` é o código de fechamento (None = queda sem código)."""

    code = "WS_CLOSED"

    def __init__(self, close_code: int | None, reason: str = "") -> None:
        super().__init__(f"conexão fechada ({close_code}): {reason}".strip())
        self.close_code = close_code
        self.reason = reason


class MessageTooLargeError(ConnectionClosedError):
    """Mensagem acima de ``OHIP_WS_MAX_MESSAGE_BYTES`` (o cliente fechou com 1009)."""

    code = "WS_MESSAGE_TOO_LARGE"


class ResourceUnavailableError(ApplicationError):
    """REST do OHIP fora, lenta, 5xx, 429 ou 401 repetido: falha de infraestrutura, não da
    mensagem. Não conta tentativa (ADR-0019). ``retry_after_s``: pausa pedida pelo servidor."""

    code = "RESOURCE_UNAVAILABLE"

    def __init__(self, message: str, *, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class ResourceRejectedError(ApplicationError):
    """A REST recusou o pedido deste evento (4xx), ou o evento não tem o necessário para a
    chamada (ex.: sem hotelId). Falha da mensagem: conta tentativa (ADR-0019)."""

    code = "RESOURCE_REJECTED"


class RuleError(ApplicationError):
    """Uma regra de normalização recusou o evento (RF-08). O texto vai para o log e para a DLQ
    (API e painel): **nunca** coloque nele valores do evento, só nomes de campos e motivos."""

    code = "RULE_ERROR"


OMITTED_DETAIL = "detalhe omitido (pode conter dados pessoais)"


class EnrichmentFailedError(ApplicationError):
    """Falha de uma mensagem no enricher, já classificada pelo estágio da DLQ.

    ``detail`` é o que pode ir para log e DLQ: o texto dos nossos erros (``ApplicationError``,
    escritos sem dados pessoais) ou, para qualquer outra exceção (ex.: ``KeyError`` de uma
    regra com o valor do campo), só o aviso de omissão."""

    code = "ENRICHMENT_FAILED"

    def __init__(self, raw_event_id: int, stage: str, cause: BaseException) -> None:
        detail = (
            (str(cause) or cause.code) if isinstance(cause, ApplicationError) else OMITTED_DETAIL
        )
        super().__init__(f"{type(cause).__name__}: {detail}")
        self.raw_event_id = raw_event_id
        self.stage = stage
        self.cause = cause
        self.detail = detail


class HandshakeRejectedError(ApplicationError):
    """O gateway recusou o upgrade (ex.: HTTP 400 por chave ou URL errada): configuração."""

    code = "WS_HANDSHAKE_REJECTED"
