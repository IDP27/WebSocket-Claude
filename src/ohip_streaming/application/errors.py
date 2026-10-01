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
