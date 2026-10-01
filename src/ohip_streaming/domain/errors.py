"""Erros de domínio. Cada um tem um ``code`` estável, usado pela API (docs/API.md)."""

from __future__ import annotations


class DomainError(Exception):
    code = "DOMAIN_ERROR"


class InvalidOffsetError(DomainError):
    code = "OFFSET_INVALID"


class InvalidIdentifierError(DomainError):
    code = "IDENTIFIER_INVALID"


class ReplayError(DomainError):
    code = "REPLAY_INVALID"


class ReplayOffsetInvalidError(ReplayError):
    code = "REPLAY_OFFSET_INVALID"


class ReplayConfirmationError(ReplayError):
    code = "REPLAY_CONFIRMATION_MISMATCH"


class ReplayReasonRequiredError(ReplayError):
    code = "REPLAY_REASON_REQUIRED"


class ReplayForwardNotAllowedError(ReplayError):
    code = "REPLAY_FORWARD_NOT_ALLOWED"
