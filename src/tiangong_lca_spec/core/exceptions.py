"""Custom exception hierarchy for the workflow."""

from __future__ import annotations


class SpecCodingError(Exception):
    """Base error for the Tiangong spec coding workflow."""


class FlowSearchError(SpecCodingError):
    """Raised when flow search fails."""


class FlowAlignmentError(SpecCodingError):
    """Raised when exchange-to-flow alignment is not possible."""


class ProcessExtractionError(SpecCodingError):
    """Raised when process data cannot be extracted or normalized."""


class TidasValidationError(SpecCodingError):
    """Raised when TIDAS validation fails."""
