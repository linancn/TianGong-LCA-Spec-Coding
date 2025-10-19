"""Logging configuration helpers."""

from __future__ import annotations

import logging

import structlog

from .config import Settings, get_settings


def configure_logging(
    level: str | None = None,
    *,
    settings: Settings | None = None,
) -> None:
    """Configure standard logging, structlog, and LangGraph instrumentation."""
    resolved_settings = settings or get_settings()
    resolved_settings.apply_langsmith_environment()
    effective_level = level or resolved_settings.log_level
    numeric_level = getattr(logging, effective_level.upper(), logging.INFO)

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            level=numeric_level,
        )
    root_logger.setLevel(numeric_level)

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        cache_logger_on_first_use=True,
    )
    _configure_langgraph_logging(numeric_level)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger."""
    return structlog.get_logger(name)


def _configure_langgraph_logging(level: int) -> None:
    """Ensure LangGraph loggers propagate with the configured level."""
    for logger_name in ("langgraph", "langgraph.pregel", "langgraph.graph"):
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.propagate = True
