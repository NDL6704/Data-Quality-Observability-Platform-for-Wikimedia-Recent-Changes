"""Structured logging configuration for pipeline services and CLI tools."""

import logging
import sys


def setup_logger(
    name: str = "wikimedia_dq",
    level: str | None = None,
) -> logging.Logger:
    """Configures and returns a standardized logger."""
    # Đảm bảo Windows console tự động hỗ trợ UTF-8 cho log tiếng Việt
    if hasattr(sys.stdout, "reconfigure") and sys.stdout.encoding:
        if sys.stdout.encoding.lower() != "utf-8":
            try:
                sys.stdout.reconfigure(encoding="utf-8")
            except Exception:
                pass

    logger = logging.getLogger(name)

    if not logger.handlers:
        log_level = level or "INFO"
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
