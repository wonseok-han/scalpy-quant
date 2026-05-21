import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import structlog

_configured = False


def setup_logging(log_dir: str = "logs", level: str = "DEBUG") -> None:
    global _configured
    if _configured:
        return
    _configured = True

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    log_level = getattr(logging, level.upper(), logging.DEBUG)

    file_handler = TimedRotatingFileHandler(
        log_path / "scalpy.log",
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.suffix = "%Y-%m-%d"

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)

    logging.basicConfig(
        format="%(message)s",
        level=log_level,
        handlers=[console_handler, file_handler],
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(),
        ],
    )
    console_handler.setFormatter(formatter)

    json_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
    )
    file_handler.setFormatter(json_formatter)
