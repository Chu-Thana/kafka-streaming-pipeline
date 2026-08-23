from __future__ import annotations

import logging
import sys
from pathlib import Path

import redis

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from common.config import (  # noqa: E402
    PRODUCER_EXECUTION_REPORT_FILE,
    REDIS_HOST,
    REDIS_PORT,
    STAGING_FILE,
    STREAMING_SUMMARY_REPORT_FILE,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def remove_file(file_path: Path) -> bool:
    """Remove an execution artifact when it exists."""
    if not file_path.exists():
        logger.info(
            "Artifact does not exist, skipping | file=%s",
            file_path,
        )
        return False

    file_path.unlink()

    logger.info(
        "Removed execution artifact | file=%s",
        file_path,
    )

    return True


def clear_streaming_dedup_keys(
    redis_host: str = REDIS_HOST,
    redis_port: int = REDIS_PORT,
) -> int:
    """Delete only Vendor Payments streaming deduplication keys from Redis."""
    client = redis.Redis(
        host=redis_host,
        port=redis_port,
        decode_responses=True,
    )

    deleted_key_count = 0

    for key in client.scan_iter(match="event:*"):
        deleted_key_count += client.delete(key)

    logger.info(
        "Cleared Redis deduplication keys | pattern=event:* count=%s",
        deleted_key_count,
    )

    return deleted_key_count


def reset_streaming_execution() -> dict[str, int]:
    """Reset local artifacts and Redis state before a clean execution."""
    removed_file_count = 0

    execution_files = [
        STAGING_FILE,
        PRODUCER_EXECUTION_REPORT_FILE,
        STREAMING_SUMMARY_REPORT_FILE,
    ]

    for file_path in execution_files:
        if remove_file(file_path):
            removed_file_count += 1

    deleted_redis_key_count = clear_streaming_dedup_keys()

    summary = {
        "removed_file_count": removed_file_count,
        "deleted_redis_key_count": deleted_redis_key_count,
    }

    logger.info(
        "Streaming execution reset completed | "
        "removed_files=%s deleted_redis_keys=%s",
        removed_file_count,
        deleted_redis_key_count,
    )

    return summary


if __name__ == "__main__":
    reset_streaming_execution()
