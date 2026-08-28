from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def write_event_to_staging(
    event: dict[str, Any],
    staging_file: Path,
) -> None:
    """Append an accepted streaming event to one staging window file."""
    staging_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    staging_record = {
        **event,
        "dedup_status": "accepted",
        "ingested_at": datetime.now(UTC).isoformat(),
    }

    with staging_file.open(
        "a",
        encoding="utf-8",
    ) as file:
        file.write(
            json.dumps(
                staging_record,
                ensure_ascii=False,
            )
            + "\n"
        )