import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from common.config import (  # noqa: E402
    STREAM_INPUT_DIR,
    STREAM_WINDOW_COUNT,
    STREAM_WINDOW_SIZE,
    VENDOR_PAYMENTS_ETL_SILVER_FILE,
)


def prepare_stream_sample() -> None:
    """Create bounded streaming input windows from Vendor Payments ETL silver output."""

    if not VENDOR_PAYMENTS_ETL_SILVER_FILE.exists():
        raise FileNotFoundError(
            "Vendor Payments ETL silver file not found: "
            f"{VENDOR_PAYMENTS_ETL_SILVER_FILE}"
        )

    STREAM_INPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    total_rows = (
        STREAM_WINDOW_SIZE
        * STREAM_WINDOW_COUNT
    )

    stream_df = pd.read_csv(
        VENDOR_PAYMENTS_ETL_SILVER_FILE,
        nrows=total_rows,
        dtype={
            "purchase_order": "string",
        },
    )

    if len(stream_df) < total_rows:
        raise ValueError(
            "Vendor Payments ETL silver file does not contain "
            f"enough rows. Required: {total_rows:,}, "
            f"available: {len(stream_df):,}."
        )

    for window_number in range(
        1,
        STREAM_WINDOW_COUNT + 1,
    ):
        start = (
            (window_number - 1)
            * STREAM_WINDOW_SIZE
        )
        end = start + STREAM_WINDOW_SIZE

        window_df = stream_df.iloc[
            start:end
        ].copy()

        window_id = (
            f"stream_window_{window_number:03d}"
        )

        now = datetime.now(
            UTC
        ).isoformat()

        window_df.insert(
            0,
            "event_id",
            [
                str(uuid.uuid4())
                for _ in range(len(window_df))
            ],
        )
        window_df.insert(
            1,
            "event_type",
            "vendor_payment_event",
        )
        window_df.insert(
            2,
            "event_timestamp",
            now,
        )
        window_df.insert(
            3,
            "source_system",
            "vendor_payments_etl_silver",
        )
        window_df.insert(
            4,
            "window_id",
            window_id,
        )

        output_file = (
            STREAM_INPUT_DIR
            / (
                "vendor_payments_"
                f"stream_window_{window_number:03d}.csv"
            )
        )

        window_df.to_csv(
            output_file,
            index=False,
        )

        print(
            f"Created streaming window: {output_file}"
        )
        print(
            f"Window ID: {window_id}"
        )
        print(
            f"Rows: {len(window_df):,}"
        )

    print(
        f"Source: {VENDOR_PAYMENTS_ETL_SILVER_FILE}"
    )
    print(
        f"Total rows prepared: {total_rows:,}"
    )


if __name__ == "__main__":
    prepare_stream_sample()