import argparse

from common.config import STREAM_INPUT_DIR
from producer.producer import main


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Produce one Vendor Payments "
            "streaming input window."
        )
    )

    parser.add_argument(
        "--window",
        type=int,
        required=True,
        help="Streaming window number, for example 1, 2, or 3.",
    )

    return parser.parse_args()


def run() -> None:
    args = parse_args()

    source_file = (
        STREAM_INPUT_DIR
        / (
            "vendor_payments_"
            f"stream_window_{args.window:03d}.csv"
        )
    )

    main(source_file)


if __name__ == "__main__":
    run()