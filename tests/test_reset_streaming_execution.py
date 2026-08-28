from scripts.reset_streaming_execution import (
    clear_staging_directory,
)


def test_clear_staging_directory_removes_window_files(
    tmp_path,
):
    staging_dir = tmp_path / "staging"

    window_001 = staging_dir / "stream_window_001"
    window_002 = staging_dir / "stream_window_002"

    window_001.mkdir(parents=True)
    window_002.mkdir(parents=True)

    (window_001 / "events.jsonl").write_text(
        '{"event_id": "event-001"}\n',
        encoding="utf-8",
    )

    (window_001 / "_SUCCESS").touch()

    (window_002 / "events.jsonl").write_text(
        '{"event_id": "event-002"}\n',
        encoding="utf-8",
    )

    outside_file = tmp_path / "keep.txt"
    outside_file.write_text(
        "keep",
        encoding="utf-8",
    )

    removed_file_count = clear_staging_directory(
        staging_dir
    )

    assert removed_file_count == 3

    assert staging_dir.exists()
    assert not window_001.exists()
    assert not window_002.exists()

    assert outside_file.exists()


def test_clear_staging_directory_returns_zero_when_missing(
    tmp_path,
):
    staging_dir = tmp_path / "missing-staging"

    removed_file_count = clear_staging_directory(
        staging_dir
    )

    assert removed_file_count == 0
    assert not staging_dir.exists()
