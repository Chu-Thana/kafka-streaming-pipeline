import json

from common.reporting import (
    build_event_balance_validation,
    build_producer_execution_report,
    build_producer_input_validation,
    build_producer_send_validation,
    build_staging_count_validation,
    build_streaming_summary_report,
    calculate_duplicate_rate,
    count_jsonl_records,
    determine_pipeline_status,
    write_producer_execution_report,
    write_streaming_summary_report,
)


def test_calculate_duplicate_rate():
    duplicate_rate = calculate_duplicate_rate(
        consumed_events=105000,
        rejected_duplicates=5000,
    )

    assert round(duplicate_rate, 4) == 0.0476


def test_calculate_duplicate_rate_returns_zero_when_no_events():
    duplicate_rate = calculate_duplicate_rate(
        consumed_events=0,
        rejected_duplicates=0,
    )

    assert duplicate_rate == 0.0


def test_count_jsonl_records_counts_non_empty_lines(tmp_path):
    staging_file = tmp_path / "staging.jsonl"

    staging_file.write_text(
        '{"event_id": "event-001"}\n'
        '\n'
        '{"event_id": "event-002"}\n',
        encoding="utf-8",
    )

    assert count_jsonl_records(staging_file) == 2


def test_count_jsonl_records_returns_zero_when_file_missing(tmp_path):
    staging_file = tmp_path / "missing.jsonl"

    assert count_jsonl_records(staging_file) == 0


def test_event_balance_validation_passes():
    validation = build_event_balance_validation(
        consumed_events=105000,
        accepted_events=100000,
        rejected_duplicates=5000,
        failed_events=0,
    )

    assert validation == {
        "status": "PASS",
        "expected": 105000,
        "actual": 105000,
    }


def test_event_balance_validation_fails():
    validation = build_event_balance_validation(
        consumed_events=105000,
        accepted_events=99999,
        rejected_duplicates=5000,
        failed_events=0,
    )

    assert validation["status"] == "FAIL"
    assert validation["expected"] == 105000
    assert validation["actual"] == 104999


def test_staging_count_validation_passes():
    validation = build_staging_count_validation(
        accepted_events=100000,
        staging_record_count=100000,
    )

    assert validation == {
        "status": "PASS",
        "expected": 100000,
        "actual": 100000,
    }


def test_staging_count_validation_fails():
    validation = build_staging_count_validation(
        accepted_events=100000,
        staging_record_count=99999,
    )

    assert validation["status"] == "FAIL"
    assert validation["expected"] == 100000
    assert validation["actual"] == 99999


def test_determine_pipeline_status_returns_success():
    status = determine_pipeline_status(
        failed_events=0,
        validation_status="PASS",
    )

    assert status == "success"


def test_determine_pipeline_status_returns_success_with_failures():
    status = determine_pipeline_status(
        failed_events=2,
        validation_status="PASS",
    )

    assert status == "success_with_failures"


def test_determine_pipeline_status_returns_failed():
    status = determine_pipeline_status(
        failed_events=0,
        validation_status="FAIL",
    )

    assert status == "failed"


def test_build_streaming_summary_report_contains_expected_metadata(
    tmp_path,
):
    staging_file = tmp_path / "vendor_payments_staging.jsonl"

    staging_file.write_text(
        "\n".join(
            [
                '{"event_id": "event-001"}',
                '{"event_id": "event-002"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = build_streaming_summary_report(
        consumed_events=3,
        accepted_events=2,
        rejected_duplicates=1,
        failed_events=0,
        large_payment_alerts_sent=1,
        runtime_seconds=12.34567,
        consumer_group="test-consumer-group",
        topic="test-vendor-payments-topic",
        staging_file=staging_file,
    )

    assert report["project"] == "Vendor Payments Kafka Streaming"
    assert report["pipeline_version"] == "1.0.0"
    assert report["execution_scope"] == "consumer"
    assert report["status"] == "success"
    assert report["runtime_seconds"] == 12.346

    assert report["configuration"] == {
        "topic": "test-vendor-payments-topic",
        "consumer_group": "test-consumer-group",
        "dedup_ttl_seconds": report["configuration"][
            "dedup_ttl_seconds"
        ],
    }

    assert report["consumer"] == {
        "events_consumed": 3,
        "accepted_events": 2,
        "rejected_duplicates": 1,
        "failed_events": 0,
        "large_payment_alerts_sent": 1,
    }

    assert (
        report["deduplication"]["architecture_layer_count"]
        == 2
    )
    assert (
        report["deduplication"]["executed_layer_count"]
        == 1
    )
    assert (
        report["deduplication"]["current_stage"]["layer"]
        == 1
    )
    assert (
        report["deduplication"]["current_stage"]["stage"]
        == "kafka_consumer"
    )
    assert (
        report["deduplication"]["current_stage"]["strategy"]
        == "redis_event_id_deduplication"
    )
    assert (
        report["deduplication"]["current_stage"][
            "duplicate_event_count"
        ]
        == 1
    )
    assert (
        report["deduplication"]["current_stage"][
            "observed_duplicate_rate"
        ]
        == 0.3333
    )
    assert (
        report["deduplication"]["downstream_stage"]["layer"]
        == 2
    )
    assert (
        report["deduplication"]["downstream_stage"][
            "included_in_this_execution"
        ]
        is False
    )

    assert report["outputs"]["staging"]["row_count"] == 2
    assert report["outputs"]["staging"]["available"] is True
    assert (
        report["outputs"]["staging"]["file"]
        == str(staging_file)
    )

    assert report["validation"]["event_balance"]["status"] == "PASS"
    assert (
        report["validation"]["staging_record_count"]["status"]
        == "PASS"
    )
    assert report["validation"]["status"] == "PASS"


def test_build_streaming_summary_report_fails_when_staging_count_mismatches(
    tmp_path,
):
    staging_file = tmp_path / "vendor_payments_staging.jsonl"

    staging_file.write_text(
        '{"event_id": "event-001"}\n',
        encoding="utf-8",
    )

    report = build_streaming_summary_report(
        consumed_events=3,
        accepted_events=2,
        rejected_duplicates=1,
        failed_events=0,
        large_payment_alerts_sent=0,
        runtime_seconds=1.5,
        consumer_group="test-consumer-group",
        staging_file=staging_file,
    )

    assert report["status"] == "failed"
    assert (
        report["validation"]["staging_record_count"]["status"]
        == "FAIL"
    )
    assert report["validation"]["status"] == "FAIL"


def test_write_streaming_summary_report_creates_json_file(
    tmp_path,
):
    staging_file = tmp_path / "staging.jsonl"
    report_file = tmp_path / "streaming_pipeline_summary.json"

    staging_file.write_text(
        '{"event_id": "event-001"}\n',
        encoding="utf-8",
    )

    report = build_streaming_summary_report(
        consumed_events=1,
        accepted_events=1,
        rejected_duplicates=0,
        failed_events=0,
        large_payment_alerts_sent=0,
        runtime_seconds=2.25,
        consumer_group="test-consumer-group",
        staging_file=staging_file,
    )

    write_streaming_summary_report(
        report,
        report_file=report_file,
    )

    saved_report = json.loads(
        report_file.read_text(encoding="utf-8")
    )

    assert saved_report["status"] == "success"
    assert saved_report["consumer"]["events_consumed"] == 1
    assert saved_report["outputs"]["staging"]["row_count"] == 1
    assert saved_report["validation"]["status"] == "PASS"


def test_producer_input_validation_passes():
    validation = build_producer_input_validation(
        source_row_count=100000,
        base_event_count=100000,
    )

    assert validation == {
        "status": "PASS",
        "expected": 100000,
        "actual": 100000,
    }


def test_producer_input_validation_fails():
    validation = build_producer_input_validation(
        source_row_count=100000,
        base_event_count=99999,
    )

    assert validation == {
        "status": "FAIL",
        "expected": 100000,
        "actual": 99999,
    }


def test_producer_send_validation_passes():
    validation = build_producer_send_validation(
        events_attempted=105000,
        events_acknowledged=105000,
        failed_events=0,
    )

    assert validation == {
        "status": "PASS",
        "expected": 105000,
        "actual": 105000,
    }


def test_producer_send_validation_passes_with_failed_delivery():
    validation = build_producer_send_validation(
        events_attempted=105000,
        events_acknowledged=104998,
        failed_events=2,
    )

    assert validation == {
        "status": "PASS",
        "expected": 105000,
        "actual": 105000,
    }


def test_producer_send_validation_fails_when_outcomes_do_not_balance():
    validation = build_producer_send_validation(
        events_attempted=105000,
        events_acknowledged=104998,
        failed_events=1,
    )

    assert validation == {
        "status": "FAIL",
        "expected": 105000,
        "actual": 104999,
    }


def test_build_producer_execution_report_contains_expected_metadata(
    tmp_path,
):
    source_file = tmp_path / "vendor_payments_stream_sample.csv"

    source_file.write_text(
        "event_id\n",
        encoding="utf-8",
    )

    report = build_producer_execution_report(
        source_file=source_file,
        source_row_count=100000,
        base_event_count=100000,
        duplicate_events_injected=5000,
        events_attempted=105000,
        events_acknowledged=105000,
        failed_events=0,
        runtime_seconds=18.76543,
        duplicate_rate_configured=0.05,
        topic="test-vendor-payments-topic",
    )

    assert report["project"] == "Vendor Payments Kafka Streaming"
    assert report["pipeline_version"] == "1.0.0"
    assert report["execution_scope"] == "producer"
    assert report["status"] == "success"
    assert report["runtime_seconds"] == 18.765

    assert report["configuration"] == {
        "topic": "test-vendor-payments-topic",
        "duplicate_rate_configured": 0.05,
    }

    assert report["input"] == {
        "source_file": str(source_file),
        "source_row_count": 100000,
        "available": True,
    }

    assert report["producer"] == {
        "base_event_count": 100000,
        "duplicate_events_injected": 5000,
        "events_attempted": 105000,
        "events_acknowledged": 105000,
        "failed_events": 0,
        "observed_duplicate_rate": 0.0476,
    }

    assert report["validation"]["input_event_count"] == {
        "status": "PASS",
        "expected": 100000,
        "actual": 100000,
    }

    assert report["validation"]["send_outcome_balance"] == {
        "status": "PASS",
        "expected": 105000,
        "actual": 105000,
    }

    assert report["validation"]["status"] == "PASS"


def test_build_producer_execution_report_returns_success_with_failures(
    tmp_path,
):
    source_file = tmp_path / "vendor_payments_stream_sample.csv"

    source_file.write_text(
        "event_id\n",
        encoding="utf-8",
    )

    report = build_producer_execution_report(
        source_file=source_file,
        source_row_count=100000,
        base_event_count=100000,
        duplicate_events_injected=5000,
        events_attempted=105000,
        events_acknowledged=104998,
        failed_events=2,
        runtime_seconds=20.0,
        duplicate_rate_configured=0.05,
    )

    assert report["status"] == "success_with_failures"
    assert report["validation"]["status"] == "PASS"


def test_build_producer_execution_report_fails_when_counts_mismatch(
    tmp_path,
):
    source_file = tmp_path / "vendor_payments_stream_sample.csv"

    source_file.write_text(
        "event_id\n",
        encoding="utf-8",
    )

    report = build_producer_execution_report(
        source_file=source_file,
        source_row_count=100000,
        base_event_count=99999,
        duplicate_events_injected=5000,
        events_attempted=104999,
        events_acknowledged=104999,
        failed_events=0,
        runtime_seconds=20.0,
        duplicate_rate_configured=0.05,
    )

    assert report["status"] == "failed"
    assert (
        report["validation"]["input_event_count"]["status"]
        == "FAIL"
    )
    assert report["validation"]["status"] == "FAIL"


def test_write_producer_execution_report_creates_json_file(
    tmp_path,
):
    source_file = tmp_path / "vendor_payments_stream_sample.csv"
    report_file = tmp_path / "producer_execution_summary.json"

    source_file.write_text(
        "event_id\n",
        encoding="utf-8",
    )

    report = build_producer_execution_report(
        source_file=source_file,
        source_row_count=2,
        base_event_count=2,
        duplicate_events_injected=1,
        events_attempted=3,
        events_acknowledged=3,
        failed_events=0,
        runtime_seconds=1.25,
        duplicate_rate_configured=0.5,
    )

    write_producer_execution_report(
        report,
        report_file=report_file,
    )

    saved_report = json.loads(
        report_file.read_text(encoding="utf-8")
    )

    assert saved_report["execution_scope"] == "producer"
    assert saved_report["status"] == "success"
    assert saved_report["producer"]["events_attempted"] == 3
    assert saved_report["producer"]["events_acknowledged"] == 3
    assert saved_report["validation"]["status"] == "PASS"