"""Hourly, independent poll-feed monitoring (:25 UTC, no matcher dependency).

Environment controls: POLLER_HEALTH_DURATION_MINUTES (15),
POLLER_HEALTH_COVERAGE_FRACTION (0.8 minimum), POLLER_HEALTH_THRESHOLD (0.5),
POLLER_HEALTH_LOOKBACK_DAYS (28, bounded to 21..28), POLLER_HEALTH_MINIMUM_FLEET
(20), and optional POLLER_HEALTH_WEBHOOK_URL (HTTPS). Baselines require three
same-weekday samples per minute; raw-GPS seeds (dag_poller_health_seed) stand in
for summaries from before collection began.
Source paths: POLLER_HEALTH_GCS_PREFIX (health/poller/hourly) and
POLLER_HEARTBEAT_GCS_PATH (health/poller/latest.json), both stripped and nonempty.

Hourly reports are the durable evaluation and alert outbox. Retries reuse them,
not a mutable wall-clock latest snapshot. Warehouse rows, public history and alert
delivery are parallel tasks after persistence; BigQuery failure gates neither.
Missing/insufficient telemetry has separate monitoring_gap/monitoring_restored
transitions, never a claim of zero feed or fleet recovery. Alert delivery is at-least-once; the
receiver must deduplicate event_id. delivered_at acknowledges only successful
HTTP delivery; optional logged_at acknowledges a disabled-webhook log instead.
Log-acknowledged events are not sent retrospectively when the webhook is enabled.
The independent alert task replays the last 48 report hours, oldest first, without
copying events. Older pending events stay in their originating report and require
manual deliver_alerts(bucket, hour, webhook_url) replay before archive expiry.

After missed evaluations, bounded report-name metadata over the preceding 90 days
locates the newest durable evaluation independently of BigQuery/publication.
Its active incidents/history survive, but minute tails and the preceding summary
are discarded: unevaluated hours cannot prove continuous degradation/recovery.
Backfill discovery excludes report keys at/after the target hour.

After inspecting recent metrics, an operator may use DAG run conf
{"rebaseline_modes": ["bus"]} (bus/tram, unique) on a new chronological hour.
Only active low_fleet/no_accepted incidents can be administratively rebaselined.
A rebaseline event, not recovery, records the action; a private per-mode epoch
excludes pre-reset baseline samples. Matching frozen-action retries resume safely;
other existing reports/backfills reject reset requests. Rebaselining rewrites no
historical report. Acknowledging a pending alert in a version 1 report (from the
retired stale-share rules) saves that report as version 2, dropping its hourly
baseline; incidents and events are kept.
Publication writes the public status-page history (poller_health_public) after
each evaluation. Configure bucket lifecycle retention: hourly summaries >=28
days, reports >=90 days. Do not expire latest.json or anything under public/.
"""

from __future__ import annotations

import json
import logging
import os
import re
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import requests
from airflow.sdk import DAG, get_current_context, task
from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery, storage
from poller_health import (
    METRIC_FIELDS,
    MODES,
    REPORT_MAX_BYTES,
    SEED_MAX_BYTES,
    Config,
    baseline,
    comparable_hours,
    completed_hour,
    evaluate,
    hour_path,
    iso,
    snapshot,
    timestamp,
    upgrade_report,
    validate_report,
    validate_reset_modes,
    validate_seed,
)
from poller_health_gcs import (
    collection_start,
    first_collection_start,
    read_object,
    read_summary,
)
from poller_health_public import HISTORY_HOURS, HISTORY_MAX_BYTES, HISTORY_PATH, LOOKAHEAD_HOURS, feed_history
from ztm_airflow_common import (
    AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    BIGQUERY_LOCATION,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    airflow_failure_alert,
)

LOGGER = logging.getLogger(__name__)
RAW_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_poller_hourly_health"
RAW_RETENTION_DAYS = 90
OUTBOX_RETRY_HOURS = 48
REPORT_PREFIX = "health/poller/reports/"
REPORT_LOOKBACK_DAYS = 90
REPORT_LIST_LIMIT = REPORT_LOOKBACK_DAYS * 24 + 1
REPORT_LIST_PAGE_SIZE = 1024
RAW_FIELDS = {
    "version": "INTEGER",
    "evaluated_at": "TIMESTAMP",
    "hour_start": "TIMESTAMP",
    "collection_started_at": "TIMESTAMP",
    "mode": "STRING",
    "status": "STRING",
    "reasons": "STRING",
    "intervals": "STRING",
    "monitored_minutes": "INTEGER",
    "baseline_samples": "INTEGER",
    **{name: "FLOAT" if name.startswith("mean_") else "INTEGER" for name in METRIC_FIELDS},
}


def config_from_env() -> Config:
    """Read bounded controls at task runtime, not during DAG parsing."""
    return Config(
        duration_minutes=int(os.getenv("POLLER_HEALTH_DURATION_MINUTES", "15")),
        coverage_fraction=float(os.getenv("POLLER_HEALTH_COVERAGE_FRACTION", "0.8")),
        threshold=float(os.getenv("POLLER_HEALTH_THRESHOLD", "0.5")),
        lookback_days=int(os.getenv("POLLER_HEALTH_LOOKBACK_DAYS", "28")),
        minimum_fleet=int(os.getenv("POLLER_HEALTH_MINIMUM_FLEET", "20")),
    )


def write_object(  # noqa: PLR0913 - bounds and cache policy are per object
    bucket: storage.Bucket,
    path: str,
    data: dict[str, Any],
    generation: int,
    *,
    limit: int = REPORT_MAX_BYTES,
    cache_control: str | None = None,
) -> int:
    """Generation guards protect report/outbox updates from overlapping manual runs."""
    payload = json.dumps(data, sort_keys=True, allow_nan=False, separators=(",", ":"))
    if len(payload.encode()) > limit:
        raise ValueError("evaluated report exceeds bound")
    blob = bucket.blob(path)
    if cache_control:
        blob.cache_control = cache_control
    blob.upload_from_string(payload, content_type="application/json", if_generation_match=generation)
    return int(blob.generation)


def read_seed(bucket: storage.Bucket, hour: datetime) -> dict[str, Any] | None:
    """Pre-collection baseline evidence; a bad seed is skipped, not trusted."""
    try:
        data, _ = read_object(bucket, hour_path("baseline-seed", hour), SEED_MAX_BYTES)
        return validate_seed(data, hour) if data is not None else None
    except ValueError:
        LOGGER.warning("Invalid poller baseline seed for %s", iso(hour))
        return None


def read_sample(bucket: storage.Bucket, hour: datetime) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Return (summary, sample): the summary when collected, else its raw-GPS seed."""
    summary, _ = read_summary(bucket, hour)
    return summary, summary or read_seed(bucket, hour)


def read_report(bucket: storage.Bucket, hour: datetime) -> tuple[dict[str, Any] | None, int]:
    """Reject incompatible/corrupt persisted state rather than invent transitions."""
    report, generation = read_object(bucket, hour_path("reports", hour), REPORT_MAX_BYTES)
    if report is not None:
        report = upgrade_report(report)
        validate_report(report, hour)
    return report, generation


def pending_events(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Log acknowledgement is terminal too; never relabel it as HTTP delivery."""
    return [entry for entry in report["events"] if entry["delivered_at"] is None and entry.get("logged_at") is None]


def warn_outbox_expiry(hour: datetime, count: int) -> None:
    """Never silently discard pending alerts that leave the automatic retry horizon."""
    LOGGER.warning(
        "Pending poller alerts at/outside the %d-hour retry window: hour=%s count=%d report=%s; "
        "if still unacknowledged after this run, manually replay deliver_alerts for this hour before archive expiry",
        OUTBOX_RETRY_HOURS,
        iso(hour),
        count,
        hour_path("reports", hour),
    )


def newest_report_hour(bucket: storage.Bucket, hour: datetime) -> datetime | None:
    """List only bounded name metadata, never summaries/GPS or every report payload."""
    start = hour - timedelta(days=REPORT_LOOKBACK_DAYS)
    blobs = bucket.list_blobs(
        prefix=REPORT_PREFIX,
        start_offset=hour_path("reports", start),
        end_offset=hour_path("reports", hour),
        max_results=REPORT_LIST_LIMIT,
        page_size=REPORT_LIST_PAGE_SIZE,
        fields="items(name),nextPageToken",
    )
    newest = None
    for count, blob in enumerate(blobs, start=1):
        if count >= REPORT_LIST_LIMIT:
            raise ValueError("poller report metadata exceeds bounded lookback; refusing truncated state")
        suffix = blob.name.removeprefix(REPORT_PREFIX)
        if not blob.name.startswith(REPORT_PREFIX) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}/\d{2}\.json", suffix):
            continue
        try:
            candidate = datetime.strptime(suffix, "%Y-%m-%d/%H.json").replace(tzinfo=UTC)
        except ValueError:
            continue
        if start <= candidate < hour and (newest is None or candidate > newest):
            newest = candidate
    return newest


def preceding_report(bucket: storage.Bucket, hour: datetime) -> tuple[dict[str, Any] | None, bool]:
    """Read the preceding hour, or newest durable report discovered by bounded names."""
    previous_hour = hour - timedelta(hours=1)
    report, _ = read_report(bucket, previous_hour)
    if report is not None:
        return report, False
    latest_hour = newest_report_hour(bucket, hour)
    if latest_hour is None:
        return None, False
    report, _ = read_report(bucket, latest_hour)
    if report is None:
        raise ValueError("listed poller report is missing; restore state explicitly")
    if latest_hour < hour - timedelta(hours=OUTBOX_RETRY_HOURS - 1):
        LOGGER.warning(
            "Monitor resumed beyond the %d-hour automatic alert horizon; older reports may contain pending "
            "alerts requiring manual replay before archive expiry (last durable hour=%s)",
            OUTBOX_RETRY_HOURS,
            iso(latest_hour),
        )
        pending = pending_events(report)
        if pending:
            warn_outbox_expiry(latest_hour, len(pending))
    # Internal synthetic predecessor, not a rewritten or fabricated hourly report.
    restored = deepcopy(report)
    restored["hour_start"] = iso(previous_hour)
    restored["events"] = []
    for row in restored["vehicle_types"].values():
        row["state"]["bad_tail"] = []
        row["state"]["good_tail"] = []
        if "monitoring" in row["state"]:
            row["state"]["monitoring"]["gap_tail"] = []
            row["state"]["monitoring"]["good_tail"] = []
    return restored, True


def persist_rows(client: bigquery.Client, report: dict[str, Any]) -> None:
    """Replace just two rows for one hour transactionally; reasons/intervals are JSON STRINGs."""
    if not re.fullmatch(r"[a-zA-Z0-9_-]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+", RAW_TABLE):
        raise ValueError("invalid raw table identifier")
    table = bigquery.Table(RAW_TABLE, schema=[bigquery.SchemaField(name, kind) for name, kind in RAW_FIELDS.items()])
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="hour_start",
        expiration_ms=RAW_RETENTION_DAYS * 24 * 60 * 60 * 1000,
    )
    table.clustering_fields = ["mode"]
    client.create_table(table, exists_ok=True)
    rows = []
    for mode in MODES:
        row = snapshot(report)["vehicle_types"][mode]
        rows.append(
            {
                **{key: report[key] for key in ("version", "evaluated_at", "hour_start", "collection_started_at")},
                "mode": mode,
                **row,
                "reasons": json.dumps(row["reasons"]),
                "intervals": json.dumps(row["intervals"]),
            }
        )
    sql_types = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "TIMESTAMP": "TIMESTAMP", "STRING": "STRING"}
    projections = ",\n".join(
        f"CAST(JSON_VALUE(row, '$.{name}') AS {sql_types[kind]}) AS {name}" for name, kind in RAW_FIELDS.items()
    )
    query = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{RAW_TABLE}` WHERE hour_start = @hour_start;
        INSERT INTO `{RAW_TABLE}` ({", ".join(RAW_FIELDS)})
        SELECT {projections} FROM UNNEST(JSON_QUERY_ARRAY(@rows)) AS row;
        COMMIT TRANSACTION;
    """  # noqa: S608 - identifier validated above; values are query parameters
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("hour_start", "TIMESTAMP", timestamp(report["hour_start"])),
            bigquery.ScalarQueryParameter("rows", "STRING", json.dumps(rows, allow_nan=False)),
        ]
    )
    client.query(query, job_config=job_config, location=BIGQUERY_LOCATION).result()


def lookahead_baselines(
    bucket: storage.Bucket, report: dict[str, Any], config: Config
) -> dict[str, list[dict[str, Any]]]:
    """Baselines for the hours after the evaluated one, so live readers know "usual now"."""
    hour = timestamp(report["hour_start"])
    result: dict[str, list[dict[str, Any]]] = {mode: [] for mode in MODES}
    for offset in range(1, LOOKAHEAD_HOURS + 1):
        candidates = comparable_hours(hour + timedelta(hours=offset), config)
        samples = [sample for candidate in candidates if (sample := read_sample(bucket, candidate)[1])]
        for mode in MODES:
            reset_at = report["vehicle_types"][mode]["state"].get("baseline_reset_at")
            result[mode].append(baseline(samples, mode, config, reset_at=reset_at))
    return result


def optional_report(bucket: storage.Bucket, hour: datetime) -> dict[str, Any] | None:
    """Chart context only: one corrupt past report shows as a gap instead of blocking a day of publication."""
    try:
        return read_report(bucket, hour)[0]
    except ValueError:
        LOGGER.warning("Invalid poller report for %s; shown as unknown in public history", iso(hour))
        return None


def publish_history(bucket: storage.Bucket, report: dict[str, Any], config: Config) -> None:
    """Backfills must not regress the public history or overwrite concurrent writes."""
    latest, generation = read_object(bucket, HISTORY_PATH, HISTORY_MAX_BYTES)
    hour = timestamp(report["hour_start"])
    try:
        newer = latest is not None and timestamp(latest.get("hour_start")) > hour
    except ValueError:
        newer = False  # A corrupt public object is replaced, not trusted.
    if newer:
        return
    previous = [optional_report(bucket, hour - timedelta(hours=offset)) for offset in range(HISTORY_HOURS - 1, 0, -1)]
    history = feed_history(report, previous, lookahead_baselines(bucket, report, config), config)
    # Public readers poll this object; GCS must not serve a cached older copy.
    write_object(bucket, HISTORY_PATH, history, generation, limit=HISTORY_MAX_BYTES, cache_control="no-cache")


def evaluate_report(
    bucket: storage.Bucket,
    hour: datetime,
    config: Config,
    *,
    reset_modes: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Read bounded evidence and restore incident state without bridging evaluation gaps."""
    summary, source_reason = read_summary(bucket, hour)
    previous_summary, _ = read_summary(bucket, hour - timedelta(hours=1))
    previous_report, evaluation_gap = preceding_report(bucket, hour)
    if evaluation_gap:
        previous_summary = None
    samples = []
    collection_markers = []
    heartbeat_start = collection_start(bucket)
    if heartbeat_start:
        collection_markers.append(heartbeat_start)
    for candidate in comparable_hours(hour, config):
        historical, sample = read_sample(bucket, candidate)
        if historical:
            collection_markers.append(historical["collection_started_at"])
        if sample:
            samples.append(sample)
    known_start = bool(
        collection_markers
        or summary
        or previous_summary
        or (previous_report and previous_report.get("collection_started_at"))
    )
    if not known_start:
        first_start = first_collection_start(bucket)
        if first_start:
            collection_markers.append(first_start)
    report = evaluate(
        hour,
        summary,
        samples,
        previous_summary=previous_summary,
        previous_report=previous_report,
        collection_started_at=min(collection_markers, key=timestamp) if collection_markers else None,
        config=config,
        source_reason=source_reason,
        reset_modes=reset_modes,
    )
    if evaluation_gap:
        for row in report["vehicle_types"].values():
            row["reasons"] = sorted({*row["reasons"], "monitoring_evaluation_gap"})
    return report


def ensure_chronological_reset(bucket: storage.Bucket, hour: datetime) -> None:
    """An operator cannot reset an older/publicly frozen slot or supersede newer durable work."""
    latest, _ = read_object(bucket, HISTORY_PATH, HISTORY_MAX_BYTES)
    if latest is not None:
        if type(latest.get("version")) is not int or latest["version"] != 1:
            raise ValueError("invalid poller public history version")
        if timestamp(latest.get("hour_start")) >= hour:
            raise ValueError("rebaseline requires a new chronological hour; published backfills are forbidden")
    # One name suffices to reject any durable report at/after the requested hour,
    # including a newer evaluation whose BigQuery/publication step failed.
    later = bucket.list_blobs(
        prefix=REPORT_PREFIX,
        start_offset=hour_path("reports", hour),
        max_results=1,
        page_size=1,
        fields="items(name),nextPageToken",
    )
    if next(iter(later), None) is not None:
        raise ValueError("rebaseline requires a new chronological hour newer than all durable reports")


def matching_reset_retry(report: dict[str, Any], hour: datetime, modes: tuple[str, ...]) -> bool:
    """Resume only the exact already-persisted operator action, never reset a frozen hour again."""
    applied = {entry["mode"] for entry in report["events"] if entry["transition"] == "rebaseline"}
    return applied == set(modes) and all(
        report["vehicle_types"][mode]["state"].get("baseline_reset_at") == iso(hour) for mode in modes
    )


def freeze_report(
    bucket: storage.Bucket,
    hour: datetime,
    config: Config,
    *,
    reset_modes: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Resume a frozen evaluation; only a new chronological hour can authorize a reset."""
    reset_modes = validate_reset_modes(reset_modes)
    report, _ = read_report(bucket, hour)
    if report is not None and reset_modes and not matching_reset_retry(report, hour, reset_modes):
        raise ValueError("cannot rebaseline an existing frozen report")
    if report is None:
        if reset_modes:
            ensure_chronological_reset(bucket, hour)
        report = evaluate_report(bucket, hour, config, reset_modes=reset_modes)
        try:
            write_object(bucket, hour_path("reports", hour), report, 0)
        except PreconditionFailed:
            # A concurrent run won; use its incident/event identities.
            report, _ = read_report(bucket, hour)
            if report is None:
                raise RuntimeError("report disappeared after concurrent create") from None
            if reset_modes and not matching_reset_retry(report, hour, reset_modes):
                raise ValueError("concurrent frozen report did not apply the requested rebaseline") from None
    return report


def run_monitor(
    bucket: storage.Bucket,
    client: bigquery.Client,
    hour: datetime,
    config: Config,
    *,
    reset_modes: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Evaluate and publish synchronously for operator replay; DAG tasks run independently."""
    report = freeze_report(bucket, hour, config, reset_modes=reset_modes)
    persist_rows(client, report)
    publish_history(bucket, report, config)
    return report


def deliver_alerts(bucket: storage.Bucket, hour: datetime, webhook_url: str) -> None:
    """Independent retryable outbox task; transport errors never claim delivery."""
    report, generation = read_report(bucket, hour)
    if report is None:
        raise ValueError("alert task requires a persisted report")
    deliver_report_alerts(bucket, hour, report, generation, webhook_url)


def deliver_report_alerts(
    bucket: storage.Bucket,
    hour: datetime,
    report: dict[str, Any],
    generation: int,
    webhook_url: str,
) -> None:
    """Acknowledge each event only after its HTTP request or disabled-webhook log."""
    pending = pending_events(report)
    if not webhook_url:
        for entry in pending:
            LOGGER.warning("Poller health transition (webhook disabled): %s", json.dumps(entry, sort_keys=True))
            entry["logged_at"] = iso(datetime.now(UTC))
            generation = write_object(bucket, hour_path("reports", hour), report, generation)
        return
    url = urlsplit(webhook_url)
    if url.scheme != "https" or not url.hostname or url.username or url.password:
        raise ValueError("POLLER_HEALTH_WEBHOOK_URL must be HTTPS without userinfo")
    for entry in pending:
        payload = {key: value for key, value in entry.items() if key not in {"delivered_at", "logged_at"}}
        try:
            response = requests.post(webhook_url, json=payload, timeout=10, allow_redirects=False)
            if not requests.codes.ok <= response.status_code < requests.codes.multiple_choices:
                raise RuntimeError("poller health webhook failed")  # noqa: TRY301 - sanitized transport error below
        except (requests.RequestException, RuntimeError):
            # Do not log exceptions containing a private webhook URL or response.
            raise RuntimeError("Poller health alert transport failed; event remains pending") from None
        entry["delivered_at"] = iso(datetime.now(UTC))
        generation = write_object(bucket, hour_path("reports", hour), report, generation)


def remember_acknowledgements(report: dict[str, Any], known: dict[str, tuple[str, str]]) -> None:
    """Recognize event identity across reports; HTTP proof takes precedence over logging."""
    for entry in report["events"]:
        if entry["delivered_at"] is not None:
            known[entry["event_id"]] = ("delivered_at", entry["delivered_at"])
        elif entry.get("logged_at") is not None:
            known.setdefault(entry["event_id"], ("logged_at", entry["logged_at"]))


def acknowledge_known_events(report: dict[str, Any], known: dict[str, tuple[str, str]]) -> bool:
    """Persist an existing acknowledgement of the same event_id, never invent delivery."""
    changed = False
    for entry in pending_events(report):
        acknowledgement = known.get(entry["event_id"])
        if acknowledgement:
            field, at = acknowledgement
            entry[field] = at
            changed = True
    return changed


def retry_alert_outbox(bucket: storage.Bucket, hour: datetime, webhook_url: str) -> None:
    """Read at most 48 reports and deduplicate acknowledged identities before replay."""
    first_hour = hour - timedelta(hours=OUTBOX_RETRY_HOURS - 1)
    reports = []
    known: dict[str, tuple[str, str]] = {}
    for offset in range(OUTBOX_RETRY_HOURS):
        candidate = first_hour + timedelta(hours=offset)
        report, generation = read_report(bucket, candidate)
        if report is None:
            if candidate == hour:
                raise ValueError("alert task requires a persisted current report")
            continue
        reports.append((candidate, report, generation))
        remember_acknowledgements(report, known)
    for candidate, report, stored_generation in reports:
        generation = stored_generation
        if acknowledge_known_events(report, known):
            generation = write_object(bucket, hour_path("reports", candidate), report, generation)
        pending = pending_events(report)
        if pending and candidate == first_hour:
            warn_outbox_expiry(candidate, len(pending))
        deliver_report_alerts(bucket, candidate, report, generation, webhook_url)
        remember_acknowledgements(report, known)


def persisted_report(bucket: storage.Bucket, hour_start: str) -> dict[str, Any]:
    """Publication tasks only ever read the frozen evaluation."""
    report, _ = read_report(bucket, timestamp(hour_start))
    if report is None:
        raise ValueError("publication requires a persisted report")
    return report


def reset_modes_from_context(context: dict[str, Any]) -> tuple[str, ...]:
    """Only explicit DAG-run JSON list configuration requests operator intervention."""
    conf = getattr(context.get("dag_run"), "conf", None)
    if conf is None:
        return ()
    if not isinstance(conf, dict) or not isinstance(conf.get("rebaseline_modes", []), list):
        raise ValueError("DAG run conf rebaseline_modes must be a list")  # noqa: TRY004 - one config failure boundary
    return validate_reset_modes(conf.get("rebaseline_modes", []))


with DAG(
    dag_id="poller_health",
    dag_display_name="Poller feed health",
    schedule="25 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    on_failure_callback=airflow_failure_alert,
    tags=["poller", "health"],
) as dag:

    @task
    def evaluate_hour() -> str:
        """Evaluate the completed hour after allowing closed-hour summary upload."""
        context = get_current_context()
        hour = completed_hour(context["data_interval_end"])
        bucket = storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
        freeze_report(bucket, hour, config_from_env(), reset_modes=reset_modes_from_context(context))
        return iso(hour)

    @task
    def persist_hour(hour_start: str) -> None:
        """Persist frozen warehouse rows; a BigQuery failure gates neither alerts nor the status page."""
        bucket = storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
        persist_rows(bigquery.Client(project=GCP_PROJECT), persisted_report(bucket, hour_start))

    @task
    def publish_hour(hour_start: str) -> None:
        """Publish the public status-page history."""
        bucket = storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
        publish_history(bucket, persisted_report(bucket, hour_start), config_from_env())

    @task
    def alert_transitions(hour_start: str) -> None:
        """Retry alerts without coupling them to raw GPS ingestion or fact builds."""
        bucket = storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
        retry_alert_outbox(bucket, timestamp(hour_start), os.getenv("POLLER_HEALTH_WEBHOOK_URL", "").strip())

    evaluated_hour = evaluate_hour()
    persist_hour(evaluated_hour)
    publish_hour(evaluated_hour)
    alert_transitions(evaluated_hour)
