import asyncio
import csv
import io
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

import boto3
import psycopg
from botocore.config import Config
from fastapi import FastAPI, HTTPException

from app.shared import ch_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("aggregator")
lock = threading.Lock()


def parse_day(day: str) -> date:
    return date.fromisoformat(day)


def calculate_day(target: date) -> dict:
    started_at = time.monotonic()
    computed_at = datetime.now(timezone.utc)
    version = time.time_ns()
    log.info("cycle started date=%s", target)
    with ch_client() as ch:
        summary = ch.query("""
            SELECT uniqExact(user_id) AS dau,
                   countIf(event_type='VIEW_STARTED') AS started,
                   countIf(event_type='VIEW_FINISHED') AS finished,
                   if(countIf(event_type='VIEW_FINISHED')=0, 0,
                      avgIf(ifNull(progress_seconds, 0), event_type='VIEW_FINISHED')) AS avg_watch,
                   count() AS events
            FROM cinema.raw_events FINAL
            WHERE event_date = {day:Date}
        """, parameters={"day": target}).first_row
        dau, started, finished, avg_watch, events = summary
        top = ch.query("""
            SELECT movie_id, countIf(event_type='VIEW_STARTED') AS views
            FROM cinema.raw_events FINAL
            WHERE event_date = {day:Date}
            GROUP BY movie_id HAVING views > 0
            ORDER BY views DESC, movie_id ASC
        """, parameters={"day": target}).result_rows
        retention = ch.query("""
            WITH first_seen AS (
                SELECT user_id, min(event_date) AS cohort_date
                FROM cinema.raw_events FINAL
                WHERE event_type = 'VIEW_STARTED'
                GROUP BY user_id
            ), activity AS (
                SELECT DISTINCT user_id, event_date AS active_day
                FROM cinema.raw_events FINAL
                WHERE event_type = 'VIEW_STARTED'
            )
            SELECT f.cohort_date,
                   dateDiff('day', f.cohort_date, a.active_day) AS day_offset,
                   uniqExact(a.user_id) AS returned
            FROM first_seen f INNER JOIN activity a USING (user_id)
            WHERE f.cohort_date BETWEEN {start:Date} AND {end:Date}
              AND day_offset BETWEEN 0 AND 7
            GROUP BY f.cohort_date, day_offset
            ORDER BY f.cohort_date, day_offset
        """, parameters={"start": target - timedelta(days=7), "end": target}).result_rows

        pct = (finished / started * 100) if started else 0.0
        entries = [
            (target, "dau", "", float(dau)),
            (target, "avg_watch_seconds", "", float(avg_watch)),
            (target, "view_conversion_percent", "", float(pct)),
            (target, "views_started", "", float(started)),
            (target, "views_finished", "", float(finished)),
        ]
        entries.extend((target, "top_movie_views", str(movie), float(count)) for movie, count in top)

        ret_counts = {}
        for cohort_date, offset, returned in retention:
            ret_counts[(cohort_date, int(offset))] = int(returned)
        cohort_rows = []
        for cohort_date in sorted({key[0] for key in ret_counts}):
            size = ret_counts.get((cohort_date, 0), 0)
            for offset in range(8):
                # Future cohort days are not yet observed; do not misreport them as 0%.
                if cohort_date + timedelta(days=offset) > datetime.now(timezone.utc).date():
                    continue
                returned = ret_counts.get((cohort_date, offset), 0)
                percentage = (100.0 * returned / size) if size else 0.0
                cohort_rows.append((cohort_date, offset, size, returned, percentage, computed_at, version))
                entries.append((cohort_date, "retention_percent", f"day_{offset}", percentage))
                if offset in (1, 7):
                    entries.append((cohort_date, f"retention_d{offset}", "", percentage))

        if cohort_rows:
            ch.insert("cohort_retention", cohort_rows,
                      column_names=["cohort_date", "day_offset", "cohort_size", "returned",
                                    "retention_percent", "computed_at", "version"], database="cinema")
        ch.insert("daily_metrics", [(d, m, x, v, computed_at, version) for d, m, x, v in entries],
                  column_names=["day", "metric", "dimension", "value", "computed_at", "version"],
                  database="cinema")

    # Retention cohorts for the 7 preceding dates get refreshed as new activity arrives.
    # PG is atomic and idempotent; transient failures are retried with exponential backoff.
    for attempt in range(4):
        try:
            with psycopg.connect(os.environ["PG_DSN"]) as pg:
                with pg.cursor() as cursor:
                    cursor.executemany("""
                        INSERT INTO aggregates (day, metric, dimension, value, computed_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (day, metric, dimension) DO UPDATE
                        SET value=EXCLUDED.value, computed_at=EXCLUDED.computed_at
                    """, [(d, m, x, v, computed_at) for d, m, x, v in entries])
            break
        except psycopg.OperationalError as exc:
            log.warning("PostgreSQL retry=%d date=%s error=%s", attempt+1, target, exc)
            if attempt == 3:
                raise
            time.sleep(0.5 * 2**attempt)

    elapsed = time.monotonic() - started_at
    log.info("cycle finished date=%s events=%d metrics=%d elapsed=%.2fs",
             target, events, len(entries), elapsed)
    return {"date": target.isoformat(), "events_processed": events,
            "metrics": len(entries), "elapsed_seconds": round(elapsed, 3)}


def export_day(target: date) -> dict:
    with psycopg.connect(os.environ["PG_DSN"]) as pg:
        rows = pg.execute("""
            SELECT day, metric, dimension, value, computed_at
            FROM aggregates WHERE day = %s ORDER BY metric, dimension
        """, (target,)).fetchall()
    if not rows:
        raise ValueError(f"no metrics for {target}")

    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(["date", "metric", "dimension", "value", "computed_at_utc"])
    for day, metric, dimension, value, computed_at in rows:
        writer.writerow([day.isoformat(), metric, dimension, value, computed_at.isoformat()])

    client = boto3.client(
        "s3", endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "cinema"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "cinema_password"),
        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}),
    )
    bucket = "movie-analytics"
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)
    key = f"daily/{target.isoformat()}/aggregates.csv"
    client.put_object(Bucket=bucket, Key=key, Body=stream.getvalue().encode("utf-8"),
                      ContentType="text/csv; charset=utf-8")
    log.info("export success date=%s rows=%d object=s3://%s/%s", target, len(rows), bucket, key)
    return {"date": target.isoformat(), "rows": len(rows), "s3": f"s3://{bucket}/{key}"}


def run_aggregation(day: date):
    with lock:
        result = calculate_day(day)
        return result


async def scheduler():
    await asyncio.sleep(int(os.getenv("AGG_STARTUP_DELAY_SECONDS", "35")))
    interval = max(15, int(os.getenv("AGG_INTERVAL_SECONDS", "120")))
    while True:
        today = datetime.now(timezone.utc).date()
        # Recompute recent history so the heatmap has data immediately.
        for n in range(11, -1, -1):
            day = today - timedelta(days=n)
            try:
                await asyncio.to_thread(run_aggregation, day)
            except Exception:
                log.exception("scheduled aggregation failed date=%s", day)
        try:
            await asyncio.to_thread(export_day, today - timedelta(days=1))
        except Exception:
            log.exception("scheduled export failed; retry on next cycle")
        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(scheduler())
    yield
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


app = FastAPI(title="Movie Aggregation and Export", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "aggregator"}


@app.post("/aggregate/{day}")
def manual_aggregate(day: date):
    try:
        return run_aggregation(day)
    except Exception as exc:
        log.exception("aggregation failed day=%s", day)
        raise HTTPException(503, "Aggregation failed; check logs") from exc


@app.post("/export/{day}")
def manual_export(day: date):
    try:
        return export_day(day)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        log.exception("export failed day=%s", day)
        raise HTTPException(503, "Export failed; check logs") from exc
