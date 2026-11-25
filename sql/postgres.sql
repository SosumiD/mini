CREATE TABLE IF NOT EXISTS aggregates (
    day DATE NOT NULL,
    metric TEXT NOT NULL,
    dimension TEXT NOT NULL DEFAULT '',
    value DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (day, metric, dimension)
);
CREATE INDEX IF NOT EXISTS aggregates_metric_day_idx ON aggregates (metric, day);
