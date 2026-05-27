-- Daily snapshot of dashboard metrics — long format so adding a new
-- metric is an INSERT, not an ALTER TABLE. One row per
-- (snapshot_date, region, metric); `region` is either a configured
-- region name or the sentinel `_combined` for cloud-wide rollups
-- (sums for per-region counts, pass-throughs for global services
-- like Keystone). The collector (`opsbi snapshot-metrics`) uses
-- INSERT OR REPLACE so re-running a day is idempotent.
CREATE TABLE IF NOT EXISTS dashboard_metric_history (
    snapshot_date TEXT    NOT NULL,
    snapshot_at   TEXT    NOT NULL,
    region        TEXT    NOT NULL,
    metric        TEXT    NOT NULL,
    value         INTEGER NOT NULL,
    PRIMARY KEY (snapshot_date, region, metric)
);

CREATE INDEX IF NOT EXISTS idx_dmh_metric_date
    ON dashboard_metric_history(metric, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_dmh_region_date
    ON dashboard_metric_history(region, snapshot_date);
