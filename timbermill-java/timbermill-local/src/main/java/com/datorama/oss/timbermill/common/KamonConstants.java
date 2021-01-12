package com.datorama.oss.timbermill.common;

import kamon.Kamon;
import kamon.metric.Metric;

public class KamonConstants {
	public static final Metric.RangeSampler MESSAGES_IN_INPUT_QUEUE_RANGE_SAMPLER = Kamon.rangeSampler("timbermill2.inputQueue.size.range.sampler");
	public static final Metric.RangeSampler MESSAGES_IN_OVERFLOWED_QUEUE_RANGE_SAMPLER = Kamon.rangeSampler("timbermill2.overflowedQueue.size.range.sampler");
	public static final Metric.Timer ORPHAN_RESOLUTION_LATENCY = Kamon.timer("timbermill2.orphan.resolution.latency.timer");
	public static final Metric.Histogram ORPHANS_ADOPTED_HISTOGRAM = Kamon.histogram("timbermill2.orphans.adopted.histogram");
	public static final Metric.RangeSampler TASKS_IN_TASK_CACHE = Kamon.rangeSampler("timbermill2.taskCache.size.range.sampler");
	public static final Metric.RangeSampler ORPHANS_IN_ORPHANS_CACHE = Kamon.rangeSampler("timbermill2.orphanCache.size.range.sampler");
	public static final Metric.Timer PARTIALS_JOB_LATENCY = Kamon.timer("timbermill2.partial.tasks.job.latency.timer");
	public static final Metric.Histogram PARTIAL_TASKS_FAILED_TO_MIGRATED_HISTOGRAM = Kamon.histogram("timbermill2.partial.tasks.failed.to.migrate.histogram");
	public static final Metric.Histogram PARTIAL_TASKS_MIGRATED_HISTOGRAM = Kamon.histogram("timbermill2.partial.tasks.migrated.histogram");
	public static final Metric.Histogram PARTIAL_TASKS_FOUND_HISTOGRAM = Kamon.histogram("timbermill2.partial.tasks.found.histogram");
	public static final Metric.Timer BULK_FETCH_JOB_LATENCY = Kamon.timer("timbermill2.failed.tasks.fetch.job.latency.timer");
	public static final Metric.Histogram TASKS_FETCHED_FROM_DISK_HISTOGRAM = Kamon.histogram("timbermill2.failed.tasks.fetched.from.disk.histogram");
	public static final Metric.Histogram BATCH_DURATION_HISTOGRAM = Kamon.histogram("timbermill2.batch.duration.histogram");
	public static final Metric.Histogram TASKS_INDEXED_HISTOGRAM = Kamon.histogram("timbermill2.tasks.indexed.histogram");
	public static final Metric.Histogram MISSING_PARENTS_TASKS_FETCHED_HISTOGRAM = Kamon.histogram("timbermill2.missing.parents.tasks.fetched.histogram");
	public static final Metric.Histogram MISSING_PARENTS_HISTOGRAM = Kamon.histogram("timbermill2.missing.parents.histogram");
	public static final Metric.Gauge CURRENT_DATA_IN_DB_GAUGE = Kamon.gauge("timbermill2.data.in.db.gauge");
	public static final Metric.Timer EVENTS_FETCH_JOB_LATENCY = Kamon.timer("timbermill2.overflowed.events.fetch.job.latency.timer");
}
