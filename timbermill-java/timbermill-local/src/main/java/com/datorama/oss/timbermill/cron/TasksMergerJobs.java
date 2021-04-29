package com.datorama.oss.timbermill.cron;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.redis.RedisService;
import com.github.jedis.lock.JedisLock;
import kamon.metric.Timer;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.UUID;

@DisallowConcurrentExecution
public class TasksMergerJobs implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(TasksMergerJobs.class);
    public static final String LOCK_NAME = TasksMergerJobs.class.getSimpleName();

	@Override public void execute(JobExecutionContext context) {
		JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
		ElasticsearchClient client = (ElasticsearchClient) jobDataMap.get(ElasticsearchUtil.CLIENT);
        RedisService redisService = (RedisService) jobDataMap.get(ElasticsearchUtil.REDIS_SERVICE);
        JedisLock lock;
        String flowId = "Partials Tasks Merger Job - " + UUID.randomUUID().toString();
        MDC.put("id", flowId);
        LOG.info("Partials Tasks Merger Job started.");
        Timer.Started started = KamonConstants.PARTIALS_JOB_LATENCY.withoutTags().start();

        if (redisService == null) {
            client.migrateTasksToNewIndex();
        } else if ((lock = redisService.lockIfUnlocked(LOCK_NAME)) != null) {
            try {
                client.migrateTasksToNewIndex();
            } finally {
                redisService.release(lock);
            }
        }

        LOG.info("Partials Tasks Merger Job ended.");
        started.stop();
    }
}
