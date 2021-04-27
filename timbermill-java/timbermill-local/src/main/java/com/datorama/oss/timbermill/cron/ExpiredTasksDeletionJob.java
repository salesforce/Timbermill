package com.datorama.oss.timbermill.cron;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.redis.RedisService;
import com.github.jedis.lock.JedisLock;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.MDC;

import java.util.UUID;

@DisallowConcurrentExecution
public class ExpiredTasksDeletionJob implements Job {

    private static final String LOCK_NAME = ExpiredTasksDeletionJob.class.getSimpleName();

    @Override
    public void execute(JobExecutionContext context) {
        ElasticsearchClient client = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
        RedisService redisService = (RedisService) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.REDIS_SERVICE);
        JedisLock lock;
        String flowId = "Expired Tasks Deletion Job - " + UUID.randomUUID().toString();
        MDC.put("id", flowId);

        if (redisService == null) {
            client.deleteExpiredTasks();
        } else if ((lock = redisService.lockIfUnlocked(LOCK_NAME)) != null) {
            try {
                client.deleteExpiredTasks();
            } finally {
                redisService.release(lock);
            }
        }
    }
}
