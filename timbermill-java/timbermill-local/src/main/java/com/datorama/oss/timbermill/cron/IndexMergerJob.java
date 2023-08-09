package com.datorama.oss.timbermill.cron;


import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.redis.RedisService;
import com.github.jedis.lock.JedisLock;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.MDC;

import java.util.UUID;

@DisallowConcurrentExecution
public class IndexMergerJob implements Job {

    private static final String LOCK_NAME = ExpiredTasksDeletionJob.class.getSimpleName();

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        ElasticsearchClient client = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
        RedisService redisService = (RedisService) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.REDIS_SERVICE);
        JedisLock lock;
        String flowId = "Index Merging Job - " + UUID.randomUUID();
        MDC.put("id", flowId);

        if (redisService == null) {
            client.mergeIndexes();
        } else if ((lock = redisService.lockIfUnlocked(LOCK_NAME)) != null) {
            try {
                client.mergeIndexes();
            } finally {
                redisService.release(lock);
            }
        }
    }
}
