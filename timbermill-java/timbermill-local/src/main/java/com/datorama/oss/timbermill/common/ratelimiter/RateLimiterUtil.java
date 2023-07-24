package com.datorama.oss.timbermill.common.ratelimiter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;

import java.time.Duration;

public class RateLimiterUtil {

    private static final Duration CACHE_EXPIRE_MINUTES = Duration.ofMinutes(1);
    private static RateLimiterConfig config = null;


    public static LoadingCache<String, RateLimiter> initRateLimiter(int limitForPeriod, Duration limitRefreshPeriodMinutes, int rateLimiterCapacity) {
        if (config == null) {
            config = RateLimiterConfig.custom()
                    .limitForPeriod(limitForPeriod)
                    .limitRefreshPeriod(limitRefreshPeriodMinutes)
                    .timeoutDuration(Duration.ofMillis(1))
                    .build();
        }

        CacheLoader<String, RateLimiter> loader = new CacheLoader<String, RateLimiter>() {
            @Override
            public RateLimiter load(String key) {
                return createRateLimiter(config, key);
            }
        };
        return CacheBuilder.newBuilder()
                .maximumSize(rateLimiterCapacity)
                .expireAfterAccess(CACHE_EXPIRE_MINUTES)
                .build(loader);
    }


    private static RateLimiter createRateLimiter(RateLimiterConfig config, String name) {
        RateLimiterRegistry registry = RateLimiterRegistry.of(config);
        return registry.rateLimiter(name);
    }

    public static RateLimiterConfig getRateLimiterConfig() {
        return config;
    }
}
