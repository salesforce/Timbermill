package com.datorama.timbermill.redis.conf;

import com.datorama.timbermill.DateTimeTypeConverter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PostConstruct;


@Configuration
public class TimbermillJedisConfig {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillJedisConfig.class);

	@Value("${com.datorama.timbermill.redis.conf:{\"maxTotalConnections\":10000,\"maxIdleConnections\":10000,\"minIdleConnections\":10,\"host\":\"localhost\",\"port\":6379,\"connectTimeout\":5000,\"purgeThreadPoolSize\":10,\"testOnReturn\":false,\"useSSL\":false}}")
	private String jedisClientConfigStr;

	private Gson gson = new GsonBuilder().registerTypeAdapter(DateTime.class, new DateTimeTypeConverter()).create();

	@PostConstruct
	public void init() {
		LOG.info("TimbermillJedisConfig initialized.");
	}


	@Bean(name = "timbermillJedisConnectionFactory")
	JedisConnectionFactory jedisConnectionFactory() {
		JedisClientConfig jedisClientConfig = gson.fromJson(jedisClientConfigStr, JedisClientConfig.class);
		JedisConnectionFactory jedisConFactory = new JedisConnectionFactory();

		if (jedisClientConfig != null) {
			//jedis pool config
			JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

			if (jedisClientConfig.isTestOnReturn()) {
				jedisPoolConfig.setTestOnReturn(true);
			}

			if (jedisClientConfig.isTestOnBorrow()) {
				jedisPoolConfig.setTestOnBorrow(true);
			}

			if (jedisClientConfig.isTestOnCreate()) {
				jedisPoolConfig.setTestOnCreate(true);
			}

			if (jedisClientConfig.getMaxTotalConnections() != null) {
				jedisPoolConfig.setMaxTotal(jedisClientConfig.getMaxTotalConnections());// max number of connections that can be created at a given time.
			}

			if (jedisClientConfig.getMaxIdleConnections() != null) {
				jedisPoolConfig.setMaxIdle(jedisClientConfig.getMaxIdleConnections());//This is the max number of connections that can be idle in the pool without being immediately evicted (closed)
			}
			if (jedisClientConfig.getMinIdleConnections() != null) {
				//This is the number of "warm" connections (e.g. ready for immediate use) that remain in the pool even when load has reduced. If not set, the default is 0.
				jedisPoolConfig.setMinIdle(jedisClientConfig.getMinIdleConnections());
			}

			if (jedisClientConfig.getEvictionPolicyClassName() != null) {
				jedisPoolConfig.setEvictionPolicyClassName(jedisClientConfig.getEvictionPolicyClassName());
			}

			if (jedisClientConfig.getMinEvictableIdleTimeMillis() != null) {
				jedisPoolConfig.setMinEvictableIdleTimeMillis(jedisClientConfig.getMinEvictableIdleTimeMillis());
			}

			if (jedisClientConfig.getSoftMinEvictableIdleTimeMillis() != null) {
				jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(jedisClientConfig.getSoftMinEvictableIdleTimeMillis());
			}

			if (jedisClientConfig.getTimeBetweenEvictionRunsMillis() != null) {
				jedisPoolConfig.setTimeBetweenEvictionRunsMillis(jedisClientConfig.getTimeBetweenEvictionRunsMillis());
			}

			if (jedisClientConfig.getNumTestsPerEvictionRun() != null) {
				jedisPoolConfig.setNumTestsPerEvictionRun(jedisClientConfig.getNumTestsPerEvictionRun());
			}

//			if (!jedisClientConfig.getTestWhileIdle()) {
//				jedisPoolConfig.setTestWhileIdle(false);
//			}

			//client config
			jedisConFactory.setTimeout(jedisClientConfig.getConnectTimeout());
			jedisConFactory.setHostName(jedisClientConfig.getHost());
			jedisConFactory.setPort(jedisClientConfig.getPort());
			jedisConFactory.setUsePool(true);
			if (jedisClientConfig.getPassword() != null) {
				jedisConFactory.setPassword(jedisClientConfig.getPassword());
			}
			if (jedisClientConfig.isUseSSL()) {
				jedisConFactory.setUseSsl(true);
			}
			jedisConFactory.setPoolConfig(jedisPoolConfig);

			LOG.info("Jedis connection factory was created");

		} else {
			LOG.error("ERROR: could not parse jedis config!");
		}

		return jedisConFactory;
	}

	@Bean(name = "timbermillRedisTemplate")
	RedisTemplate<String, String> redisTemplate() {
		RedisTemplate template = new StringRedisTemplate();
		template.setConnectionFactory(jedisConnectionFactory());
		template.setDefaultSerializer(new StringRedisSerializer());
		template.afterPropertiesSet();
		return template;
	}
}
