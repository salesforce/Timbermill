package com.datorama.oss.timbermill;

import com.datorama.oss.timbermill.pipe.TimbermillServerOutputPipeBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service("timbermill")
public class TimbermillBootstraperService {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillBootstraperService.class);

	@Value("${timbermill.log.enabled:true}")
	private boolean timberLogEnabled;

	@Value("${env:undefined}")
	private String hostType;

	@Value("${timbermill.url:http://localhost:8484}")
	private String timbermillServer;

	@Value("${timbermill.batch.max-size:2097152}")
	private int maxEventsBatchSize;

	@Value("${timbermill.buffer.max-size:500000000}")
	private int maxBufferSize;

	@Value("${timbermill.batch.max-seconds-interval:3}")
	private int maxSecondsBeforeBatchTimeout;

	@Value("${timbermill.env:default}")
	private String timbermillEnv;

	private static final String JVM_UUID = UUID.randomUUID().toString();

	@PostConstruct
	public void init() {
		if (timberLogEnabled) {
			if (!StringUtils.isEmpty(timbermillServer)) {
				LOG.info(String.format("Bootstrapping TimberLog v2; hostType=%s", hostType));
				Map<String, String> bootstrapParams = new HashMap<>();
				bootstrapParams.put("host", getHostName());
				bootstrapParams.put("jvm", getJvmUuid());
				bootstrapParams.put("hostType", hostType);

				TimbermillServerOutputPipeBuilder builder = new TimbermillServerOutputPipeBuilder().timbermillServerUrl(timbermillServer);
				builder.maxEventsBatchSize(maxEventsBatchSize);
				builder.maxSecondsBeforeBatchTimeout(maxSecondsBeforeBatchTimeout);
				builder.maxBufferSize(maxBufferSize);

				TimberLogger.bootstrap(builder.build(), bootstrapParams, timbermillEnv);
				TimberLogger.spot("server_startup");
			}
			else {
				LOG.warn("Timbermill server url is missing, not bootstrapping");
			}

		} else {
			LOG.info("TimberLog DISABLED, not bootstrapping");
		}
	}

	private static String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			LOG.warn(e.getMessage(), e);
			return null;
		}
	}

	private static String getJvmUuid() {
		return JVM_UUID;
	}

	@PreDestroy
	public void stop(){
		TimberLogger.exit();
	}
}