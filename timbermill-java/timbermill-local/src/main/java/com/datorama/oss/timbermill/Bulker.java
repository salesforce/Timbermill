package com.datorama.oss.timbermill;

import com.datorama.oss.timbermill.common.disk.DbBulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Bulker {
	private RestHighLevelClient client;
	private static final Logger LOG = LoggerFactory.getLogger(Bulker.class);
	Bulker(RestHighLevelClient client) {
		this.client = client;
	}

	// wrap bulk method as a not-final method in order that Mockito will able to mock it
	public BulkResponse bulk(DbBulkRequest request) throws IOException {
		return client.bulk(request.getRequest(), RequestOptions.DEFAULT);
	}
}
