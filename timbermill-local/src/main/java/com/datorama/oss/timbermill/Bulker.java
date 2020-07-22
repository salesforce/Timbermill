package com.datorama.oss.timbermill;

import java.io.IOException;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import com.datorama.oss.timbermill.common.disk.DbBulkRequest;

public class Bulker {
	private RestHighLevelClient client;

	Bulker(RestHighLevelClient client) {
		this.client = client;
	}

	// wrap bulk method as a not-final method in order that Mockito will able to mock it
	public BulkResponse bulk(DbBulkRequest request) throws IOException {
		return client.bulk(request.getRequest(), RequestOptions.DEFAULT);
	}
}
