package com.datorama.oss.timbermill;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.getOldAlias;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.getTimbermillIndexAlias;

public class ElasticsearchClientForTests extends ElasticsearchClient{

	public ElasticsearchClientForTests(String elasticUrl, String awsRegion) {
			super(elasticUrl, 1000, 1, awsRegion, null, null,
					7, 100, 1000000000,3, 3, 1000,null ,1, 1,
					4000, null, 10 , 60, 10000, 10, "1gb", "12.0");
    }

	public void createTimbermillAliasForMigrationTest(String currentIndex, String oldIndex, String env) throws IOException {
		IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
		String alias = getTimbermillIndexAlias(env);
		String oldAlias = getOldAlias(alias);
		IndicesAliasesRequest.AliasActions addOldIndexAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(oldIndex).alias(oldAlias);
		IndicesAliasesRequest.AliasActions addNewIndexAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(currentIndex).alias(alias);
		indicesAliasesRequest.addAliasAction(addOldIndexAction);
		indicesAliasesRequest.addAliasAction(addNewIndexAction);
		client.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
	}

	public void createTimbermillIndexForTests(String index) throws IOException {
		GetIndexRequest exists = new GetIndexRequest(index);
		boolean isExists = client.indices().exists(exists, RequestOptions.DEFAULT);
		if (!isExists) {
			CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
			client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
		}
	}

}

