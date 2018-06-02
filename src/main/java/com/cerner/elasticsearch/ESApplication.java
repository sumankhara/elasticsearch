package com.cerner.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.google.gson.Gson;

public class ESApplication {

	private static Client client;
	private static Settings settings;

	public static Client getESConnection() {
		settings = Settings.builder().put("cluster.name", "elasticsearch").put("path.home", "/").build();
		try {
			client = new PreBuiltTransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		return client;
	}

	public static void main(String[] args) {
		Map<String, Object> document1 = new HashMap<String, Object>();
		document1.put("name", "Reebok");
		document1.put("size", 9);
		document1.put("color", "blue");

		// Indexing a document
		IndexResponse indexResponse = getESConnection().prepareIndex().setIndex("products").setType("shoes").setId("3")
				.setSource(document1).execute().actionGet();

		System.out.println("Index: " + indexResponse.getIndex());
		System.out.println("Type: " + indexResponse.getType());
		System.out.println("Id: " + indexResponse.getId());
		System.out.println("Version: " + indexResponse.getVersion());

		// Fetching a document
		GetResponse getResponse = getESConnection().prepareGet().setIndex("products").setType("mobiles").setId("1")
				.execute().actionGet();

		Map<String, Object> source = getResponse.getSource();

		System.out.println("Index: " + getResponse.getIndex());
		System.out.println("Type: " + getResponse.getType());
		System.out.println("Id: " + getResponse.getId());
		System.out.println("Version: " + getResponse.getVersion());

		Gson gson = new Gson();
		String json = gson.toJson(source);
		System.out.println("source: " + json);

		// Search
		SearchResponse response = getESConnection().prepareSearch("products")
											.setTypes("mobiles")
											.setSearchType(SearchType.QUERY_THEN_FETCH)
											.setQuery(QueryBuilders.termQuery("name", "iphone"))
											.setFrom(0).setSize(60).setExplain(true)
											.execute()
											.actionGet();
		SearchHit[] results = response.getHits().getHits();
		System.out.println("Current results: " + results.length);
		for (SearchHit hit : results) {
			System.out.println("------------------------------");
			Map<String, Object> result = hit.getSource();
			System.out.println("search result: " + gson.toJson(result));
		}

		getESConnection().close();
	}

}
