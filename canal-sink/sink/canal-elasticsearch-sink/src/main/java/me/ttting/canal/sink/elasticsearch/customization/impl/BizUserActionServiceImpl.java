package me.ttting.canal.sink.elasticsearch.customization.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import me.ttting.canal.sink.elasticsearch.ResetHighLevelClientFactory;
import me.ttting.canal.sink.elasticsearch.customization.CustomizationService;

public class BizUserActionServiceImpl extends CustomizationService {

	@Override
	public String getKey() {
		return "userAction";
	}

	@Override
	public List<Map<String, Object>> excute(Map map, String index, String doc) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		Map<String, Object> sourceResult = new HashMap<String, Object>();
		RestHighLevelClient client = ResetHighLevelClientFactory.getClient();
		SearchRequest request = new SearchRequest(index);
		request.types(doc);
		Object mobile = map.get("distinct_id");
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		MatchQueryBuilder mobileMatchQueryBuilder = QueryBuilders.matchQuery("MOBILE", mobile.toString());
		MatchQueryBuilder telMatchQueryBuilder = QueryBuilders.matchQuery("TEL", mobile.toString());
		boolQueryBuilder.should(mobileMatchQueryBuilder);
		boolQueryBuilder.should(telMatchQueryBuilder);
		sourceBuilder.query(boolQueryBuilder);
		request.source(sourceBuilder);
		SearchResponse getResponse;
		try {
			getResponse = client.search(request);
			SearchHits searchHits = getResponse.getHits();
			SearchHit[] hits = searchHits.getHits();
			for (SearchHit hit : hits) {
				Map<String, Object> result = hit.getSourceAsMap();
				if (null == result.get("service_line") || !Integer.valueOf(result.get("service_line").toString()).equals(1)) {
					continue;
				}
				List<Map<String, Object>> userActions = null;
				if (result.get("userActions") == null) {
					userActions = new ArrayList<Map<String, Object>>();
				} else {
					userActions = (List<Map<String, Object>>) result.get("userActions");
				}
				Object orderInfosObj = result.get("orderInfoList");
				List<Map<String, Object>> orderInfos = null;
				if (orderInfosObj == null) {
					orderInfos = new ArrayList<Map<String, Object>>();
				} else {
					orderInfos = (List<Map<String, Object>>) orderInfosObj;
				}
				Integer havePromoteOrder = 0;
				Long businessUnitId = Long.parseLong(map.get("business_unit_id").toString());
				for (Map<String, Object> orderInfo : orderInfos) {
					if (orderInfo.get("business_unit_id") != null
							&& businessUnitId.equals(Long.valueOf(orderInfo.get("business_unit_id").toString()))) {
						havePromoteOrder = 1;
						break;
					}
				}
				Map<String, Object> userAction=null;
				List<Map<String, Object> >newUserActions=new ArrayList<Map<String, Object> >();
				for(Map<String, Object> beforeUserAction:userActions) {
					if(beforeUserAction.get("id")!=null)
					newUserActions.add(beforeUserAction);
				}
				for(Map<String, Object> beforeUserAction:newUserActions) {
					if(beforeUserAction.get("id")!=null&&beforeUserAction.get("id").toString().equals(map.get("id").toString())) {
						userAction=beforeUserAction;
					}
				}
				if(userAction==null) {
					userAction= new HashMap<String, Object>();
					userAction.put("id", map.get("id"));
					userAction.put("business_unit_id", businessUnitId);
					userAction.put("have_promote_order", havePromoteOrder);
					userAction.put("query_time", map.get("add_time"));
					userActions.add(userAction);
				}else {
					userAction.put("id", map.get("id"));
					userAction.put("business_unit_id", businessUnitId);
					userAction.put("have_promote_order", havePromoteOrder);
					userAction.put("query_time", map.get("add_time"));
				}
				sourceResult.put("userActions", userActions);
				sourceResult.put("ID", hit.getId());
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		resultList.add(sourceResult);
		return resultList;
	}
}
