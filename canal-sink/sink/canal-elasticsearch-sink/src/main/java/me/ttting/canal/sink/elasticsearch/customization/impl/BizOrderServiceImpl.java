package me.ttting.canal.sink.elasticsearch.customization.impl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

import lombok.extern.slf4j.Slf4j;
import me.ttting.canal.sink.elasticsearch.ResetHighLevelClientFactory;
import me.ttting.canal.sink.elasticsearch.customization.CustomizationService;

@Slf4j
public class BizOrderServiceImpl extends CustomizationService {
	public String getKey() {
		return "bizOrder";
	}

	@Override
	public List<Map<String,Object>> excute(Map map, String index, String doc) {
		List<Map<String,Object>>sourceResult=new ArrayList<Map<String,Object>>();
		RestHighLevelClient client = ResetHighLevelClientFactory.getClient();
		//订单表中只存储了教育商机id，索引通过手机号找到对应每个业务线的商机数据
		SearchRequest request = new SearchRequest(index);
		request.types(doc);
		Object mobile = map.get("MOBILE");
		if (mobile == null) {
			return sourceResult;
		}
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
				boolean isExsitOrder = false;
				 Object orderInfosObj=result.get("orderInfoList");
				 List<Map<String, Object>> orderInfos=null;
				 if(orderInfosObj==null) {
					 orderInfos=new ArrayList<Map<String, Object>>();
				 }else {
					 orderInfos = (List<Map<String, Object>>)orderInfosObj;
				 }
				for (Map<String, Object> orderInfo : orderInfos) {
					Object orderId = orderInfo.get("ID");
					Integer orderType = (Integer) orderInfo.get("order_type");
					if (orderType != 0) {
						continue;
					}
					Object dbOrderId = map.get("ID");
					// 订单号相等
					if (dbOrderId.equals(orderId)) {
						isExsitOrder = true;
						Object dbReayPay = map.get("REAL_PAY");
						Object dbRefund= map.get("REFUND");
						Object dbBusinessUnitId = map.get("business_unit_id");
						Object businessUnitId = orderInfo.get("business_unit_id");
						Object orderInfoReayPay = orderInfo.get("real_pay");
						Object orderInfoRefund = orderInfo.get("refund");
						if (orderInfoReayPay!=null&&orderInfoReayPay.equals(dbReayPay)&&orderInfoRefund!=null&&orderInfoRefund.equals(dbRefund)&&dbBusinessUnitId!=null&&dbBusinessUnitId.equals(businessUnitId)) {
							return sourceResult;
						} else {
							// 更新
							orderInfo.put("real_pay", dbReayPay);
							orderInfo.put("refund", dbRefund);
							orderInfo.put("business_unit_id", dbBusinessUnitId);
						}
					}
				}
				 Object orderTypeInfosObj=result.get("orderTypeInfoList");
				 List<Map<String, Object>> orderTypeInfos=null;
				 if(orderTypeInfosObj==null) {
					 orderTypeInfos=new ArrayList<Map<String, Object>>();
				 }else {
					 orderTypeInfos = (List<Map<String, Object>>)orderTypeInfosObj;
				 }
				 Object userActionsObj=result.get("userActions");
				 List<Map<String, Object>> userActions=null;
				 if(userActionsObj==null) {
					 userActions=new ArrayList<Map<String, Object>>();
				 }else {
					 userActions = (List<Map<String, Object>>)userActionsObj;
				 }
				String id = hit.getId();
				if (!isExsitOrder) {
					addNewOrderInfo(map, orderInfos, userActions,orderTypeInfos);
				}
				Map<String,Object>sourceMap=new HashMap<String,Object>();
				sourceMap.put("orderTypeInfoList", orderTypeInfos);
				sourceMap.put("orderInfoList", orderInfos);
				sourceMap.put("userActions", userActions);
				sourceMap.put("ID", id);
				sourceResult.add(sourceMap);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sourceResult;
	}

	private void addNewOrderInfo(Map dbOrder, List<Map<String, Object>> orderInfos,List<Map<String, Object>> userActions,
			List<Map<String, Object>> orderTypeInfos) {
		//订单数据处理
		if(orderInfos==null) {
			orderInfos=new ArrayList<Map<String, Object>>();
		}
		Map<String,Object>orderInfo=new HashMap<String,Object>();
		orderInfo.put("ID", dbOrder.get("ID"));
		orderInfo.put("real_pay", dbOrder.get("REAL_PAY"));
		orderInfo.put("refund", dbOrder.get("REFUND"));
		orderInfo.put("user_no", dbOrder.get("USER_NO"));
		orderInfo.put("order_time", dbOrder.get("ORDER_TIME"));
		orderInfo.put("business_unit_id", dbOrder.get("business_unit_id"));
		orderInfo.put("order_type", 0);
		orderInfos.add(orderInfo);
		//订单类型数据处理
		if(orderTypeInfos==null) {
			orderTypeInfos=new ArrayList<Map<String, Object>>();
		}
		Map<String,Object>exsitOrderTypeInfo=null;
		for(Map<String,Object>orderTypeInfo:orderTypeInfos) {
			Object  orderType=orderTypeInfo.get("order_type");
			if(orderType!=null&&((Integer)orderType).equals(0)) {
				exsitOrderTypeInfo=orderTypeInfo;
				break;
			}
		}
		if(exsitOrderTypeInfo==null) {
			exsitOrderTypeInfo=new HashMap<String,Object>();
			exsitOrderTypeInfo.put("order_count", 1);
			exsitOrderTypeInfo.put("first_order_time", dbOrder.get("ORDER_TIME"));
			exsitOrderTypeInfo.put("latest_order_time", dbOrder.get("ORDER_TIME"));
			exsitOrderTypeInfo.put("order_type", 0);
			orderTypeInfos.add(exsitOrderTypeInfo);
		}else {
			SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {
				Date firstOrderTime = dateformat.parse(exsitOrderTypeInfo.get("first_order_time").toString());;
				Date latestOrderTime = dateformat.parse(exsitOrderTypeInfo.get("latest_order_time").toString());;
				Date orderTime = dateformat.parse(dbOrder.get("ORDER_TIME").toString());
				Integer newOrderCount=(Integer) exsitOrderTypeInfo.get("order_count")+1;
				exsitOrderTypeInfo.put("order_count",newOrderCount );
				if(orderTime.before(firstOrderTime)) {
					exsitOrderTypeInfo.put("first_order_time", dbOrder.get("ORDER_TIME"));
				}
				if(orderTime.after(latestOrderTime)) {
					exsitOrderTypeInfo.put("latest_order_time", dbOrder.get("ORDER_TIME"));
				}
			} catch (ParseException e) {
				log.error("addNewOrderInfo parse time error dbOrder:{}",dbOrder,e);
			}
		}
		//用户行为数据处理
		if(userActions!=null) {
			for(Map<String,Object>userAction:userActions) {
				Object  businessUnitId=userAction.get("business_unit_id");
				Object  dbBusinessUnitId=dbOrder.get("business_unit_id");
				if(businessUnitId!=null&&dbBusinessUnitId!=null&&businessUnitId.toString().equals(dbBusinessUnitId.toString())) {
					userAction.put("have_promote_order", 1);
				}
			}
		}
	}
}
