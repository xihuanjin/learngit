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
public class BizOrderOnlineServiceImpl extends CustomizationService {
	public String getKey() {
		return "bizOnlineOrder";
	}

	@Override
	public List<Map<String, Object>> excute(Map map, String index, String doc) {
		List<Map<String, Object>> sourceResult = new ArrayList<Map<String, Object>>();
		String syncOrderType = map.get("sync_order_type").toString();
		Integer dbOrderType = null;
		if ("online".equals(syncOrderType)) {
			dbOrderType = 1;
		}
		if ("book".equals(syncOrderType)) {
			dbOrderType = 2;
		}
		if (dbOrderType == null) {
			return sourceResult;
		}
		RestHighLevelClient client = ResetHighLevelClientFactory.getClient();
		// 订单表中只存储了教育商机id，索引通过手机号找到对应每个业务线的商机数据
		SearchRequest request = new SearchRequest(index);
		request.types(doc);
		Object mobile = map.get("mobile");
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
				boolean isDelOrder = false;
				Object orderInfosObj = result.get("orderInfoList");
				List<Map<String, Object>> orderInfos = null;
				if (orderInfosObj == null) {
					orderInfos = new ArrayList<Map<String, Object>>();
				} else {
					orderInfos = (List<Map<String, Object>>) orderInfosObj;
				}
				for (Map<String, Object> orderInfo : orderInfos) {
					Object orderCode = orderInfo.get("order_code");
					Integer orderType = (Integer) orderInfo.get("order_type");
					if (orderType != dbOrderType) {
						continue;
					}
					Object dbOrderCode = map.get("order_code");
					// 订单号相等
					if (orderCode.equals(dbOrderCode)) {
						isExsitOrder = true;
						if (map.get("pay_status") == null || map.get("pay_status").equals("已取消")) {
							isDelOrder = true;
						} else {
							Object dbReayPay = map.get("real_pay");
							Object orderInfoReayPay = orderInfo.get("real_pay");
							Object dbRefund = map.get("refund");
							Object orderInfoRefund = orderInfo.get("refund");
							Object dbMobile = map.get("mobile");
							Object orderMobile = orderInfo.get("mobile");
							if (orderInfoReayPay != null && orderInfoReayPay.equals(dbReayPay) && dbMobile != null
									&& dbMobile.equals(orderMobile) && orderInfoRefund != null
									&& orderInfoRefund.equals(dbRefund)) {
								return sourceResult;
							} else {
								// 更新
								log.error("dbRefund{}",dbRefund);;
								orderInfo.put("real_pay", dbReayPay);
								orderInfo.put("mobile", dbMobile);
								orderInfo.put("refund", dbRefund);
							}
						}
					}
				}
				Object orderTypeInfosObj = result.get("orderTypeInfoList");
				List<Map<String, Object>> orderTypeInfos = null;
				if (orderTypeInfosObj == null) {
					orderTypeInfos = new ArrayList<Map<String, Object>>();
				} else {
					orderTypeInfos = (List<Map<String, Object>>) orderTypeInfosObj;
				}
				String id = hit.getId();
				if (!isExsitOrder) {
					addNewOrderInfo(map, orderInfos, orderTypeInfos, dbOrderType);
				}
				if (isDelOrder) {
					delOrderInfo(map, orderInfos, orderTypeInfos, dbOrderType);
				}
				Map<String, Object> sourceMap = new HashMap<String, Object>();
				sourceMap.put("orderTypeInfoList", orderTypeInfos);
				sourceMap.put("orderInfoList", orderInfos);
				sourceMap.put("ID", id);
				sourceResult.add(sourceMap);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sourceResult;
	}

	private void delOrderInfo(Map dbOrder, List<Map<String, Object>> orderInfos,
			List<Map<String, Object>> orderTypeInfos, Integer dbOrderType) {
		// 订单数据处理
		if (orderInfos == null) {
			orderInfos = new ArrayList<Map<String, Object>>();
		}
		List<Map<String, Object>> newOrderInfos = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> orderInfo : orderInfos) {
			if (!orderInfo.get("order_code").equals(dbOrder.get("order_code"))) {
				newOrderInfos.add(orderInfo);
			}
		}
		orderInfos.clear();
		orderInfos.addAll(newOrderInfos);
		// 订单类型数据处理
		if (orderTypeInfos == null) {
			orderTypeInfos = new ArrayList<Map<String, Object>>();
		}
		Map<String, Object> exsitOrderTypeInfo = null;
		for (Map<String, Object> orderTypeInfo : orderTypeInfos) {
			Object orderType = orderTypeInfo.get("order_type");
			if (orderType != null && ((Integer) orderType).equals(dbOrderType)) {
				exsitOrderTypeInfo = orderTypeInfo;
				break;
			}
		}
		if (exsitOrderTypeInfo != null) {
			if ((Integer) exsitOrderTypeInfo.get("order_count") == 1) {
				orderTypeInfos.remove(exsitOrderTypeInfo);
			} else {
				SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				try {
					Date firstOrderTime = null;
					Date latestOrderTime = null;
					int newOrderCount =0;
					for (Map<String, Object> newOrderInfo : orderInfos) {
						if (newOrderInfo.get("order_type").toString()
								.equals(exsitOrderTypeInfo.get("order_type").toString())) {
							newOrderCount++;
							Date tempTime = dateformat.parse(newOrderInfo.get("order_time").toString());
							if (firstOrderTime == null) {
								firstOrderTime = tempTime;
							} else {
								if (tempTime.before(firstOrderTime)) {
									firstOrderTime = tempTime;
								}
							}
							if (latestOrderTime == null) {
								latestOrderTime = tempTime;
							} else {
								if (latestOrderTime.before(tempTime)) {
									latestOrderTime = tempTime;
								}
							}
						}
					}
					
					exsitOrderTypeInfo.put("order_count", newOrderCount);
					exsitOrderTypeInfo.put("first_order_time", dateformat.format(firstOrderTime));
					exsitOrderTypeInfo.put("latest_order_time", dateformat.format(latestOrderTime));
				} catch (ParseException e) {
					log.error("delNewOrderInfo parse time error dbOrder:{}", dbOrder, e);
				}
			}
		}
	}

	private void addNewOrderInfo(Map dbOrder, List<Map<String, Object>> orderInfos,
			List<Map<String, Object>> orderTypeInfos, Integer dbOrderType) {
		if (dbOrder.get("pay_status") == null || dbOrder.get("pay_status").equals("已取消")) {
			return;
		}
		// 订单数据处理
		if (orderInfos == null) {
			orderInfos = new ArrayList<Map<String, Object>>();
		}
		Map<String, Object> orderInfo = new HashMap<String, Object>();
		orderInfo.put("order_code", dbOrder.get("order_code"));
		orderInfo.put("real_pay", dbOrder.get("real_pay"));
		orderInfo.put("refund", dbOrder.get("refund"));
		orderInfo.put("user_no", dbOrder.get("ehr_staff_no"));
		orderInfo.put("order_time", dbOrder.get("order_time"));
		orderInfo.put("order_type", dbOrderType);
		orderInfo.put("mobile", dbOrder.get("mobile"));
		orderInfos.add(orderInfo);
		// 订单类型数据处理
		if (orderTypeInfos == null) {
			orderTypeInfos = new ArrayList<Map<String, Object>>();
		}
		Map<String, Object> exsitOrderTypeInfo = null;
		for (Map<String, Object> orderTypeInfo : orderTypeInfos) {
			Object orderType = orderTypeInfo.get("order_type");
			if (orderType != null && ((Integer) orderType).equals(dbOrderType)) {
				exsitOrderTypeInfo = orderTypeInfo;
				break;
			}
		}
		if (exsitOrderTypeInfo == null) {
			exsitOrderTypeInfo = new HashMap<String, Object>();
			exsitOrderTypeInfo.put("order_count", 1);
			exsitOrderTypeInfo.put("first_order_time", dbOrder.get("order_time"));
			exsitOrderTypeInfo.put("latest_order_time", dbOrder.get("order_time"));
			exsitOrderTypeInfo.put("order_type", dbOrderType);
			orderTypeInfos.add(exsitOrderTypeInfo);
		} else {
			SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			try {
				Date firstOrderTime = dateformat.parse(exsitOrderTypeInfo.get("first_order_time").toString());
				;
				Date latestOrderTime = dateformat.parse(exsitOrderTypeInfo.get("latest_order_time").toString());
				;
				Date orderTime = dateformat.parse(dbOrder.get("order_time").toString());
				Integer newOrderCount = (Integer) exsitOrderTypeInfo.get("order_count") + 1;
				exsitOrderTypeInfo.put("order_count", newOrderCount);
				if (orderTime.before(firstOrderTime)) {
					exsitOrderTypeInfo.put("first_order_time", dbOrder.get("order_time"));
				}
				if (orderTime.after(latestOrderTime)) {
					exsitOrderTypeInfo.put("latest_order_time", dbOrder.get("order_time"));
				}
			} catch (ParseException e) {
				log.error("addNewOrderInfo parse time error dbOrder:{}", dbOrder, e);
			}
		}
	}
}
