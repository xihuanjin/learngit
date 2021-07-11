package me.ttting.canal.sink.elasticsearch.customization.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import lombok.extern.slf4j.Slf4j;
import me.ttting.canal.sink.elasticsearch.ResetHighLevelClientFactory;
import me.ttting.canal.sink.elasticsearch.customization.CustomizationService;

@Slf4j
public class BizServiceImpl extends CustomizationService {
	
	public  static List<String>notBindRepoTypes=Arrays.asList("0","2'","3");
	
	public String getKey() {
		return "biz";
	}
	
	@Override
	public Map<String, Object> excuteSingleSource(Map map, String index, String doc, String id,Map<String,String>originalData) {
		Map<String, Object> sourceResult = new HashMap<String, Object>();
		sourceResult.putAll(map);
		RestHighLevelClient client = ResetHighLevelClientFactory.getClient();
		GetRequest getRequest = new GetRequest(index, doc, id);
		try {
			if(sourceResult.get("REPO_TYPE")!=null&&notBindRepoTypes.contains(sourceResult.get("REPO_TYPE").toString())) {
				sourceResult.put("binding_operator_id", null);
				sourceResult.put("binding_time", null);
				sourceResult.put("binding_user_id", null);
				sourceResult.put("binding_node_id", null);
				sourceResult.put("rate", null);
				sourceResult.put("deadline", null);
			}
			GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
			Map<String, Object> source = response.getSource();
			if (null != source) {
				if (null != source.get("orderInfoList")) {
					List<Map<String, Object>> orderInfos = (List<Map<String, Object>>) source.get("orderInfoList");
					Set<String> mobiles = new HashSet<String>();
					if (null != source.get("MOBILE")) {
						mobiles.add(source.get("MOBILE").toString());
					}
					if (null != source.get("TEL") && !"".equals(source.get("TEL"))) {
						mobiles.add(source.get("TEL").toString());
					}
					List<Map<String, Object>> newOrderInfos = new ArrayList<Map<String, Object>>(orderInfos.size());
					for (Map<String, Object> orderInfo : orderInfos) {
						if (null != orderInfo.get("order_type")) {
							Integer orderType = (Integer) orderInfo.get("order_type");
							if (orderType == 0) {
								newOrderInfos.add(orderInfo);
							}
							if (orderType == 1 || orderType == 2) {
								if (mobiles.contains(orderInfo.get("mobile"))) {
									newOrderInfos.add(orderInfo);
								}
							}
						}
					}
					if (newOrderInfos.size() != orderInfos.size()) {
						sourceResult.put("orderInfoList", newOrderInfos);
						List<Map<String, Object>> orderTypeInfos = getOrderTypeInfosByOrders(newOrderInfos);
						sourceResult.put("orderTypeInfoList", orderTypeInfos);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("excuteSingleSource fail Map:{}",map,e);
		}
		return sourceResult;
	}

	private List<Map<String, Object>> getOrderTypeInfosByOrders(List<Map<String, Object>> newOrderInfos)
			throws Exception {
		List<Map<String, Object>> orderTypeInfos = new ArrayList<Map<String, Object>>();
		Map<Integer, Map<String, Object>> orderTypeTypeInfoMap = new HashMap<Integer, Map<String, Object>>();
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (Map<String, Object> orderInfo : newOrderInfos) {
			Map<String, Object> orderTypeInfo = orderTypeTypeInfoMap.get(orderInfo.get("order_type"));
			Integer orderCount = 0;
			Date firstOrderTime = null;
			Date latestOrderTime = null;
			if (orderTypeInfo == null) {
				orderTypeInfo = new HashMap<String, Object>();
				orderTypeInfos.add(orderTypeInfo);
				orderTypeTypeInfoMap.put((Integer) orderInfo.get("order_type"), orderTypeInfo);
				orderCount = 1;
				firstOrderTime = dateformat.parse(orderInfo.get("order_time").toString());
				latestOrderTime = dateformat.parse(orderInfo.get("order_time").toString());
			} else {
				orderCount=orderCount+1;
				firstOrderTime=dateformat.parse(orderTypeInfo.get("first_order_time").toString());
				latestOrderTime=dateformat.parse(orderTypeInfo.get("latest_order_time").toString());
				Date orderTime = dateformat.parse(orderInfo.get("order_time").toString());
				if(orderTime.before(firstOrderTime)) {
					firstOrderTime=orderTime;
				}
				if(orderTime.after(latestOrderTime)) {
					latestOrderTime=orderTime;
				}
			}
			orderTypeInfo.put("order_count", orderCount);
			orderTypeInfo.put("first_order_time", dateformat.format(firstOrderTime));
			orderTypeInfo.put("latest_order_time", dateformat.format(latestOrderTime));
			orderTypeInfo.put("order_type", orderInfo.get("order_type"));
		}
		return orderTypeInfos;
	}
}
