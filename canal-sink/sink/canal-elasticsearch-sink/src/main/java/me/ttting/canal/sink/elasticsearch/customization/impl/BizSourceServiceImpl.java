package me.ttting.canal.sink.elasticsearch.customization.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import lombok.extern.slf4j.Slf4j;
import me.ttting.canal.sink.elasticsearch.ResetHighLevelClientFactory;
import me.ttting.canal.sink.elasticsearch.customization.CustomizationService;
@Slf4j
public class BizSourceServiceImpl extends CustomizationService{

	@Override
	public String getKey() {
		return "bizSource";
	}
	
	@Override
	public Map<String, Object> excuteSingleSource(Map map, String index, String doc, String id,Map<String,String>originalData) {
		Map<String, Object> sourceResult = new HashMap<String, Object>();
		sourceResult.putAll(map);
		RestHighLevelClient client = ResetHighLevelClientFactory.getClient();
		GetRequest getRequest = new GetRequest(index, doc, id);
		try {
			GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
			Map<String, Object> source = response.getSource();
			if (null != source) {
				List<Map<String, Object>> sourceInfos = null;
				if (source.get("sourceInfoList") == null) {
					sourceInfos = new ArrayList<Map<String, Object>>();
				} else {
					sourceInfos = (List<Map<String, Object>>) source.get("sourceInfoList");
				}
				boolean isExsitSource=false;
				for(Map<String, Object> sourceInfo:sourceInfos) {
					if(sourceInfo.get("bizSourceId")!=null&&sourceInfo.get("bizSourceId").toString().equals(originalData.get("ID"))) {
						isExsitSource=true;
					}
				}
				if(!isExsitSource) {
					Map<String,Object>sourceInfo=new HashMap<String,Object>();
					sourceInfo.put("source", originalData.get("SOURCE"));
					sourceInfo.put("refreshTime", originalData.get("REFRESH_TIME"));
					sourceInfo.put("bizSourceId", originalData.get("ID"));
					sourceInfos.add(sourceInfo);
					sourceResult.put("sourceInfoList",sourceInfos);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("BizSourceServiceImpl excuteSingleSource fail originalData:{}",originalData,e);
		}
		return sourceResult;
	}
}
