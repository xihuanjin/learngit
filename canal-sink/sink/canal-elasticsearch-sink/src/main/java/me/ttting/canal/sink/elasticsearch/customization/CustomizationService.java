package me.ttting.canal.sink.elasticsearch.customization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class CustomizationService {
	public abstract String getKey();
	
	public List<Map<String,Object>> excute(Map maps,String index,String doc){
		List<Map<String,Object>>result=new ArrayList<Map<String,Object>>();
		return result;
	}
	
	public Map<String,Object> excuteSingleSource(Map maps,String index,String doc,String id,Map<String,String>originalData){
		Map<String,Object>result=new HashMap<String,Object>();
		return result;
	}
}
