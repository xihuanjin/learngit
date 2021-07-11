package me.ttting.canal.sink.elasticsearch.parsersupport.impl;

import java.util.ArrayList;
import java.util.List;

import me.ttting.canal.sink.elasticsearch.parsersupport.ParserSupport;
import me.ttting.canal.sink.elasticsearch.parsersupport.SupportParam;

public class FileStringToArray extends ParserSupport{
	private String parseSupportType = "stringToArray";
	
	@Override
	public boolean isMatch(String supportType) {
		return this.parseSupportType.equals(supportType);
	}

	@Override
	public Object dataProcess(SupportParam supportParam) {
		if(null==supportParam.getValue()) {
			return supportParam.getNullReplace();
		}
		String[]arr= ((String)supportParam.getValue()).split(",");
		List<String>result=new ArrayList<String>();
		for(String str:arr) {
			if(!"".equals(str.trim())) {
				result.add(str);
			}
		}
		return result;
	}
	
}
