package me.ttting.canal.sink.elasticsearch.parsersupport.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import me.ttting.canal.sink.elasticsearch.ResetHighLevelClientFactory;
import me.ttting.canal.sink.elasticsearch.parsersupport.ParserSupport;
import me.ttting.canal.sink.elasticsearch.parsersupport.SupportParam;

public class EsDataIntegrateSupport extends ParserSupport {

	private String parseSupportType = "esDataIntegrate";

	@Override
	public boolean isMatch(String supportType) {
		return this.parseSupportType.equals(supportType);
	}

	@SuppressWarnings({ "null", "unchecked" })
	@Override
	public Object dataProcess(SupportParam supportParam) {
		RestHighLevelClient client = ResetHighLevelClientFactory.getClient();
		GetRequest getRequest = new GetRequest(supportParam.getIndex(),supportParam.getType(), supportParam.getId());
		List<String> esValue = new ArrayList<String>();
		if (supportParam.getMappingValue() == null && null ==supportParam.getNullReplace()) {
			return null;
		}
		try {
			GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
			Map<String, Object> source = response.getSource();
			if (null != source) {
				if (null != source.get(supportParam.getKey())) {
					esValue = (List<String>) source.get(supportParam.getKey());
				}
			}
			if (null == supportParam.getValue()) {
				if (supportParam.getNullReplace() != null) {
					esValue.remove(supportParam.getNullReplace());
				}
				return esValue;
			}
			Map<String, String> mutexMap = new HashMap<String, String>();
			if (null != supportParam.getMutex()) {
				String[] mutexArray = supportParam.getMutex().split("\\|");
				for (String mutexStr : mutexArray) {
					String[] mutexRel = mutexStr.split(":");
					mutexMap.put(mutexRel[0], mutexRel[1]);
				}
			}
			String[] mappings = supportParam.getMappingValue().split("\\|");
			for (String mapping : mappings) {
				String[] mappingArr = mapping.split(":");
				if (mappingArr[0].equals(supportParam.getValue())) {
					if (null == esValue) {
						esValue.add(mappingArr[1]);
					} else {
						if (esValue.contains(mutexMap.get(mappingArr[1]))) {
							esValue.remove(mutexMap.get(mappingArr[1]));
						}
						if (!esValue.contains(mappingArr[1])) {
							esValue.add(mappingArr[1]);
						}
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return esValue;
	}
}
