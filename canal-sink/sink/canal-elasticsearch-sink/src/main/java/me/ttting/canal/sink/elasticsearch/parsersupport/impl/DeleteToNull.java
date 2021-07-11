package me.ttting.canal.sink.elasticsearch.parsersupport.impl;

import me.ttting.canal.sink.elasticsearch.FlatMessageType;
import me.ttting.canal.sink.elasticsearch.parsersupport.ParserSupport;
import me.ttting.canal.sink.elasticsearch.parsersupport.SupportParam;

public class DeleteToNull extends ParserSupport {

	private String parseSupportType = "deleteToNull";

	@Override
	public boolean isMatch(String supportType) {
		return this.parseSupportType.equals(supportType);
	}

	@Override
	public Object dataProcess(SupportParam supportParam) {
		if(supportParam.getFlatMessageType().equals(FlatMessageType.DELETE)) {
			return null;
		}
		return supportParam.getValue();
	}
}
