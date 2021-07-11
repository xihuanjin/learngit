package me.ttting.canal.sink.elasticsearch.parsersupport;

import lombok.Builder;
import lombok.Data;
import me.ttting.canal.sink.elasticsearch.FlatMessageType;

@Data
@Builder
public class SupportParam {
	private String key;
	private Object value;
	private String index;
	private String type;
	private String id;
	private String mappingValue;
	private String mutex;
	private String nullReplace;
	private FlatMessageType flatMessageType;
}
