package me.ttting.canal.sink.elasticsearch;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.update.UpdateRequest;

/**
 * Created by jiangtiteng on 2018/10/18
 */
public interface FlatMessageParser {
    String parsePrimaryKey(FlatMessage flatMessage, Map<String, String> data);

    Map  parseSource(FlatMessage flatMessage, Map<String, String> data);

    String parseIndex(FlatMessage flatMessage);

    String parseType(FlatMessage flatMessage);
    
    String parseTable(FlatMessage flatMessage);

    boolean isConfigured(FlatMessage flatMessage);
    
    boolean deleteChangeUpdate(FlatMessage flatMessage);
    
    boolean parseMultipleResult(FlatMessage flatMessage);

	List<UpdateRequest> parseMultipleResultSource(FlatMessage flatMessage, Map<String, String> data, String index, String type);

	boolean submitRequest(FlatMessage flatMessage);
}
