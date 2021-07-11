package me.ttting.canal.sink.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jiangtiteng on 2018/10/18
 */
@Slf4j
public class DefaultDocWriteRequestFactory implements DocWriteRequestFactory {
	private FlatMessageParser flatMessageParser;

	public DefaultDocWriteRequestFactory(FlatMessageParser flatMessageParser) {
		this.flatMessageParser = flatMessageParser;
	}

	@Override
	public List<DocWriteRequest> buildWithFlatMessage(FlatMessage flatMessage) {
		FlatMessageType flatMessageType = FlatMessageType.safeValueof(flatMessage.getType());
		if (!flatMessageParser.isConfigured(flatMessage)) {
//            log.info("FlatMessage is not configured, flatMessage :{}", flatMessage.toString());
			return null;
		}

		switch (flatMessageType) {
		case INSERT:
		case UPDATE:
			return buildForUpsert(flatMessage);
		case DELETE:
			if (flatMessageParser.deleteChangeUpdate(flatMessage)) {
			    // 删除SQL对应ES的更新操作，如biz_binding_info的删除对应ES中索引的相关字段设置成null
				return buildForUpsert(flatMessage);
			}
			return buildForDelete(flatMessage);
		default:
			throw new UnsupportMessageTypeException(flatMessage);
		}
	}

	protected List<DocWriteRequest> buildForUpsert(FlatMessage flatMessage) {
		List<DocWriteRequest> docWriteRequests = new LinkedList<>();

		String index = flatMessageParser.parseIndex(flatMessage);
		String type = flatMessageParser.parseType(flatMessage);

		for (Map<String, String> data : flatMessage.getData()) {
			if (flatMessageParser.parseMultipleResult(flatMessage)) {
				List<UpdateRequest> requests = flatMessageParser.parseMultipleResultSource(flatMessage, data, index,
						type);
				docWriteRequests.addAll(requests);
			} else {
				Map source = flatMessageParser.parseSource(flatMessage, data);
				String id = flatMessageParser.parsePrimaryKey(flatMessage, data);
				if (id != null) {
                    log.info("update id:{}, source:{}", id, source);
					UpdateRequest updateRequest = new UpdateRequest(index, type, id);
					updateRequest.doc(source);
					updateRequest.docAsUpsert(true);
					docWriteRequests.add(updateRequest);
				}
			}
		}
		return docWriteRequests;
	}

	protected List<DocWriteRequest> buildForDelete(FlatMessage flatMessage) {
		List<DocWriteRequest> docWriteRequests = new LinkedList<>();

		String index = flatMessageParser.parseIndex(flatMessage);
		String type = flatMessageParser.parseType(flatMessage);

		for (Map<String, String> data : flatMessage.getData()) {
			String id = flatMessageParser.parsePrimaryKey(flatMessage, data);
            log.info("delete id:{}, data:{}", id, data);
			DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
			docWriteRequests.add(deleteRequest);
		}
		return docWriteRequests;
	}

	@Override
	public boolean bolSubmitRequest(FlatMessage flatMessage) {
		if (flatMessageParser.submitRequest(flatMessage)) {
			return true;
		}
		return false;
	}
}
