package me.ttting.canal.sink.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import me.ttting.canal.sink.elasticsearch.customization.impl.RobotWechatServiceImpl;
import org.elasticsearch.action.update.UpdateRequest;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Preconditions;

import me.ttting.canal.sink.elasticsearch.customization.CustomizationService;
import me.ttting.canal.sink.elasticsearch.customization.impl.BizOrderOnlineServiceImpl;
import me.ttting.canal.sink.elasticsearch.customization.impl.BizOrderServiceImpl;
import me.ttting.canal.sink.elasticsearch.customization.impl.BizServiceImpl;
import me.ttting.canal.sink.elasticsearch.customization.impl.BizSourceServiceImpl;
import me.ttting.canal.sink.elasticsearch.customization.impl.BizUserActionServiceImpl;
import me.ttting.canal.sink.elasticsearch.parsersupport.ParserSupport;
import me.ttting.canal.sink.elasticsearch.parsersupport.SupportParam;
import me.ttting.canal.sink.elasticsearch.parsersupport.impl.DeleteToNull;
import me.ttting.canal.sink.elasticsearch.parsersupport.impl.EsDataIntegrateSupport;
import me.ttting.canal.sink.elasticsearch.parsersupport.impl.FileStringToArray;

/**
 * Created by jiangtiteng on 2018/10/18
 */
@Slf4j
public class DefaultFlatMessageParser implements FlatMessageParser {
	private static List<ParserSupport> supportList;
	private static Map<String, CustomizationService> customizationMap;

	static {
		supportList = new ArrayList<ParserSupport>();
		EsDataIntegrateSupport es = new EsDataIntegrateSupport();
		FileStringToArray sta = new FileStringToArray();
		DeleteToNull dt = new DeleteToNull();
		supportList.add(sta);
		supportList.add(es);
		supportList.add(dt);
		customizationMap=new HashMap<String, CustomizationService>();
		BizOrderServiceImpl BizOrderServiceImpl = new BizOrderServiceImpl();
		BizOrderOnlineServiceImpl BizOrderOnlineServiceImpl = new BizOrderOnlineServiceImpl();
		BizServiceImpl BizServiceImpl = new BizServiceImpl();
		BizSourceServiceImpl BizSourceServiceImpl = new BizSourceServiceImpl();
		BizUserActionServiceImpl BizUserActionServiceImpl = new BizUserActionServiceImpl();
		RobotWechatServiceImpl robotWechatService = new RobotWechatServiceImpl();
		customizationMap.put(BizOrderServiceImpl.getKey(), BizOrderServiceImpl);
		customizationMap.put(BizOrderOnlineServiceImpl.getKey(), BizOrderOnlineServiceImpl);
		customizationMap.put(BizServiceImpl.getKey(), BizServiceImpl);
		customizationMap.put(BizSourceServiceImpl.getKey(), BizSourceServiceImpl);
		customizationMap.put(BizUserActionServiceImpl.getKey(), BizUserActionServiceImpl);
		customizationMap.put(robotWechatService.getKey(), robotWechatService);
	}
	private Map<String, ESinkConfig> eSinkConfigMap;

	private static final DateTimeFormatter iso8601Formatter = ISODateTimeFormat.dateTime();

	private static final Chronology UTC_CHRONOLOGY = ISOChronology.getInstance(DateTimeZone.UTC);

	public DefaultFlatMessageParser(List<ESinkConfig> eSinkConfigs) {
		Preconditions.checkNotNull(eSinkConfigs, "esSinkConfig must not be null");
		Preconditions.checkArgument(eSinkConfigs.size() > 0, "esSinkConfig must not be null");
		eSinkConfigMap = eSinkConfigs.stream()
				.collect(Collectors.toMap(e -> e.getDatabase() + "::" + e.getTable(), e -> e));
	}

	@Override
	public String parsePrimaryKey(FlatMessage flatMessage, Map<String, String> data) {
		ESinkConfig eSinkConfig = eSinkConfigMap.get(configKeyForFlatMessage(flatMessage));
		String primaryKeyName = eSinkConfig.getPrimaryKeyName();
		if (primaryKeyName == null) {
             return data.get("ID");
		}
		String primaryKey = data.get(primaryKeyName);
		return primaryKey;
	}

	@Override
	public Map parseSource(FlatMessage flatMessage, Map<String, String> data) {
		if (data != null) {
			data.put("@timestamp", iso8601Formatter.print(new DateTime(UTC_CHRONOLOGY)));
		}
		String pk = parsePrimaryKey(flatMessage, data);
		Map result = data;
		ESinkConfig eSinkConfig = eSinkConfigMap.get(configKeyForFlatMessage(flatMessage));
		// 表的数据哪些字段需要存储到es
		Map<String, Map<String, String>> filedMappingMap = eSinkConfig.getFiledMappings();
		// 表中的数据存储在es需要额外转换一下
		Map<String, Map<String, String>> extraProcesss = eSinkConfig.getExtraProcesss();
		if (null != extraProcesss) {
			// 字段转换，如biz表中的exam_type字段(字符串)对应ES中exam_type_array字段(数组类型)
			result = getSourceExtraProcessData(flatMessage, result, extraProcesss, pk);
		}
		if (null != filedMappingMap) {
			// 如biz_source表中delivery_channel字段取哪些值对应SEM标签
			result = getSourceFileMappingData(flatMessage, result, filedMappingMap, pk);
		}
		if (null != eSinkConfig.getCustomizationKey()) {
			CustomizationService customizationService = customizationMap.get(eSinkConfig.getCustomizationKey());
			result=customizationService.excuteSingleSource(result, parseIndex(flatMessage), parseType(flatMessage), pk,data);
		}
		return result;
	}

	private Map getSourceExtraProcessData(FlatMessage flatMessage, Map<String, Object> data,
			Map<String, Map<String, String>> extraProcesss, String pk) {
		Map<String, Object> newData = new HashMap<String, Object>();
		data.forEach((k, v) -> {
			// 获取需要处理的字段数据处理配置
			Map<String, String> dealConfigMap = extraProcesss.get(k);
			if (null != dealConfigMap) {
				String parceSupportType = dealConfigMap.get("parseSupportType");
				String mappingfiled = dealConfigMap.get("mappingfiled");
				String mappingValue = dealConfigMap.get("mappingValue");
				String mutex = dealConfigMap.get("mutex");
				String nullReplace = dealConfigMap.get("nullReplace");
				// 字段名称转换,但是数据不需要处理
				for (ParserSupport support : supportList) {
					if (support.isMatch(parceSupportType)) {
						if (null != data.get(k) || nullReplace != null) {
							SupportParam param = SupportParam.builder().value(data.get(k))
									.index(parseIndex(flatMessage)).type(parseType(flatMessage)).id(pk)
									.mappingValue(mappingValue).mutex(mutex).nullReplace(nullReplace).build();
							Object newValue = support.dataProcess(param);
							if (null == mappingfiled) {
								newData.put(k, newValue);
							} else {
								newData.put(mappingfiled, newValue);
							}
						}
					}
				}
			}
		});
		data.putAll(newData);
		return data;
	}

	private Map getSourceFileMappingData(FlatMessage flatMessage, Map<String, Object> data,
			Map<String, Map<String, String>> filedMappingMap, String pk) {
		Map<String, Object> newData = new HashMap<String, Object>();
		FlatMessageType flatMessageType = FlatMessageType.safeValueof(flatMessage.getType());
		data.forEach((k, v) -> {
			// 获取需要处理的字段数据处理配置
			Map<String, String> dealConfigMap = filedMappingMap.get(k);
			if (null != dealConfigMap) {
				String mappingfiled = dealConfigMap.get("mappingfiled");
				String parseSupportType = dealConfigMap.get("parseSupportType");
				String mappingValue = dealConfigMap.get("mappingValue");
				String mutex = dealConfigMap.get("mutex");
				String nullReplace = dealConfigMap.get("nullReplace");
				// 字段名称转换,但是数据不需要处理
				if (null == parseSupportType) {
					newData.put(mappingfiled, data.get(k));
				} else {
					for (ParserSupport support : supportList) {
						if (support.isMatch(parseSupportType)) {
							if (null != data.get(k) || nullReplace != null) {
								SupportParam param = SupportParam.builder().key(mappingfiled).value(data.get(k))
										.index(parseIndex(flatMessage)).type(parseType(flatMessage)).id(pk)
										.mappingValue(mappingValue).mutex(mutex).nullReplace(nullReplace)
										.flatMessageType(flatMessageType).build();
								Object newValue = support.dataProcess(param);
								newData.put(mappingfiled, newValue);
							}
						}
					}
				}
			}
		});
		return newData;
	}

	@Override
	public String parseIndex(FlatMessage flatMessage) {
		ESinkConfig eSinkConfig = eSinkConfigMap.get(configKeyForFlatMessage(flatMessage));
		return eSinkConfig.getIndex() == null ? flatMessage.getTable() : eSinkConfig.getIndex();
	}

	@Override
	public String parseType(FlatMessage flatMessage) {
		ESinkConfig eSinkConfig = eSinkConfigMap.get(configKeyForFlatMessage(flatMessage));
		return eSinkConfig.getType() == null ? flatMessage.getTable() : eSinkConfig.getType();
	}

	@Override
	public boolean isConfigured(FlatMessage flatMessage) {
		return eSinkConfigMap.get(configKeyForFlatMessage(flatMessage)) == null ? false : true;
	}

	public String configKeyForFlatMessage(FlatMessage flatMessage) {
		return flatMessage.getDatabase() + "::" + flatMessage.getTable();
	}

	@Override
	public String parseTable(FlatMessage flatMessage) {
		ESinkConfig eSinkConfig = eSinkConfigMap.get(configKeyForFlatMessage(flatMessage));
		return eSinkConfig.getTable() == null ? flatMessage.getTable() : eSinkConfig.getTable();
	}

	@Override
	public boolean deleteChangeUpdate(FlatMessage flatMessage) {
		ESinkConfig eSinkConfig = eSinkConfigMap.get(configKeyForFlatMessage(flatMessage));
		if (eSinkConfig.getDeleteChangeUpdate() != null) {
			return true;
		}
		return false;
	}

	@Override
	public boolean parseMultipleResult(FlatMessage flatMessage) {
		ESinkConfig eSinkConfig = eSinkConfigMap.get(configKeyForFlatMessage(flatMessage));
		if (eSinkConfig.getParseMultipleResult() != null) {
			return true;
		}
		return false;
	}

	@Override
	public List<UpdateRequest> parseMultipleResultSource(FlatMessage flatMessage, Map<String, String> data,String index, String type) {
		List<UpdateRequest>result=new ArrayList<UpdateRequest>();
		ESinkConfig eSinkConfig = eSinkConfigMap.get(configKeyForFlatMessage(flatMessage));
		if (null != eSinkConfig.getCustomizationKey()) {
			CustomizationService customizationService = customizationMap.get(eSinkConfig.getCustomizationKey());
			List<Map<String,Object>>sources=customizationService.excute(data, eSinkConfig.getIndex(), eSinkConfig.getType());
			for(Map<String,Object>source:sources) {
				 if(source.get("ID")!=null) {
					 log.info("update id:{}, source:{}", source.get("ID").toString(), source);
					 UpdateRequest updateRequest = new UpdateRequest(index, type, source.get("ID").toString());
	                 updateRequest.doc(source);
	                 updateRequest.docAsUpsert(true);
	                 result.add(updateRequest);
				 }
			}
		}
		return result;
	}

	@Override
	public boolean submitRequest(FlatMessage flatMessage) {
		ESinkConfig eSinkConfig = eSinkConfigMap.get(configKeyForFlatMessage(flatMessage));
		if(eSinkConfig==null) {
			return false;
		}
		if (eSinkConfig.getSubmitRequest() != null) {
			return true;
		}
		return false;
	}
}
