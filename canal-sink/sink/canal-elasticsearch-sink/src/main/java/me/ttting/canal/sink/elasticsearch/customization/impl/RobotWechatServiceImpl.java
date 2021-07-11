package me.ttting.canal.sink.elasticsearch.customization.impl;

import lombok.extern.slf4j.Slf4j;
import me.ttting.canal.sink.elasticsearch.ResetHighLevelClientFactory;
import me.ttting.canal.sink.elasticsearch.customization.CustomizationService;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: chencs
 * @date: 2021/3/4 19:29
 */
@Slf4j
public class RobotWechatServiceImpl extends CustomizationService {
    @Override
    public String getKey() {
        return "biz_robot_relation";
    }

    @Override
    public Map<String, Object> excuteSingleSource(Map maps, String index, String doc, String id, Map<String, String> originalData) {
        Map<String, Object> result = new HashMap<>();
        // 查询指定ID的文档数据
        RestHighLevelClient client = ResetHighLevelClientFactory.getClient();
        GetRequest getRequest = new GetRequest(index, doc, id);
        try {
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            Map<String, Object> source = response.getSource();
            if (null != source) {
                // 获取文档的wechatRobotList字段值
                List<Map<String, Object>> wechatRobotList = null;
                if (source.get("wechatRobotList") == null) {
                    wechatRobotList = new ArrayList<>();
                } else {
                    wechatRobotList = (List<Map<String, Object>>) source.get("wechatRobotList");
                }
                log.info("id:{}, wechatRobotList:{}", id, wechatRobotList);
                boolean exist = false;
                for (Map<String, Object> wechatRobot : wechatRobotList) {
                    if (wechatRobot.get("robot_wx_id") != null && wechatRobot.get("robot_wx_id").toString().equals(originalData.get("robot_wx_id"))) {
                        exist = true;
                    }
                }
                if (!exist) {
                    String friend_relation = originalData.get("friend_relation");
                    if ("1".equals(friend_relation)) {
                        // 只有添加成功的微信好友才加入ES
                        Map<String, Object> wechatRobot = new HashMap<>();
                        wechatRobot.put("robot_wx_id", originalData.get("robot_wx_id"));
                        wechatRobot.put("robot_wx_no", originalData.get("robot_wx_no"));
                        wechatRobot.put("robot_wx_name", originalData.get("robot_wx_name"));
                        wechatRobotList.add(wechatRobot);
                        result.put("wechatRobotList", wechatRobotList);
                        result.put("addWechatRobot", 1);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("RobotWechatServiceImpl excuteSingleSource fail originalData:{}", originalData, e);
        }

        log.info("result:{}", result);
        return result;
    }
}
