package me.ttting.canal.sink.elasticsearch;

import com.google.common.base.Preconditions;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * Created by jiangtiteng on 2018/10/22
 */
public class ResetHighLevelClientFactory {
	//RestHighLevelClient 单例情况下，能保证线程安全
	private static RestHighLevelClient restHighLevelClient;
	
    public static RestHighLevelClient createClient(String hostNames, String protocol) {
    	if(null!=restHighLevelClient) {
    		return restHighLevelClient;
    	}
    	synchronized (ResetHighLevelClientFactory.class) {
    		if(null!=restHighLevelClient) {
        		return restHighLevelClient;
        	}
    		String[] hostNameArray = hostNames.split(",");
            Preconditions.checkNotNull(hostNameArray, "hostNameArray");
            Preconditions.checkArgument(hostNameArray.length > 0, "hostNames must special");
            Preconditions.checkNotNull(protocol, "protocol");
            HttpHost[] httpHosts = new HttpHost[hostNameArray.length];
            for (int i = 0; i < hostNameArray.length; i++) {
                String hostName = hostNameArray[i];
                String[] hostInfos = hostName.split(":");

                httpHosts[i] = new HttpHost(hostInfos[0], Integer.parseInt(hostInfos[1]), protocol);
            }
            restHighLevelClient =new RestHighLevelClient(RestClient.builder(httpHosts));
		}
        return restHighLevelClient;
    }
    
    public static RestHighLevelClient getClient() {
    	return restHighLevelClient;
    }
}
