package me.ttting.canal.sink.elasticsearch.parsersupport;

/**
 * 对source传递的过来的数据解析之后进一步处理
 *
 */
public abstract class ParserSupport {
	 
	public abstract boolean isMatch(String supportType);
	
   public abstract Object  dataProcess(SupportParam supportParam);
	   
}
