package com.alibaba.middleware.race.mom;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message ,目前只需要 三个属性,用于消费者消费(可能有遗漏,根据消费者消费即可)
 * @author youngforever
 */
public class Message implements Serializable{

	private static final long serialVersionUID = 1982349723732590L;
	private transient static  AtomicLong msgIdProduce=new AtomicLong();
	/**
	 * 测试严证,消费等需要的必要字段
	 */
	private String msgId;
	private String topic;
	private byte[] body;
	private Map<String, String> properties = new HashMap<String, String>();
	private long bornTime;
	
	
//	public void makeID(){
//		if(msgId==null){
//			msgId=this.topic+this.properties.toString()+"_"+String.valueOf(msgIdProduce.incrementAndGet());
//		}
//	}
	
	public String getTopic() {
		return topic;
	}
	public Message(){}
	public Message( String topic, byte[] body,
			Map<String, String> properties, long bornTime) {
		this.topic = topic;
		this.body = body;
		this.properties = properties;
		this.bornTime = bornTime;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public byte[] getBody() {
		return body;
	}
	public void setBody(byte[] body) {
		this.body = body;
	}
	public Map<String, String> getProperties() {
		return properties;
	}
	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}
	public void setProperty(String key,String property){
		this.properties.put(key, property);
	}
	public String getProperty(String key){
		return this.properties.get(key);
	}
	public long getBornTime() {
		return bornTime;
	}
	public void setBornTime(long bornTime) {
		this.bornTime = bornTime;
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}
	public String toString(){
		return this.msgId;
	}
	
}
