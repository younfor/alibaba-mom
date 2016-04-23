package com.alibaba.middleware.race.mom.store;


import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5295808332504208830L;
	private String topic;
	private byte[] body;
	//全局唯一的消息id，不同消息不能重复
	private String msgId;
	private int offset;
	private long bornTime;
	
	private Map<String, String> properties = new HashMap<String, String>();

	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getMsgId() {
		return msgId;
	}
	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}
	public String getTopic() {
		return topic;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public byte[] getBody() {
		return body;
	}

	public String getProperty(String key) {
		return properties.get(key);
	}
	/**
	 * 设置消息属性
	 * @param key
	 * @param value
	 */
	public void setProperty(String key, String value) {
		properties.put(key, value);
	}
	/**
	 * 删除消息属性
	 * @param key
	 */
	public void removeProperty(String key) {
		properties.remove(key);
	}
	public long getBornTime() {
		return bornTime;
	}
	public void setBornTime(long bornTime) {
		this.bornTime = bornTime;
	}
	public Map<String, String> getProperties() {
		return properties;
	}
	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(body);
		result = prime * result + (int) (bornTime ^ (bornTime >>> 32));
		result = prime * result + ((msgId == null) ? 0 : msgId.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		if (!Arrays.equals(body, other.body))
			return false;
		if (bornTime != other.bornTime)
			return false;
		if (msgId == null) {
			if (other.msgId != null)
				return false;
		} else if (!msgId.equals(other.msgId))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "Message [topic=" + topic + ", body=" + Arrays.toString(body) + ", msgId=" + msgId + ", bornTime="
				+ bornTime + ", properties=" + properties + "]";
	}
	
	

}
