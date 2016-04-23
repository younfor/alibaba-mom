
package com.alibaba.middleware.race.mom.util;

import java.io.Serializable;
import java.util.Map;
/**
 * topic and filter的封装,一种TopicAndFilter代表一种消息,来自同一生产者集群组
 * 订阅者集群组 订阅某一TopicAndFilter(注意,订阅者filter为null,则订阅topic下所有filter消息)
 * 使用注意:构建订阅者TopicAndFilter须根据订阅者信息; 构建生产者或消息的TopicAndFilter须根据生产者的信息(系统大量使用TopicAndFilter为map键,构建绝对不能出错)
 * @author youngforever
 *
 */
public class TopicAndFilter implements Serializable{
	private static final long serialVersionUID = -7983983900930009841L;
	private String topic;
	private Map<String,String> filter;
	
	public TopicAndFilter(){};

	public TopicAndFilter(String topic, Map<String,String> filter) {
		if (topic == null) {
			throw new IllegalArgumentException("Left value is not effective.");
		}
		//允许filter为null
//		if (filter == null) {
//			throw new IllegalArgumentException("Right value is not effective.");
//		}
		this.topic = topic;
		this.filter = filter;
	}
//	public TopicAndFilter(String topicAndFilter) {
//
//	}

	public String getTopic() {
		return topic;
	}

	public Map<String,String> getFilter() {
		return filter;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		result = prime * result + ((filter == null) ? 0 : filter.hashCode());
		return result;
	}
//	public int hashCode() {
//		if(right!=null){
//			return (left.toString()+right.toString()).hashCode();
//		}else return left.toString().hashCode();
//	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof TopicAndFilter))
			return false;
		TopicAndFilter other = (TopicAndFilter) obj;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		if (filter == null) {
			if (other.filter != null)
				return false;
		} else if (!filter.equals(other.filter))
			return false;
		return true;
	}

	@Override
	public String toString() {
		if(filter!=null)
			return  topic + filter.toString() ;
		else return  topic +"null";
	}

}
