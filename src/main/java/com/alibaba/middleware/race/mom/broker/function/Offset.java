package com.alibaba.middleware.race.mom.broker.function;

import java.io.Serializable;
import java.util.Map;

import com.alibaba.middleware.race.mom.util.TopicAndFilter;

public class Offset implements Serializable{
	private static final long serialVersionUID = -7988990980809880981L;
	/**
	 * 消息信息,生产者集群信息
	 */
	private String topic;
	private Map<String,String> filter;
//	private TopicAndFilter topicAndFilter;
//	private String groupId;
	/**
	 * 存储采用多队列多文件时采用
	 */
	private int queueIndex;
	/**
	 * 消费者集群信息,姑且不需要,因为恢复 会重新发起订阅
	 */
	
	/**
	 * 与存储顺序(拉取索引)对应的 消费进度表示
	 */
	int currentOffset=-1; //当前消费进度
	/**
	 * 存储成功的最大maxOffset;msg 的offset>此maxOffset,即为加入存储队列但是尚未存储(强行force)到磁盘,保证存储成功后,才来调用设置这个值
	 */
	int maxOffset=-1; //最大偏移(size)
	
	
	public Offset(){
		
	};
//	public Offset(TopicAndFilter topicAndFilter){
//		this.topicAndFilter=topicAndFilter;
//	}
	
	/*function*/
	public void addCacheMaxOffsetBy(int size)
	{
//		this.cacheOffset+=size;
	}
	public  void addMaxOffsetBy(int size)
	{
		this.maxOffset+=size;
	}
	
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public Map<String, String> getFilter() {
		return filter;
	}
	public void setFilter(Map<String, String> filter) {
		this.filter = filter;
	}
	public  void setCurrentOffsetBy(int size)
	{
		if(size>this.currentOffset)
			this.currentOffset=size;
	}
//	public String getGroupId() {
//		return groupId;
//	}
//	public void setGroupId(String groupId) {
//		this.groupId = groupId;
//	}
	public int getQueueIndex() {
		return queueIndex;
	}
	public void setQueueIndex(int queueIndex) {
		this.queueIndex = queueIndex;
	}
	public int getCurrentoffset() {
		return currentOffset;
	}
	public void setCurrentoffset(int currentoffset) {
		this.currentOffset = currentoffset;
	}
//	public int getCacheOffset() {
//		return cacheOffset;
//	}
//	public void setCacheOffset(int cacheOffset) {
//		this.cacheOffset = cacheOffset;
//	}
	public int getMaxOffset() {
		return maxOffset;
	}
	public void setMaxOffset(int maxOffset) {
		this.maxOffset = maxOffset;
	}
//	public String getTopic() {
//		return topic;
//	}
//	public void setTopic(String topic) {
//		this.topic = topic;
//	}
//	public TopicAndFilter getTopicAndFilter() {
//		return topicAndFilter;
//	}
//	public void setTopicAndFilter(TopicAndFilter topicAndFilter) {
//		this.topicAndFilter = topicAndFilter;
//	}
//	@Override
//	public String toString() {
//		return "Offset: [topicAndFilter=" + topicAndFilter  + ", queueIndex=" + queueIndex
//				+ ", currentoffset=" + currentOffset + ",  MaxOffset=" + maxOffset
//				+ "]";
//	}
	@Override
	public String toString() {
		return "Offset: [topic=" + topic+"filter:"+filter  + ", queueIndex=" + queueIndex
				+ ", currentoffset=" + currentOffset + ",  MaxOffset=" + maxOffset
				+ "]";
	}
//	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + maxOffset;
		result = prime * result + currentOffset;
//		result = prime * result + ((queueIndex == 0) ? 0 : queueIndex.hashCode());
//		result = prime * result + ((topicAndFilter == null) ? 0 : topicAndFilter.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		result = prime * result + ((filter == null) ? 0 : filter.hashCode());
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
		Offset other = (Offset) obj;
		if (maxOffset != other.maxOffset)
			return false;
//		if (cacheOffset != other.cacheOffset)
//			return false;
		if (currentOffset != other.currentOffset)
			return false;
//		if (topicAndFilter == null) {
//			if (other.topicAndFilter != null)
//				return false;
//		} else if (!topicAndFilter.equals(other.topicAndFilter))
//			return false;
		if (queueIndex != other.queueIndex) {
				return false;
		} 
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}
	
	
		
		
}
