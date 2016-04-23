package com.alibaba.middleware.race.mom;

import java.io.Serializable;

import com.alibaba.middleware.race.mom.util.TopicAndFilter;

public class ConsumeResult implements Serializable {
	private static final long serialVersionUID = -4489150726886715441L;
	private ConsumeStatus status=ConsumeStatus.FAIL;
	private String info;
	private String msgId;
	//来自哪个订阅集群
	private String groupId;
	//来自哪 些 订阅集群组
	private TopicAndFilter topicAndFilter;
	
	
	public String getGroupId() {
		return groupId;
	}
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	public TopicAndFilter getTopicAndFilter() {
		return topicAndFilter;
	}
	public void setTopicAndFilter(TopicAndFilter topicAndFilter) {
		this.topicAndFilter = topicAndFilter;
	}
	public void setStatus(ConsumeStatus status) {
		this.status = status;
	}
	public ConsumeStatus getStatus() {
		return status;
	}
	public void setInfo(String info) {
		this.info = info;
	}
	public String getInfo() {
		return info;
	}
	public String getMsgId() {
		return msgId;
	}
	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}
	
}
