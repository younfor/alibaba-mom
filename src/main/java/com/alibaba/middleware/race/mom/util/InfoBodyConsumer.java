package com.alibaba.middleware.race.mom.util;

import java.io.Serializable;
import java.util.Map;

public class InfoBodyConsumer implements Serializable{
     /**
	 * 
	 */
	private static final long serialVersionUID = 4500208867228323993L;
	 private String groupId;
     private String topic;
     private Map<String,String> filterMap;
	public String getGroupId() {
		return groupId;
	}
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public Map<String, String> getFilterMap() {
		return filterMap;
	}
	public void setFilterMap(Map<String, String> filterMap) {
		this.filterMap = filterMap;
	}
	public String toString(){
		return "group:"+this.groupId+" topic:"+this.topic+" filter:"+this.filterMap;
	}
     
}
