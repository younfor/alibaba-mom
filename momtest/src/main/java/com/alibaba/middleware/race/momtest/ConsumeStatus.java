package com.alibaba.middleware.race.momtest;

import java.util.ArrayList;
import java.util.List;

public class ConsumeStatus {
	String msgId;
	List<Boolean> statusList=new ArrayList<Boolean>();
	public String getMsgId() {
		return msgId;
	}
	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}
	public List<Boolean> getStatusList() {
		return statusList;
	}
	public void setStatusList(List<Boolean> statusList) {
		this.statusList = statusList;
	}	
}
