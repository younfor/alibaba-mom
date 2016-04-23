package com.alibaba.middleware.race.mom.util;

import java.io.Serializable;
import java.util.ArrayList;

import com.alibaba.middleware.race.mom.broker.function.MessageInfo;

/**
 * 
 * @author Administrator
 *   封装producer的请求信息
 */
public class InfoBodyProduceList implements Serializable{
  
	private static final long serialVersionUID = 9075042672715185764L;
	ArrayList<MessageInfo> msglist=new ArrayList<>();
	 public ArrayList<MessageInfo> getMsglist() {
		return msglist;
	}
	public void setMsglist(ArrayList<MessageInfo> msglist) {
		this.msglist = msglist;
	}
	public InfoBodyProduceList() {

	}

	
}
