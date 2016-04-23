package com.alibaba.middleware.race.mom;

import java.io.Serializable;

public class SendResult implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 321275686710864167L;
	private String info;
	private SendStatus status=SendStatus.SUCCESS;
	private String msgId;
	public String getInfo() {
		return info;
	}
	public String getMsgId() {
		return msgId;
	}
	public SendStatus getStatus() {
		return status;
	}
	public void setInfo(String info) {
		this.info = info;
	}
	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}
	public void setStatus(SendStatus status) {
		this.status = status;
	}
	@Override
	public String toString(){
		return "msg "+msgId+"  send "+(status==SendStatus.SUCCESS?"success":"fail")+"   info:"+info;
	}
	
}
