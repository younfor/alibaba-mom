package com.alibaba.middleware.race.mom.store;

import java.io.Serializable;

public class StoreMsg implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2571744737752908548L;
	String id;
	String topic;
	int queueid;
	byte[] body;

	public StoreMsg(String id, String topic, int queueid, byte[] body) {
		this.id = id;
		this.topic = topic;
		this.queueid = queueid;
		this.body = body;
	}
}