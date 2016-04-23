package com.alibaba.middleware.race.mom.broker.function;

import io.netty.channel.Channel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageSend implements Serializable{

	private static final long serialVersionUID = 1980989080980980L;
	private ArrayList<String> sendIds=new ArrayList<>();
	private String topic;
	private ArrayList<byte[]> bodys=new ArrayList<>();
	private Map<String, String> properties = new HashMap<String, String>();
	private long bornTime=System.currentTimeMillis();
	private Channel producer;
	
	
	public MessageSend(){}
	
	
	

	public ArrayList<String> getSendIds() {
		return sendIds;
	}




	public void setSendIds(ArrayList<String> sendIds) {
		this.sendIds = sendIds;
	}




	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public ArrayList<byte[]> getBodys() {
		return bodys;
	}
	public void setBodys(ArrayList<byte[]>bodys) {
		this.bodys = bodys;
	}
	public Map<String, String> getProperties() {
		return properties;
	}
	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}
	public long getBornTime() {
		return bornTime;
	}
	public void setBornTime(long bornTime) {
		this.bornTime = bornTime;
	}
	public MessageSend( String topic,
			Map<String, String> properties) {
		this.topic = topic;
		this.properties = properties;
	}
	
	public MessageSend( String topic
		) {
		this.topic = topic;
	}



	public Channel getProducer() {
		return producer;
	}



	public void setProducer(Channel producer) {
		this.producer = producer;
	}
	
	
	
	
	
}
