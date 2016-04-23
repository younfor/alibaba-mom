package com.alibaba.middleware.race.mom.broker.function;
import io.netty.channel.Channel;

import java.io.Serializable;

import com.alibaba.middleware.race.mom.Message;

/**
 * 生产者生的产消息和其它信息封装
 * @author youngforever
 *
 */
public class MessageInfo implements Serializable{
	private static final long serialVersionUID = 529580833582395809L;
	/**
	 * 由broker端设置,需要再加
	 */
	private Message msg;
//	private String producerId;
//	private String producerGroupId;
	private String msgInfoId;
	private Channel producer;
	private int queueId;
	private int offset;//存储顺序,是topicandfilter下 某个queueId 下的offset
	
	
	
	public MessageInfo(Message msg, Channel producer){
		this.msg = msg;
		this.producer = producer;
	}
	public MessageInfo(Message msg, Channel producer, int queueId, int offset) {
		this.msg = msg;
		this.producer = producer;
		this.queueId = queueId;
		this.offset = offset;
	}


	/**
	 * msg 订阅集群数
	 */
//	private AtomicInteger subGroupsCount;
	
//	public MessageInfo(Message msg, String producerId, String producerGroupId) {
//		this.msg = msg;
//		this.producerId = producerId;
//		this.producerGroupId = producerGroupId;
//	}
//
	public Message getMsg() {
		return msg;
	}


	public void setMsg(Message msg) {
		this.msg = msg;
	}


//	public int getSubGroupsCount() {
//		return subGroupsCount.get();
//	}
//
//	public void setSubGroupsCount(int subGroupsCount) {
//		this.subGroupsCount = new AtomicInteger(subGroupsCount);
//	}
//	public int decreSubGroupsCount(){
//		return subGroupsCount.decrementAndGet();
//	}
//	public String getProducerId() {
//		return producerId;
//	}


	public String getMsgInfoId() {
		return msgInfoId;
	}
	public void setMsgInfoId(String msgInfoId) {
		this.msgInfoId = msgInfoId;
	}
	public int getQueueId() {
		return queueId;
	}
	public int getQueueIndex() {
		return queueId;
	}


	public void setQueueId(int queueId) {
		this.queueId = queueId;
	}


//	public void setProducerId(String producerId) {
//		this.producerId = producerId;
//	}


	public int getOffset() {
		return offset;
	}


	public void setOffset(int offset) {
		this.offset = offset;
	}


//	public String getProducerGroupId() {
//		return producerGroupId;
//	}
//
//
//	public void setProducerGroupId(String producerGroupId) {
//		this.producerGroupId = producerGroupId;
//	}


	public Channel getProducer() {
		return producer;
	}


	/**
	 *由broker 端设置,连接成功后,设置生产者channel,以便精确返回ack 
	 * @param producer
	 */
	public void setProducer(Channel producer) {
		this.producer = producer;
	}
	
	
}
