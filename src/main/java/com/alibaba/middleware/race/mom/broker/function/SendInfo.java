package com.alibaba.middleware.race.mom.broker.function;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.mom.SendStatus;
import com.alibaba.middleware.race.mom.config.Config;
import com.alibaba.middleware.race.mom.util.TopicAndFilter;

public class SendInfo implements Serializable {
	/**
	 * 发送结果，判断消费是否超时等
	 */
	private static final long serialVersionUID = 321275686710864167L;
	
//	public static final Map<String/* msgId */, SendInfo> sendInfoMap = new ConcurrentHashMap<>();
	private MessageInfo msgInfo;
	private String info;
	private SendStatus status=SendStatus.SEND;
//	private int msgOffset;
//	private String topic;
	/*
	 * 订阅集群的topicAndFilter
	 */
	private TopicAndFilter topicAndFilter;
//	private int queueId;
	
	private long sendTime;
//	private long recvAckTime;
	private long timeoutLimit=Config.timeoutLimit;
	private AtomicInteger reSendCounter=new AtomicInteger();
	
	
	public SendInfo(MessageInfo msgInfo){
		this.msgInfo=msgInfo;
	}
	/**
	 * @param timeoutLimit unit:ms 允许的超时时间限制
	 */
	public SendInfo(MessageInfo msgInfo,long timeoutLimit){
		this.msgInfo=msgInfo;
		this.timeoutLimit=timeoutLimit;
	}
	
	/**
	 * 重置snedResult,用于重投时重投次数增加、重置发送时间，状态等
	 * 
	 */
	public void reSend(){
		sendTime=System.currentTimeMillis();
//		recvAckTime=0;
		reSendCounter.incrementAndGet();
		this.status=SendStatus.RESEND;
	}
	public String getInfo() {
		return info;
	}
	public SendStatus getStatus() {
		return status;
	}
	public void setInfo(String info) {
		this.info = info;
	}
	public void setStatus(SendStatus status) {
		this.status= status ;
	}
	
//	public long getRecvAckTime() {
//		return recvAckTime;
//	}
//	public void setRecvAckTime(long recvAckTime) {
//		this.recvAckTime = recvAckTime;
//	}
	public void setSendTime(long sendTime){
		this.sendTime=sendTime;
	}
	public long getSendTime(){
		return this.sendTime;
	}
	public int getMsgOffset() {
		return this.msgInfo.getOffset();
	}
	public TopicAndFilter getTopicAndFilter() {
		return topicAndFilter;
	}
	public void setTopicAndFilter(TopicAndFilter topicAndFilter) {
		this.topicAndFilter = topicAndFilter;
	}
	public int getQueueId() {
		return this.msgInfo.getQueueIndex();
	}
	
	public MessageInfo getMsgInfo() {
		return msgInfo;
	}
	public int getReSendCount() {
		return reSendCounter.get();
	}
//	public void decreaseNeedSendGroupsNum(){
//		needSendGroupsNum.decrementAndGet();
//	}
	/**
	 * @param nowTime
	 * @return 超时则 返回true;否则返回false;
	 */
	public boolean isTimeout(){
		if(System.currentTimeMillis()-sendTime>=this.timeoutLimit){
			this.status=SendStatus.TIMEOUT;
			return true;
		}else return false; 
	}
//	public boolean canRemove(){
//		if(status==SendStatus.SUCCESS){
//			return true;
//		}else if(this.reSendCounter.get()>=Config.maxReSendTime){
//			return true;
//		}else return false;
//	}
	/**
	 * @return 超时则 返回true;否则返回false;
	 * @throws Exception :throw exception if do not receive consumer's ACK or do not set the receive ACK time:recvAckTime
	 */
//	public boolean isTimeout() throws Exception{
//		if(this.recvAckTime!=0){
//			return recvAckTime-sendTime>=this.timeoutLimit;
//		}else {
//			throw new Exception("do not recieve consumer's ACK or do not set the recieve ACK time:recvAckTime ");
//		}
//	}
//	
	@Override
	public String toString(){
		return "msg "+msgInfo.getMsg()+"  send "+status+"   info:"+info;
	}
	
}
