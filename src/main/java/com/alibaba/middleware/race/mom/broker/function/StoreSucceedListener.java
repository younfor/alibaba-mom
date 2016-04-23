package com.alibaba.middleware.race.mom.broker.function;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.middleware.race.mom.Broker;
import com.alibaba.middleware.race.mom.broker.group.ConsumerGroup;
import com.alibaba.middleware.race.mom.util.TopicAndFilter;

/**
 * 存储成功监听器:force到磁盘成功,回调触发返回ack给生产者
 * 一次存储过程,对应一个监听器
 * @author youngforever
 *
 */
public class StoreSucceedListener {
	private static Logger logger = LoggerFactory.getLogger(StoreSucceedListener.class);
	
	private static  ExecutorService sendACKPool = Executors.newFixedThreadPool(1);
	private Map<Channel/*producer*/,Integer/*msgIds*/> waitAckNumMap=new HashMap<>();
	private static int  i=0;
	private Map<Channel/*producer*/,List<String/*msgId*/>> waitAckMap=new HashMap<>();
	private Map<Channel/*producer*/,StringBuilder/*msgIds*/> waitAcksMap=new HashMap<>();
	private Map<Channel/*producer*/,ArrayList<String>/*sendId or ack String*/> waitAckGroupMap=new HashMap<>();
//	private StringBuilder acks=new StringBuilder();
	
	
	/**
	 * 一次存储成功回调,存储完成(强行force)后异步返回对应的ack
	 */
	public void onStoreSucceed(TopicAndFilter topicAndFilter,int queueIndex,int offset){
		/**
		 * ack返回
		 */
		sendACKPool.execute(new Runnable(){
			@Override
			public void run() {
//				for(Channel producer:waitAckMap.keySet()){
//					for(String msgId:waitAckMap.get(producer)){
//						producer.writeAndFlush(msgId + "\r\n");
//						i++;
////						System.out.println("存储成功,返回ack"+i+"条");
//					}
//				}
//				for(Map.Entry<Channel,StringBuilder> acks:waitAcksMap.entrySet()){
//					acks.getKey().writeAndFlush(acks.getValue().toString()+"\r\n");
//					logger.error("sendback acks"+acks.getValue().toString());
//				}
//				for(Map.Entry<Channel,Integer>acks:waitAckNumMap.entrySet()){
//					acks.getKey().writeAndFlush(acks.getValue()+"\r\n");
////					logger.error("sendback acks"+acks.getValue().toString());
//				}
				for(Map.Entry<Channel,ArrayList<String> >ack:waitAckGroupMap.entrySet()){
					StringBuilder acks=new StringBuilder();
					for(String id:ack.getValue())
					{
						acks.append("@");
						acks.append(id);
					}
					ack.getKey().writeAndFlush(acks+"\r\n");
					//System.out.println("返回的是:"+acks);
//					logger.error("sendback acks"+acks.getValue().toString());
				}
			}
		});
		/**
		 * 精确发送已存储的消息
		 */
		ConcurrentHashMap<String, ConsumerGroup> needSendGroups=ConsumerGroup.getNeedSendGroups(topicAndFilter);
		if(needSendGroups!=null)
		for(ConsumerGroup consumerGroup:needSendGroups.values()){
//			consumerGroup.startSend();
			consumerGroup.sendSpecificMsgs(queueIndex, offset);
			/**
			 * 存储到文件,再来设置MaxOffset
			 */
			consumerGroup.setMaxOffset( queueIndex,offset);
		}
	}
	/**
	 * 添加ack等待
	 */
	public void addAckWaiter(Channel producer,String msgId){
		List<String/*msgId*/> msgIds=waitAckMap.get(producer);
		if(msgIds==null){
			msgIds=new ArrayList<>();
			msgIds.add(msgId);
			waitAckMap.put(producer,msgIds);
		}else {
			msgIds.add(msgId);
		}
	}
	public void addAckWaiterGroup(Channel producer,String msgId){
		StringBuilder acks=waitAcksMap.get(producer);
		if(acks==null){
			acks=new StringBuilder();
			acks.append(msgId).append(" ");
			waitAcksMap.put(producer, acks);
		}else acks.append(msgId).append(" ");
	}
	public void addAckWaiterNum(Channel producer){
		Integer ackNum=waitAckNumMap.get(producer);
		logger.debug(String.valueOf(ackNum));
		if(ackNum==null){
			ackNum=new Integer(1);
			waitAckNumMap.put(producer, ackNum);
		}else waitAckNumMap.put(producer, ++ackNum);
	}
	public void addAckGroupWaiter(Channel producer,ArrayList<String> sendId){
		List<String> ack=waitAckGroupMap.get(producer);
		logger.debug(String.valueOf(ack));
		if(ack==null){
			waitAckGroupMap.put(producer, sendId);
		}else if(!ack.equals(sendId)){
			logger.error(" 两次存储并未串行，可能需要解决异步调用");
		}
	}
}
