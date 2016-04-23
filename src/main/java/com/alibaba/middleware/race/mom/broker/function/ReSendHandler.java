//package com.alibaba.middleware.race.mom.broker.function;
//
//import io.netty.channel.Channel;
//
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.LinkedBlockingQueue;
//
//import com.alibaba.middleware.race.mom.Message;
//import com.alibaba.middleware.race.mom.broker.group.ConsumerGroup;
//import com.alibaba.middleware.race.mom.util.TopicAndFilter;
///**
// * 每个集群有自己的ReSendHandler
// * @author youngforever
// *
// */
//public class ReSendHandler {
//	private TopicAndFilter topicAndFilter; 
//	private ConsumerGroup group;
//	public BlockingQueue<Integer> reSendQueue=new LinkedBlockingQueue<Integer>();
//	
//	int reSendThreadPoolSize=2;
//	private  ExecutorService reSendPool = Executors.newFixedThreadPool(reSendThreadPoolSize);
//	
//	
//	public ReSendHandler(TopicAndFilter topicAndFilter, ConsumerGroup group) {
//		this.topicAndFilter = topicAndFilter;
//		this.group = group;
//	}
//	public void consumerLostReSend(){
//		
//	}
//	public void timeoutReSend(){
//		
//	}
//	public void failReSend(){
//		
//	}
//	
//	
//	 /**
//	  * 开始发送任务
//	  */
//	public void startRend(){
//		for(int i=0;i<reSendThreadPoolSize;i++){//每个队列提交几次任务,占满线程池
//			reSendPool.execute(new Runnable(){
//				int offset;
//				Message msg;
//				Channel consumer;
//				@Override
//				public void run() {
//					while(true){
//						try {
//							offset=reSendQueue.take();
//						} catch (InterruptedException e) {
//							// 不可能出现线程中断异常的
//							Thread.interrupted();//清除中断状态
//						}
//						//取消息,发送
//						msg=MessageManager.messageCacheMap.get(topicAndFilter+String.valueOf(offset)).getMsg();
//						if(msg==null){
//							return;
//						}
//						consumer=group.consumersBalance();
//						if(consumer!=null){
//							consumer.writeAndFlush(msg);
//							group.getSendInfoMap().get(msg.getMsgId()).setSendTime(System.currentTimeMillis());
//						}else {
////							logger.error("消费者集群都不在线,须集群恢复后重投");
//							
//						}
//							
////							//记录发送状态信息
////							SendInfo sendResult=new SendInfo(timeoutLimit);
////							sendResult.setNeedSendGroupNum(ConsumerGroup.needSendGroupsNum(topicAndFilter));
////							sendResult.setMsgOffset(msg.getOffset());
////							sendResult.setTopicAndFilter(topicAndFilter);
////							sendResult.setSendTime(System.currentTimeMillis());
////							sendResult.setStatus(ConsumerGroup.this,SendStatus.SEND);
////							sendInfoMap.put(msg.getMsgId(),sendResult);
//					}
//				}
//			});
//		}
//	}
//	
//}
