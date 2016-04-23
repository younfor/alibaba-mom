package com.alibaba.middleware.race.mom.broker.function;


import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.broker.group.ConsumerGroup;
import com.alibaba.middleware.race.mom.broker.group.ProducerGroup;
import com.alibaba.middleware.race.mom.util.TopicAndFilter;

public class MessageManager {
	private static org.slf4j.Logger logger = LoggerFactory.getLogger(MessageManager.class);

//	public static Map<String/* TopicAndFilter+msgOffset */,MessageInfo> messageCacheMap;
	/**
	 * 每个集群有独立的消费进度
	 * 消息再broker收到后,按照TopicAndFilter 消息分类,每种TopicAndFilter消息有独立的offsetProducer
	 * 如果要以queueId分多文件,那么再分一层独立的offsetProducer
	 */
	
//	private MsgStore mstore = /* new MsgStoreImp();// */new MsgStoreImp_MappedBuffer();
//	private FSTConfiguration fst = FSTConfiguration.getDefaultConfiguration();

//	private Map<String/* topic@group */,BlockingQueue<String /*id*/>/*reSendMsgQueue*/>topicAndgroup2reSendMsgQueue=new HashMap<>();
	
	
	
//	public MessageManager(ConcurrentHashMap<String/* TopicAndFilter+msgOffset */,Message> recoverMsgMap){
//		if(recoverMsgMap!=null){
//			messageCacheMap=recoverMsgMap;
//		}else{
//			messageCacheMap=new ConcurrentHashMap<>();
//		}
//	}

	
	/**
	 * 打包消息处理
	 * @param msgSend
	 * @param producer
	 */
	public static void recieveMsg(MessageSend msgSend,ChannelHandlerContext producer){
		long start=System.currentTimeMillis();
		/*
		 * 存储处理
		 */
		msgSend.setProducer(producer.channel());
		TopicAndFilter topicAndFilter=new TopicAndFilter(msgSend.getTopic(),msgSend.getProperties());
		String queueIdAndOffsetArray[] = ProducerGroup.storeMsg(topicAndFilter,msgSend).split(" ");
		int queueIndex=Integer.parseInt(queueIdAndOffsetArray[0]);
		int offset=Integer.parseInt(queueIdAndOffsetArray[1]);
		logger.debug("currentCanUseOffset "+offset);
		/*
		 * 消息进入待发送队列，等待存储完成（）
		 */
		int i=0;
		for(byte[]body:msgSend.getBodys()){
			Message msg=new Message(msgSend.getTopic(),body,msgSend.getProperties(),msgSend.getBornTime());
			msg.setMsgId(msgSend.getSendIds().get(i));
			MessageInfo msgInfo=new MessageInfo(msg,producer.channel());
			msgInfo.setQueueId(queueIndex);
			msgInfo.setOffset(offset++);
			msgInfo.setMsgInfoId(topicAndFilter.toString()+queueIndex+""+String.valueOf(msgInfo.getOffset()));
			/*
			 * 发送处理，加入发送队列，等待存储完成
			 */
			ConsumerGroup.sendMsg(topicAndFilter, msgInfo);
			i++;
		}
//		logger.error("消息born 到生产者recv处理完毕 cost："+(System.currentTimeMillis()-msgSend.getBornTime()));
//		logger.error("消息recv生产者recv处理完毕 cost："+(System.currentTimeMillis()-start));
	}
	
	
	/**
	 * 接收到生产者 消息,触发一系列处理
	 * @param msg
	 * @param producer
	 */
//	public static void recieveMsg(Message msg,ChannelHandlerContext producer){
//		//生产者信息构建的topicAndFilter
//		TopicAndFilter topicAndFilter=new TopicAndFilter(msg.getTopic(),msg.getProperties());
////		String topicAndFilterString=topicAndFilter.toString();
//		/**
//		 * broker端给消息的编号:offset,同时对应存储位置,消费进度
//		 * topicAndFilter不同,offset将相互独立
//		 */
////		//broker端给消息的编号:offset,同时对应存储位置,消费进度
////		if(offsetProducerMap.containsKey(topicAndFilterString)){
////			int offset=offsetProducerMap.get(topicAndFilterString).getAndIncrement();
////			msg.setOffset(offset);
////		}
//		/**
//		 * 以下两个过程,保证:
//		 * 一.先加入持久化队列(以持久化队列下标编offset(对应消息在文件中的索引),保证正确对应(也不会受多线程影响)),再加入发送队列
//		 * 二.保证每一条消息到磁盘(存储底层也已经强行force),再返回ack
//		 * 三.发送队列与持久化队列一一对应(其实也可以允许队列均衡导致不一致,但大的来说顺序仍然一致),保证消费进度的正确性(我们可以通过配置,每一个topicandfilter下做几个队列)
//		 * 四.发送给消费者与消息存储,多线程执行,不保证先后顺序(理想是并行)
//		 */
//		long start=System.currentTimeMillis();
//		MessageInfo msgInfo=new MessageInfo(msg,producer.channel());
//		//持久化消息,成功后返回给生产者ack
//		String queueIdAndOffsetArray[]=ProducerGroup.storeMsg(topicAndFilter,msgInfo).split(" ");
//		int queueIndex=Integer.parseInt(queueIdAndOffsetArray[0]);
//		int offset=Integer.parseInt(queueIdAndOffsetArray[1]);
//		msgInfo.setQueueId(queueIndex);
//		msgInfo.setOffset(offset);
//		//消息对象引用存入内存map,为了避免多topicAndFilter时,map key重复,key以topicAndFilter+offset
////		msg.setOffset(offset);
////		MessageManager.addMessage(topicAndFilterString+String.valueOf(offset),msgInfo);
//		//转发给发送消费者
//		ConsumerGroup.sendMsg(topicAndFilter, msgInfo);
//		
////		mstore.writeByteNormal(topicAndfilter.toString(),"t","t",0+"" , msgbytes/*caches*/);
////		ProducerGroup.addProducer(producer.channel(),"PROO", topicAndFilter);
////		producer.channel().writeAndFlush(msg.getMsgId() + "\r\n");
//		//接收新消息处理完成,等待消费结果,进入消费结果处理
//		logger.error("消息born 到生产者recv处理完毕 cost："+(System.currentTimeMillis()-msg.getBornTime()));
//		logger.error("消息recv生产者recv处理完毕 cost："+(System.currentTimeMillis()-start));
//	}
	
	
	
//	public static void recieveMsg(BlockingQueue<MessageInfo> msgInfoqueue,ChannelHandlerContext producer){
//		MessageInfo msgInfo=msgInfoqueue.peek();
//		//生产者信息构建的topicAndFilter
//		TopicAndFilter topicAndFilter=new TopicAndFilter(msgInfo.getMsg().getTopic(),msgInfo.getMsg().getProperties());
//		String topicAndFilterString=topicAndFilter.toString();
//		/**
//		 * broker端给消息的编号:offset,同时对应存储位置,消费进度
//		 * topicAndFilter不同,offset将相互独立
//		 */
////		//broker端给消息的编号:offset,同时对应存储位置,消费进度
////		if(offsetProducerMap.containsKey(topicAndFilterString)){
////			int offset=offsetProducerMap.get(topicAndFilterString).getAndIncrement();
////			msg.setOffset(offset);
////		}
//		/**
//		 * 以下两个过程,保证:
//		 * 一.先加入持久化队列(以持久化队列下标编offset(对应消息在文件中的索引),保证正确对应(也不会受多线程影响)),再加入发送队列
//		 * 二.保证每一条消息到磁盘(存储底层也已经强行force),再返回ack
//		 * 三.发送队列与持久化队列一一对应(其实也可以允许队列均衡导致不一致,但大的来说顺序仍然一致),保证消费进度的正确性(我们可以通过配置,每一个topicandfilter下做几个队列)
//		 * 四.发送给消费者与消息存储,多线程执行,不保证先后顺序(理想是并行)
//		 */
//		msgInfo.setProducer(producer.channel());
//		MessageInfoQueue msgQueue=new MessageInfoQueue(producer.channel(),topicAndFilter,msgInfoqueue);
//		
//		//持久化消息,成功后返回给生产者ack
//		ProducerGroup.storeMsg(topicAndFilter,msgQueue);
////		int queueIndex=Integer.parseInt(queueIdAndOffsetArray[0]);
////		int offset=Integer.parseInt(queueIdAndOffsetArray[1]);
////		msgInfo.setQueueId(queueIndex);
////		msgInfo.setOffset(offset);
//		//消息对象引用存入内存map,为了避免多topicAndFilter时,map key重复,key以topicAndFilter+offset
////		msg.setOffset(offset);
////		MessageManager.addMessage(topicAndFilterString+String.valueOf(offset),msgInfo);
//		//转发给发送消费者
//		ConsumerGroup.sendMsg(topicAndFilter, msgInfo);
//		
////		mstore.writeByteNormal(topicAndfilter.toString(),"t","t",0+"" , msgbytes/*caches*/);
////		ProducerGroup.addProducer(producer.channel(),"PROO", topicAndFilter);
////		producer.channel().writeAndFlush(msg.getMsgId() + "\r\n");
//		//接收新消息处理完成,等待消费结果,进入消费结果处理
//	}
	
//	public static void addMessage(String offet,MessageInfo msgInfo){
//		messageCacheMap.put(offet, msgInfo);
//	}
//	public static void setMsgMap(ConcurrentHashMap<String/* TopicAndFilter+msgOffset */,MessageInfo> recoverMap){
//		messageCacheMap=recoverMap;
//	}
	
	
	
	 /**
	  * 开始存储任务
	  */
//	long starttttt;
//	public void startStore(){
//		
////		for(final BlockingQueue<MessageInfo> storeQueue:storeQueues){
////			for(int i=0;i<storeThreadPoolSize/storeQueueNum;i++){//每个队列提交几次任务,占满线程池
//				storePool.execute(new Runnable(){
//					@Override
//					public void run() {
//						long start=System.currentTimeMillis();
//						AtomicInteger storeCount=new AtomicInteger();
//						BlockingQueue<MessageInfo>  storeQueue=storeQueues.get(0);
//						while(true){		
//							MessageInfo msgInfo=null;
//							ArrayList<byte[]> msgbytes=new ArrayList<>();
//							StoreSucceedListener storeSucceedListener=new StoreSucceedListener();
//							/**
//							 * 准备一次存储过程,构建对应存储成功监听器
//							 */
////							int i=100;
////							try {
////								count.await(20, TimeUnit.MILLISECONDS);
////							} catch (InterruptedException e) {
////								count=new CountDownLatch(100);
////							}
////							boolean flag=false;
////							while(!storeQueue.isEmpty()&&i-->0){
//							
//							int offset=0;
//						
//							while(true){
////								if(storeCount.incrementAndGet()>98){
////									System.out.println("一百条等待"+(System.currentTimeMillis()-start));
////									break;
////								};
//								try {
//									msgInfo=storeQueue.poll(0, TimeUnit.MILLISECONDS);
//								} catch (InterruptedException e) {
//									// TODO Auto-generated catch block
//									e.printStackTrace();
//								}
//								if(msgInfo==null&&storeCount.get()==0) continue;
//								if(msgInfo==null) continue;
//								offset=msgInfo.getOffset();
//								redo.writeLog2(offset+"", msgInfo.getMsg().getTopic(), msgInfo.getQueueId(),msgInfo.getMsg().getBody());
////								i++;
//								
////								storeSucceedListener.addAckWaiter(msgInfo.getProducer(), msgInfo.getMsg().getMsgId());
//								storeSucceedListener.addAckWaiterGroup(msgInfo.getProducer(), msgInfo.getMsg().getMsgId());
//							
////								if(cando&&storeCount.get()!=0) break;
//								if(storeCount.incrementAndGet()>199) break;
////								flag=true;
////								
//							}
////							if(flag)
//							if(storeCount.get()==0) continue;
//								logger.error(Thread.currentThread().getName()+" "+(System.currentTimeMillis()-start)+" ms后提交新落盘任务,一次flush"+storeCount.get()+"条");
////								storeTask(storeQueues.indexOf(storeQueue),msgbytes,storeSucceedListener,offset);
//							
//							long submit=System.currentTimeMillis();	
//							if(starttttt==0){
//								starttttt=submit;
//							}
//							redo.flushLog();
//							logger.error("redo"+(System.currentTimeMillis()-starttttt));
//							storeSucceedListener.onStoreSucceed(topicAndfilter,0,offset);
//								storeCount.set(0);
//								start =System.currentTimeMillis();
////							}else{
////								//0就不提交了，大量废任务进入线程池。。。
////							}
//						}
//					}
//				});
////			}
			
			
			
			
			
////			for(int i=0;i<storeThreadPoolSize/storeQueueNum;i++){//每个队列提交几次任务,占满线程池
//				storePool.execute(new Runnable(){
//					@Override
//					public void run() {
//						long start=System.currentTimeMillis();
//						AtomicInteger storeCount=new AtomicInteger();
//						BlockingQueue<MessageInfo>  storeQueue=storeQueues.get(1);
//						while(true){		
//							MessageInfo msgInfo=null;
//							ArrayList<byte[]> msgbytes=new ArrayList<>();
//							StoreSucceedListener storeSucceedListener=new StoreSucceedListener();
//							/**
//							 * 准备一次存储过程,构建对应存储成功监听器
//							 */
////							int i=100;
////							try {
////								count.await(20, TimeUnit.MILLISECONDS);
////							} catch (InterruptedException e) {
////								count=new CountDownLatch(100);
////							}
////							boolean flag=false;
////							while(!storeQueue.isEmpty()&&i-->0){
//							
//							int offset=0;
//						
//							while(true){
////								if(storeCount.incrementAndGet()>98){
////									System.out.println("一百条等待"+(System.currentTimeMillis()-start));
////									break;
////								};
//								try {
//									msgInfo=storeQueue.poll(0, TimeUnit.MILLISECONDS);
//								} catch (InterruptedException e) {
//									// TODO Auto-generated catch block
//									e.printStackTrace();
//								}
//								if(msgInfo==null&&storeCount.get()==0) continue;
//								if(msgInfo==null) continue;
//								offset=msgInfo.getOffset();
//								redo2.writeLog2(offset+"", msgInfo.getMsg().getTopic(), msgInfo.getQueueId(),msgInfo.getMsg().getBody());
////								i++;
//								
////								storeSucceedListener.addAckWaiter(msgInfo.getProducer(), msgInfo.getMsg().getMsgId());
//								storeSucceedListener.addAckWaiterGroup(msgInfo.getProducer(), msgInfo.getMsg().getMsgId());
//							
////								if(cando&&storeCount.get()!=0) break;
//								if(storeCount.incrementAndGet()>99) break;
////								flag=true;
////								
//							}
////							if(flag)
//							if(storeCount.get()==0) continue;
//								logger.error(Thread.currentThread().getName()+" "+(System.currentTimeMillis()-start)+" ms后提交新落盘任务,一次flush"+storeCount.get()+"条");
////								storeTask(storeQueues.indexOf(storeQueue),msgbytes,storeSucceedListener,offset);
//								long submit=System.currentTimeMillis();	
//								if(starttttt==0){
//									starttttt=submit;
//								}
//								redo2.flushLog();
//								logger.error("redo2 "+(System.currentTimeMillis()-starttttt));
//							storeSucceedListener.onStoreSucceed(topicAndfilter,1,offset);
//							storeCount.set(0);
//								start =System.currentTimeMillis();
////							}else{
////								//0就不提交了，大量废任务进入线程池。。。
////							}
//						}
//					}
//				});
////			}
//			
//			
			
			
			
//		}
//	}
	
}
