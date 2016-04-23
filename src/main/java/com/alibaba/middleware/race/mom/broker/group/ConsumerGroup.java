package com.alibaba.middleware.race.mom.broker.group;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.SendStatus;
import com.alibaba.middleware.race.mom.broker.function.MessageInfo;
import com.alibaba.middleware.race.mom.broker.function.Offset;
import com.alibaba.middleware.race.mom.broker.function.SendInfo;
import com.alibaba.middleware.race.mom.broker.function.SendInfoScanner;
import com.alibaba.middleware.race.mom.config.Config;
import com.alibaba.middleware.race.mom.util.TopicAndFilter;
/**
 * 消费者集群管理
 * @author young
 *
 */
/**
 * 消费者集群管理
 * @author young
 *
 */
public class ConsumerGroup {
	private static Logger logger = LoggerFactory.getLogger(ConsumerGroup.class);
	
	/**
	 * 所有用于发送的 线程池可分配大小,也可以不限制大小,但也许应给避免订阅集群过多,发送线程池过大,不断优化测试.....
	 */
	private static int sendThreadPoolTotalSize=10;
	
	
	/**
	 * 每个订阅集群
	 */
	private LinkedList<Channel>consumers = new LinkedList<Channel>();
	private String groupId;
	private String subscribedTopic;
	private Map<String,String> subscribedFilter;
	private TopicAndFilter topicAndFilter; 
	/**
	 * 每个集群的发送信息(发送状态,失败,成功,超时等)
	 */
	private   Map<String/* msgId */, SendInfo> sendInfoMap = new ConcurrentHashMap<>();
	/**
	 * 重投
	 */
	private  SendInfoScanner  sendInfoScanner=new SendInfoScanner(this);
	
//	private Map<String/* topic@group */, LinkedList<Offset>> topicAndgroup2subScribe = new ConcurrentHashMap<>();
		/*
		 * 每个集群的sendQueueNum个发送队列,对应消息拆分存储为sendQueueNum份文件
		 * sendThreadPoolSize为分配改当前集群发送线程池大小,不断优化测试.....
		 * sendPool,负责当前集群发送的线程池
		 */
	/**
	 * 发送队列个数,与存储队列一一对应,须全局统一配置(错了将报错,注意!!!!)
	 */
	private int sendQueueNum=Config.maxqueue;
//	private int sendThreadPoolSize=sendQueueNum*4;
	private int sendThreadPoolSize=Runtime.getRuntime().availableProcessors()/2;	
	private  LinkedList<BlockingQueue<MessageInfo/* TopicAndFilter+msgOffset */>> sendQueues = new LinkedList<>();
	/**
	 * 当前集群(组)的消费进度情况
	 */
	private LinkedList<Offset> consumeOffsets=new  LinkedList<Offset> ();
	
	private int  sendQueueBalancer=-1;
	private  ExecutorService sendPool = Executors.newFixedThreadPool(sendThreadPoolSize);
	
	
	
	/**
	 * 构造函数
	 * @param subscribedTopic
	 * @param subscribedFilter
	 * @param groupId
	 */
	public ConsumerGroup(String subscribedTopic,Map<String,String> subscribedFilter,String groupId){
		this.subscribedTopic=subscribedTopic;
		this.subscribedFilter=subscribedFilter;
		topicAndFilter=new TopicAndFilter(subscribedTopic,subscribedFilter);
		this.groupId=groupId;
		/*
		 * 集群创建时初始化队列,初始化对应的offet进度 
		 */
		Offset offset;
		for(int i=0;i<sendQueueNum;i++){
			this.sendQueues.add(new LinkedBlockingQueue<MessageInfo>());
//			offset=new Offset(subscribedTopic,groupId,topicAndFilter.toString());
			offset=new Offset();
			offset.setQueueIndex(i);
//			offset.setTopic(subscribedTopic);
			this.consumeOffsets.add(offset);
		}
		/*
		 * 启动发送任务,不采用立即转发方式,旺旺群讨论的结果
		 */
//		startSend();
		/*
		 * 启动重投检测
		 */
		
//		sendInfoScanner.start();
	}
	/**
	 * 负载均衡,且去除死掉的消费者
	 * @return 本次发往的消费者.如果集群都不在线,则返回null
	 */
	private volatile int  consumerBalancer=-1;
	public synchronized Channel consumersBalance(){
		consumerBalancer++;
		if(consumerBalancer==consumers.size()){
			consumerBalancer=0;
		}
		Channel consumer=consumers.get(consumerBalancer);
		if (consumer==null) return null;
		if(!consumer.isOpen()){
			logger.error("消费者失去连接: " + consumer.hashCode());
			consumers.remove(consumer);
			return consumersBalance();
		}else return consumer;
		
	}
	/**
	 * 发送队列均衡
	 * @return
	 */
	 public synchronized BlockingQueue<MessageInfo> sendQueuesBalance(){
		 sendQueueBalancer++;
		if(sendQueueBalancer==sendQueueNum){
			sendQueueBalancer=0;
		}
		BlockingQueue<MessageInfo> sendQueue=sendQueues.get(sendQueueBalancer);
		return sendQueue;
	}
	 /**
	  * 开始发送任务,此方式为收到消息,立即转发,与存储并行
	  */
	public void startSend(){
		for(final BlockingQueue<MessageInfo> sendQueue:sendQueues){
			for(int i=0;i<sendThreadPoolSize/sendQueueNum;i++){//每个队列提交几次任务,占满线程池
				sendPool.execute(new Runnable(){
					MessageInfo msgInfo;
					Message msg;
					Channel consumer;
					final int queueIndex=sendQueues.indexOf(sendQueue);
					@Override
					public void run() {
						while(true){
							while(!sendQueue.isEmpty()){
	//							try {
	//								msgInfo=sendQueue.take();
									msgInfo=sendQueue.poll();
	//								System.out.println(sendQueue.size());
	//							} catch (InterruptedException e) {
									// 不可能出现线程中断异常的
	//								Thread.interrupted();//清除中断状态
	//							}
								//取消息,发送
								if(msgInfo==null) break;
								msg=msgInfo.getMsg();
	//							if(msg==null) continue;
								if(msg==null) break;
								consumer=consumersBalance();
								//记录发送状态信息
								SendInfo sendInfo=new SendInfo(msgInfo);
								sendInfo.setTopicAndFilter(topicAndFilter);
								sendInfo.setSendTime(System.currentTimeMillis());
								sendInfoMap.put(sendInfo.getTopicAndFilter().toString()+String.valueOf(sendInfo.getMsgOffset()),sendInfo);
								if(consumer!=null){
									consumer.writeAndFlush(msg);
									sendInfo.setStatus(SendStatus.SEND);
								}else {
									logger.error("消费者集群都不在线,须集群恢复后重投");
									sendInfo.setStatus(SendStatus.GROUPLOST);
								}
									
							}
						}
					}
				});
			}
		}
	}
	/**
	 * 发送当前集群 某一队列 特定offset范围的消息
	 * 此方式 为成功存储再转发
	 * @param queueIndex
	 * @param num 存出成功可发送条数
	 */
	public void sendSpecificMsgs(final int queueIndex,final int num){
		sendPool.execute(new Runnable(){

			@Override
			public void run() {
				int i=0;
				Channel consumer;
				MessageInfo msgInfo;
				Message msg;
				do{
					msgInfo=sendQueues.get(queueIndex).poll();
					if(msgInfo==null) break;
					msg=msgInfo.getMsg();
					if(msg==null) break;
					consumer=consumersBalance();
					consumer.writeAndFlush(msg);
					logger.debug("send msg:"+msg);
					SendInfo sendInfo=new SendInfo(msgInfo);
					sendInfo.setTopicAndFilter(topicAndFilter);
					sendInfo.setSendTime(System.currentTimeMillis());
					sendInfoMap.put(sendInfo.getMsgInfo().getMsgInfoId(),sendInfo);
				}while(++i<=num);
			}
			
		});
	}
	/**
	 * 设置队列的最大存储进度
	 * @param queueIndex
	 * @param maxOffset
	 */
	public void setMaxOffset( int queueIndex, int maxOffset){
		consumeOffsets.get(queueIndex).setMaxOffset(maxOffset);
	}
	public void handleConsumeResult(SendInfo sendInfo, ChannelHandlerContext ctx) {
		logger.error("result handle");
		String msgId = sendInfo.getTopicAndFilter().toString()+String.valueOf(sendInfo.getMsgOffset());
		/*
		 * 进度仅仅在此处收到ack 更新.无论成功或失败超时(进入重试队列)
		 * 假设此处未收到未更新进度,而超时扫描 到超时进入重试队列(即重试队列中消息对应offset大于此处更新的currentoffset,则必然此处currentoffset开始发生连续超时)
		 * 连续超时 将不会更新到重试 随offset表(队列)更新到 重试进度,如果故障broker重启将由此处currentoffset开始 连续恢复
		 */
//		topicAndgroup2subScribe.get(topic+"@"+groupId).get(sendInfo.getQueueId()).setCurrentoffset((sendInfo.getMsgOffset()));
		/**
		 * 当前集群投递成功
		 */
		if((!sendInfo.isTimeout())&&sendInfo.getStatus().equals(SendStatus.SUCCESS)){//未超时并且消费成功
			consumeOffsets.get(sendInfo.getQueueId()).setCurrentoffset(sendInfo.getMsgOffset());
			if(sendInfo.getReSendCount()!=0){
				logger.error("消息"+sendInfo.getMsgInfo().getMsg()+" 给集群"+this+" "+sendInfo.getReSendCount()+" 次重新投递成功");
			}else {
				System.out.println("消息"+sendInfo.getMsgInfo().getMsg()+" 给集群"+this+" "+sendInfo.getReSendCount()+" 次重新投递成功");
				logger.error("消息"+sendInfo.getMsgInfo().getMsgInfoId()+"一次投递消费成功");
			}
			sendInfoMap.remove(sendInfo.getMsgInfo().getMsgInfoId());
//			logger.error("sendInfoMap size:"+sendInfoMap.size());
		}else if(sendInfo.getStatus().equals(SendStatus.FAIL)){//消费失败
			consumeOffsets.get(sendInfo.getQueueId()).setCurrentoffset(sendInfo.getMsgOffset());
//			logger.error("消费失败"+consumeFailCount.incrementAndGet()+"条");
		}else if(sendInfo.isTimeout()){//消费超时，这个很有可能已经被超时扫描 给处理了，多久了才发过来
			if(sendInfoMap.containsKey(msgId)){//因为有可能已经被超时扫描 给处理了
				sendInfoMap.get(msgId).setStatus(SendStatus.TIMEOUT);
				//这里其实 由超时扫描来处理就好
			}
//			logger.error("消费超时"+consumeTimeoutCount.incrementAndGet()+"条");
		}
	}  
	/**
	 * 加入发送队列,queueId(queueIndex)将由存储队列均衡后的index来决定,保证了sendQueue与storeQueue的一致性,从而保证文件索引与消费进度offset的精确对应
	 * @param offset
	 * @param msgInfo
	 */
	 public void addToSendQueue(TopicAndFilter topicAndFilter/*  */,MessageInfo msgInfo){
//		 this.sendQueuesBalance().add(offset);
		 //获取队列和对应的Offset
		 sendQueues.get(msgInfo.getQueueIndex()).add(msgInfo);
		 Offset offset=consumeOffsets.get(msgInfo.getQueueIndex());
//		 if(offset.getTopicAndFilter()==null){
//			 offset.setTopicAndFilter(topicAndFilter);
//		 }
		 if(offset.getTopic()==null){
			 offset.setTopic(topicAndFilter.getTopic());
			 offset.setFilter(topicAndFilter.getFilter());
		 }
	 }
	
	public void addConsumer(Channel channel){
		if(!consumers.contains(channel)){
			consumers.add(channel);
		}
	}
	public String getGroupId() {
		return groupId;
	}
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	
	public LinkedList<Offset> getConsumeOffsets() {
		return consumeOffsets;
	}
	public String getSubscribedTopic() {
		return subscribedTopic;
	}
	public void setSubscribedTopic(String subscribedTopic) {
		this.subscribedTopic = subscribedTopic;
	}
	public Map<String, String> getSubscribedFilter() {
		return subscribedFilter;
	}
	public Map<String, SendInfo> getSendInfoMap() {
		return sendInfoMap;
	}
	public void setSendInfoMap(Map<String, SendInfo> sendInfoMap) {
		this.sendInfoMap = sendInfoMap;
	}
	public void setSubscribedFilter(Map<String, String> subscribedFilter) {
		this.subscribedFilter = subscribedFilter;
	}
	public LinkedList<Channel> getConsumers() {
		return consumers;
	}
	public void setConsumers(LinkedList<Channel> consumers) {
		this.consumers = consumers;
	}

	public int getSendQueueNum() {
		return sendQueueNum;
	}
	public void setSendQueueNum(int sendQueueNum) {
		this.sendQueueNum = sendQueueNum;
	}
	public LinkedList<BlockingQueue<MessageInfo>> getSendQueues() {
		return sendQueues;
	}
	public void setSendQueues(LinkedList<BlockingQueue<MessageInfo>> sendQueues) {
		this.sendQueues = sendQueues;
	}
	public int getSendQueueBalancer() {
		return sendQueueBalancer;
	}
	public TopicAndFilter getTopicAndFilter() {
		return topicAndFilter;
	}
	public void setTopicAndFilter(TopicAndFilter topicAndFilter) {
		this.topicAndFilter = topicAndFilter;
	}
	public void setSendQueueBalancer(int sendQueueBalancer) {
		this.sendQueueBalancer = sendQueueBalancer;
	}
	public String toString(){
		return "subscribedTopic"+subscribedTopic+" subscribedFilter"+subscribedFilter;
	}
/**
 * 非静态部分 为某一具体集群
 * 静态部分 属于集群组,管理所有订阅集群
 */
	/**
	 * 订阅集群组,每种topicAndfilter可能有一个或多个订阅集群
	 */
	public static  Map<String/* topicAndfilter */, ConcurrentHashMap<String/*groupid*/,ConsumerGroup/* group */>> topicAndFilter2groups = new ConcurrentHashMap<>();
	/**
	 * 添加 topicAndFilter的订阅集群
	 * @param topicAndFilter
	 * @param groupId
	 */
	public static ConsumerGroup addGroup(TopicAndFilter topicAndFilter,String groupId){
		Map<String,ConsumerGroup> groups=topicAndFilter2groups.get(topicAndFilter.toString());//集群组
		ConsumerGroup group=new ConsumerGroup(topicAndFilter.getTopic(),topicAndFilter.getFilter(),groupId);
		if(groups==null){
			groups=new ConcurrentHashMap<String,ConsumerGroup>();
			groups.put(groupId,group);
			topicAndFilter2groups.put(topicAndFilter.toString(), (ConcurrentHashMap<String, ConsumerGroup>) groups);
		}else if(!groups.containsKey(groupId)){
				groups.put(groupId, group);
		}
//		else {//集群已经存在,这种情况可能发生在集群全部掉线,而后恢复重连.或者集群新增消费者
//			group=groups.get(groupId);
//		}
		return group;
	}
	public static  ConcurrentHashMap<String,ConsumerGroup> getGroups(TopicAndFilter topicAndFilter){
		return topicAndFilter2groups.get(topicAndFilter.toString());
	}
	public static ConsumerGroup getGroup(TopicAndFilter topicAndFilter,String groupId){
		Map<String,ConsumerGroup> groups=getGroups(topicAndFilter);
		Map<String,ConsumerGroup> groupsNoFilter=getGroups(new TopicAndFilter(topicAndFilter.getTopic(),null));
		ConsumerGroup group;
		if(groupsNoFilter==null&&groups==null){
			return null;
		}else if(groups!=null&&(group=groups.get(groupId))!=null){
			return group;
		}else if(groupsNoFilter!=null){
			return groupsNoFilter.get(groupId);
		}else return null; 
	}
	/**
	 * 取得filter 和null两种needSend订阅集群组
	 * @param topicAndFilter
	 * @return
	 */
	public static ConcurrentHashMap<String,ConsumerGroup> getNeedSendGroups(TopicAndFilter topicAndFilter){
		ConcurrentHashMap<String,ConsumerGroup> groups=getGroups(topicAndFilter);
		TopicAndFilter topicNoFilter=new TopicAndFilter (topicAndFilter.getTopic(),null);
		ConcurrentHashMap<String,ConsumerGroup> noFilterGroups=getGroups(topicNoFilter);
	
		
		if(groups!=null&&noFilterGroups!=null){
			groups.putAll(getGroups(topicNoFilter));
		}else if(noFilterGroups!=null){
			groups=noFilterGroups;
		}
//		if(groups!=null)
//		for(ConsumerGroup group:groups.values()){
//			if(group.consumers.isEmpty()){
//				groups.remove(group.getGroupId());
//			}
//		}
		return groups;
	}
	/**
	 * 取得 此topicAndFilter下所有负载均衡后须发送的消费者
	 * @param topicAndFilter
	 * @return
	 */
	public static  LinkedList<Channel> getNeedSendBalancedConsumers(TopicAndFilter topicAndFilter){
		LinkedList<Channel> needSendBalancedConsumers=new LinkedList<Channel>();
		Map<String,ConsumerGroup>groups=getNeedSendGroups(topicAndFilter);
		for(ConsumerGroup consumerGroup:groups.values()){//每个须发送集群都要负载均衡找出消费者
			Channel consumer=consumerGroup.consumersBalance();
			if(consumer!=null){
				needSendBalancedConsumers.add(consumer);
			}else {
				logger.error("消费者集群"+consumerGroup+"全部失去连接");
			}
		}
		return needSendBalancedConsumers;
	}
	public static  int needSendGroupsNum(TopicAndFilter topicAndFilter){
		return getNeedSendGroups(topicAndFilter).size();
	}
//	public static boolean contains(TopicAndFilter topicAndFilter){
//		return topicAndFilter2groups.containsKey(topicAndFilter.toString());
//	}
	/**
	 * 注册订阅者(消费者)
	 * @param consumer
	 * @param groupId
	 * @param topicAndFilter
	 */
	public static void addConsumer(Channel consumer ,String groupId,TopicAndFilter topicAndFilter){
		ConsumerGroup group=getGroup(topicAndFilter,groupId);
		if(group!=null){
			if(group.consumers.size()==0){
				logger.error("消费者集群全部down机后恢复,触发消费者离线重投");
				/**
				 * TO DO..........................................................
				 */
			}
			group.addConsumer(consumer);
		}else addGroup(topicAndFilter,groupId).addConsumer(consumer);
	}
	/**
	 * 消费结果总处理
	 * @param consumeResult
	 * @param ctx
	 */
	public static void confirmConsumer(ConsumeResult consumeResult, ChannelHandlerContext ctx){
		logger.debug("recv consume result");
		String msgId = consumeResult.getMsgId();
		ConsumerGroup  group=ConsumerGroup.getGroup(consumeResult.getTopicAndFilter(),consumeResult.getGroupId());
		SendInfo sendInfo=group.sendInfoMap.get(msgId);
		if(sendInfo==null) return;
		switch (consumeResult.getStatus()){
			case SUCCESS:sendInfo.setStatus( SendStatus.SUCCESS); break;
			case FAIL:sendInfo.setStatus( SendStatus.FAIL); break;
			default:logger.error("消费者 无消费状态.......");break;
		}
		group.handleConsumeResult(sendInfo, ctx);
	}
	/**
	 * 准备消息发送(进入发送队列),存储完成后发送
	 * @param topicAndFilter
	 * @param msgInfo
	 */
	private static long waitSubStartTime;
	public static void  sendMsg(TopicAndFilter topicAndFilter,MessageInfo msgInfo){
		//获取消息应给发往的订阅集群组,即所有订阅此消息的集群
		Map<String,ConsumerGroup> groups=ConsumerGroup.getNeedSendGroups(topicAndFilter);
		if(groups==null){
			if(waitSubStartTime==0){
				waitSubStartTime=System.currentTimeMillis();
			}else if(System.currentTimeMillis()-waitSubStartTime>Config.noConsumerWaitTimeLimit){//800ms后仍无订阅者信息,则递归结束,
				msgInfo.getProducer().writeAndFlush("fail@"+msgInfo.getMsg().getMsgId()+ "\r\n");
				logger.error(Config.noConsumerWaitTimeLimit+"ms 任无此"+topicAndFilter+"的订阅集群(组)发起订阅");
				return;
			}
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//logger.error("没有此"+topicAndFilter+"的订阅集群(组)");
			sendMsg(topicAndFilter,msgInfo);//递归不断尝试获取订阅者集群
		}else{
			//设置需要发送的订阅集群数
//			msgInfo.setSubGroupsCount(groups.size());
			//加入各订阅集群的发送队列,自动发送给消费者
			logger.debug((System.currentTimeMillis()-waitSubStartTime)+"发现订阅信息");
			for(ConsumerGroup consumerGroup:groups.values()){
				consumerGroup.addToSendQueue(topicAndFilter,msgInfo);
			}
		}
		waitSubStartTime=0;
	}
	
//	/**
//	 * 以groupId区分俩个组
//	 */
//	@Override
//	public boolean equals(Object obj) {
//		if(obj instanceof ConsumerGroup){
//			if(((ConsumerGroup) obj).getGroupId().equals(this.groupId))return true;
//			else return false;
//		}else return false;
//	}
	

	
}
