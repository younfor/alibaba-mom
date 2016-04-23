package com.alibaba.middleware.race.mom;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.mom.broker.function.MessageManager;
import com.alibaba.middleware.race.mom.broker.function.MessageSend;
import com.alibaba.middleware.race.mom.broker.group.ConsumerGroup;
import com.alibaba.middleware.race.mom.serializer.RpcDecoder;
import com.alibaba.middleware.race.mom.serializer.RpcEncoder;
import com.alibaba.middleware.race.mom.util.InfoBodyConsumer;
import com.alibaba.middleware.race.mom.util.TopicAndFilter;


public class Broker {
	private static Logger logger = LoggerFactory.getLogger(Broker.class);
	/**
	 * 启动broker之前的netty服务器处理
	 * @param port
	 * @throws Exception
	 */
	public void bind(int port) throws Exception {
		//启动消费进度定时任务
//		storeSubscribe(true, 5000);
//		scanSendInfotMap();
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors()*3);
		try {
			ServerBootstrap serverBootstrap = new ServerBootstrap();
			serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
			 .option(ChannelOption.SO_REUSEADDR, true)
			 .childOption(ChannelOption.TCP_NODELAY, true)
					.option(ChannelOption.SO_BACKLOG, 1024*1024)
			 .childOption(ChannelOption.SO_KEEPALIVE, true)
             //
//             .option(ChannelOption.SO_BACKLOG, 1024)
//             //
//             .option(ChannelOption.SO_REUSEADDR, true)
//             //
//             .option(ChannelOption.SO_KEEPALIVE, false)
//             //
//             .childOption(ChannelOption.TCP_NODELAY, true)
//              .option(ChannelOption.SO_SNDBUF, 65535)
//                    //
//              .option(ChannelOption.SO_RCVBUF, 65535)
              .childHandler(new ChannelInitializer<Channel>() {
						@Override
						protected void initChannel(Channel ch) throws Exception {
							ch.pipeline().addLast(new RpcDecoder()).addLast(new RpcEncoder())
									.addLast(new SimpleChannelInboundHandler<Object>() {
								@Override
								protected void channelRead0(ChannelHandlerContext ctx, Object info) throws Exception {
									if (InfoBodyConsumer.class.isInstance(info)) {
										// 接受消费者订阅处理
										processConsumer((InfoBodyConsumer) info, ctx);
									} else if (ConsumeResult.class.isInstance(info)) {
										// 收到消费者ConsumeResult
										logger.debug(" 收到消费者ConsumeResult");
										ConsumerGroup.confirmConsumer((ConsumeResult)info, ctx);
//										confirmConsumer((ConsumeResult) info, ctx);
									}else if(MessageSend.class.isInstance(info)){
										//System.out.println("收到消息");
											MessageManager.recieveMsg((MessageSend) info,ctx);
//										MessageManager.recieveMsg((LinkedBlockingQueue)info,ctx);
									}
								}
								
								@Override
								public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
									logger.error("broker 异常：");
//									logger.error(cause.getMessage());
									cause.printStackTrace();
									ctx.close();
								}
							});
						}
					});//

			ChannelFuture future = serverBootstrap.bind(port).sync();
			logger.debug("mom服务启动成功...... 绑定端口" + port);

			// 等待服务端监听端口关闭
			future.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			logger.error("smom服务抛出异常  " + e.getMessage());
		} finally {
			// 优雅退出 释放线程池资源
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			logger.debug("mom服务优雅的释放了线程资源...");
		}

	}
	/**
	 * 收到消费者的订阅关系，并创建队列
	 * @param consumerRequestInfo
	 * @param ctx
	 */
	public void processConsumer(InfoBodyConsumer consumerRequestInfo, final ChannelHandlerContext ctx) {

		String topic = consumerRequestInfo.getTopic();
		String groupid = consumerRequestInfo.getGroupId();
		Map<String, String> filter = consumerRequestInfo.getFilterMap();
		TopicAndFilter topicAndFilter = new TopicAndFilter(topic, filter);
		Channel consumer = ctx.channel();
		logger.error("消费者:" + consumer.hashCode()+"发起订阅"+consumerRequestInfo);
		ConsumerGroup.addConsumer(consumer,groupid,topicAndFilter);
//		
	}
	
//	public void scanSendInfotMap(){
//		this.timoutScannerTimer.schedule(new TimerTask(){
//			@Override
//			public void run() {
//				for(Map.Entry<String , SendInfo> entry:msgManager.getSendInfoMap().entrySet()){
//					SendInfo sendInfo=entry.getValue();
//					for(Map.Entry<ConsumerGroup , SendStatus> groupstatus:sendInfo.getGroup2status().entrySet()){
//						if(groupstatus.getValue()==SendStatus.TIMEOUT||sendInfo.isTimeout(System.currentTimeMillis())){
//							String msgId=sendInfo.getMsgId();
//							final String topic=sendInfo.getTopic();
//							final TopicAndFilter topicAndfilter=sendInfo.getTopicAndFilter();
//							final ConsumerGroup group=groupstatus.getKey();
//							BlockingQueue<String> reSendMessageQueue=topicAndgroup2reSendMsgQueue.get(topic+ "@" +group);
//							if(reSendMessageQueue==null){
//								reSendMessageQueue=new LinkedBlockingQueue<String>();
//								reSendMessageQueue.add(msgId);
//								topicAndgroup2reSendMsgQueue.put(topic+ "@" +group,reSendMessageQueue);
//							}else {
//								reSendMessageQueue.add(msgId);
//							}
//	//						messageCache.remove(msgId);//消息进入重试队列，这里应该移除了，sendResultMap还可以重试结果使用
//							reSendPool.execute(new Runnable(){
//								@Override
//								public void run() {
//									reSendToConsumer(topicAndgroup2reSendMsgQueue.get(topic+ "@" +group),group);
//								}
//							});
//	//						if(msgid2offset.containsKey(msgId)){
//	//							topicAndgroup2subScribe.get(topic+"@"+group).get(sendInfo.getQueueId()).setCurrentoffset(msgid2offset.get(msgId));
//	//							msgid2offset.remove(msgId);
//	//						}
//						}
//					}
//				}
//				
//			}
//		},timeoutLimit+100,1000);
//	}
	/**
	 * 恢复订阅关系
	 */
//	public void recover(){
//		logger.debug("恢复订阅关系队列");
//		long startTime = System.currentTimeMillis();
//		ArrayList<Offset>offsets =substore.read();
//		System.out.println("offsets:"+offsets);
//		String topic;
//		String group;
//		String queueId;
//		String topicAndFilter;
//		for(Offset offset:offsets){
//			topic=offset.getTopicId();
//			topicAndFilter=offset.getTopicAndFilter();
//			group=offset.getGroupId();
//			queueId=offset.getQueueId();
//			//恢复topicAndgroup2subScribe
//			LinkedList<Offset> offsetList;
//			if(!topicAndgroup2subScribe.containsKey(topic+"@"+group))
//			{
//				offsetList=new LinkedList<Offset>();
//				topicAndgroup2subScribe.put(topic+"@"+group, offsetList);
//			}else
//			{
//				offsetList=topicAndgroup2subScribe.get(topic+"@"+group);
//			}
//			offsetList.add(Integer.parseInt(queueId),offset);
//			//恢复topicAndgroup2sendMsgQueue
//			LinkedList<BlockingQueue<Integer>> sendQueuesList;
//			if(!topicAndgroup2sendMsgQueue.containsKey(topic + "@" + group))
//			{
//				sendQueuesList=new LinkedList<BlockingQueue<Integer>>();
//				topicAndgroup2sendMsgQueue.put(topic + "@" + group, sendQueuesList);
//			}else
//			{
//				sendQueuesList=topicAndgroup2sendMsgQueue.get(topic + "@" + group);
//			}
//			BlockingQueue<Integer>sendQueue=new LinkedBlockingQueue<>();
//			sendQueuesList.add(Integer.parseInt(queueId),sendQueue);
//			//消息入队(这里只磁盘拉)
//			int currentOffset=offset.getCurrentoffset();
//			Message msg;
//			int i=0;//currentOffset偏移量
//			List<byte[]> blist = mstore.readByteNormal(topicAndFilter,"t","t",queueId+"",offset.getCurrentoffset()+1, offset.getCacheOffset()-1);
//			System.out.println(blist.size());
//			for(byte[] b:blist){
//				try{
//					msg=(Message)fst.asObject(b);
//				}catch(Exception e){
//					continue;
//				}
//				System.out.println(msg);
//				sendQueue.add(msg.getOffset());
//				msgManager.getMessageCacheMap().put(msg.getOffset(), msg);
//				msgid2offset.put(msg.getMsgId(), currentOffset+i);
//				i++;
//			}
//			
//		}
//		
//		long endTime = System.currentTimeMillis();
//		System.out.println("恢复cost：" + (endTime - startTime));
//	}
	/**
	 * 仅更新到磁盘 Subscribe之前的 offset,这部分属于 离散重试(至少中间有一条成功) ,须重试offset表存储到磁盘,broker故障重启只能离散随机恢复消息
	 * 如果存在Subscribe之后的 offset,必然发生连续>=1(甚至大量)超时,连续超时将随Subscribe存储到磁盘,broker故障重启将发生连续一片(>=1条)消息恢复
	 * @param start
	 * @param during
	 */
	public void storeResendOffset(boolean start,long during/*ms*/){
		
	}
	/**
	 * 定时任务-存储订阅关系
	 * @param start
	 * @param during
	 */
//	public void storeSubscribe(boolean start,long during/*ms*/)
//	{
//		if(start==false)
//			return;
//		logger.debug("异步-订阅关系/消费进度定时持久化任务");
//		
//		shceduledPool.scheduleWithFixedDelay(  
//	            new Runnable() {
//					@Override
//					public void run() {
//						 ArrayList<Offset> sublist=new ArrayList<>();
//						logger.error("遍历订阅关系:"+topicAndgroup2subScribe.size());
//						 for (Map.Entry<String/* topic@group */, LinkedList<Offset>> entry : topicAndgroup2subScribe.entrySet()) 
//						 {
//							    //System.out.println("key= " + entry.getKey() + " and value= " + entry.getValue());
//							 	sublist.addAll(entry.getValue());
//						 }
//						substore.write(sublist);
//						logger.error("订阅关系刷盘");
//					}
//				},  
//	            5000,  
//	            during,  
//	            TimeUnit.MILLISECONDS);  
//	}
	public static void main(String[] args) throws Exception {
	
		int port = 9999;
		Broker broker=new Broker();
//		Recover recover=new Recover();
//		recover.recover();
//		SubsStore.subscribeStoreStart();
		broker.bind(port);
		
	}

}