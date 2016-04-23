package com.alibaba.middleware.race.mom;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.mom.broker.function.MessageSend;
import com.alibaba.middleware.race.mom.serializer.RpcEncoder;


public class DefaultProducer implements Producer {
	transient private static Logger logger = LoggerFactory.getLogger(DefaultProducer.class);
	private String topic;
	private String groupId;
	private static String producerId="pro_"+String.valueOf(Math.random()*100);
	private ChannelFuture future;
	private static Random random=new Random();
	private NioEventLoopGroup group;
	private String SIP;
	private SendCallback callback;
	private ExecutorService sendPool = Executors.newFixedThreadPool(1);
	private BlockingQueue<Message> sendQueue=new LinkedBlockingQueue<Message>();
	private Map<String,BlockingQueue<SendResult>> resultMap=new ConcurrentHashMap<String, BlockingQueue<SendResult>>();
	private Bootstrap bootstrap;
	boolean isGroupMsg=false;
	private AtomicLong msgIdProduce=new AtomicLong();
	private CountDownLatch sendWait=new CountDownLatch(1);
	private Object lock=new Object();
	private Map<String,SendCallback>  asyncResults=new HashMap<>();
	
	
	public DefaultProducer() {
		
	}

	
	
	@Override
	public void asyncSendMessage(Message message, SendCallback callback) {
		Message msg = buildMessage(message, callback,1);
		//加入同步等待队列  
		logger.debug("生产者准备发起请求和消息");
//		resultMap.put(message.getMsgId(), new LinkedBlockingQueue<SendResult>(1));
		asyncResults.put(msg.getMsgId(),callback);
		MessageSend msgSend=new MessageSend();
		msgSend.getSendIds().add(msg.getMsgId());
		future.channel().writeAndFlush(msgSend);
	}
	
	private AtomicInteger msgCount=new AtomicInteger();
	@Override
	public SendResult sendMessage(Message message) {
		int msgCountL=msgCount.incrementAndGet();
		Message msg = buildMessage(message, null,"ackGroup", msgCountL);
		resultMap.put(message.getMsgId(), new LinkedBlockingQueue<SendResult>(1));
		sendQueue.add(msg);
		if(msgCountL>199){
			sendWait.countDown();
		}
		SendResult sr=null;
		try {
			sr = resultMap.get(message.getMsgId()).poll(5000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(sr==null)
		{
			sr=new SendResult();
			sr.setStatus(SendStatus.FAIL);
		}
//		synchronized(lock){
//			try {
//				lock.wait();
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		return sr;
	}
	
	/**
	 * 对Message 进行必要的信息设置,测试设置了groupId和topic	producer.setGroupId(PID+code);producer.setTopic(topic);
	 * @param msg
	 * @param callback
	 * @return
	 */
	private Message buildMessage (Message msg, SendCallback callback,int count) {
		if (callback != null) {
			this.callback = callback;
			logger.debug("生产者异步发送消息");
		}
		msg.setMsgId(String.valueOf(Math.random())+" "+String.valueOf(count));
		msg.setTopic(topic);
		msg.setBornTime(System.currentTimeMillis());
		return msg;
	}
	private Message buildMessage (Message msg, SendCallback callback,String qianzhui,int count) {
		if (callback != null) {
			this.callback = callback;
			logger.debug("生产者异步发送消息");
		}
		//msg.setMsgId(qianzhui+" "+String.valueOf(count));
		msg.setMsgId(String.valueOf((int)(Math.random()*999%999))+" "+String.valueOf(count));
		msg.setTopic(topic);
		msg.setBornTime(System.currentTimeMillis());
		return msg;
	}
	@Override
	public void start() {
		SIP=System.getProperty("SIP");
		if(SIP==null)
			SIP="127.0.0.1";
		System.out.println("connect:"+System.getProperty("SIP"));
		group = new NioEventLoopGroup();
		bootstrap = new Bootstrap();
		bootstrap.group(group).channel(NioSocketChannel.class)
		.option(ChannelOption.SO_REUSEADDR, true).option(ChannelOption.SO_KEEPALIVE, true)
		.option(ChannelOption.TCP_NODELAY, true)
//            //
        .option(ChannelOption.SO_KEEPALIVE,true)
//            //
//            .option(ChannelOption.SO_SNDBUF, 65535)
//////            //
//            .option(ChannelOption.SO_RCVBUF,65535)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel channel) throws Exception {
						channel.pipeline()
								.addLast(new RpcEncoder())
								.addLast(new LineBasedFrameDecoder(1024*256))
								.addLast(new StringDecoder())
								.addLast(new SimpleChannelInboundHandler<Object>() {
						
							@Override
							public void channelInactive(ChannelHandlerContext ctx) throws Exception {
								logger.error("producer失去broker链接");
								connect();
							}
							@Override
							protected void channelRead0(ChannelHandlerContext arg0, Object info) throws Exception {
									
								logger.debug("recv ack： "+(String)info);
								
								if(info!=null/*&&info.toString().startsWith("ackGroup"*/){
									String id[]=((String)info).split("@");
									//System.out.println("收到:"+info.toString());
									//System.out.println(id.length+":len:"+id[0]+" | "+id[1]);
									if(id[0].startsWith("fail"))
									{
										//System.out.println("无人订阅");
										SendResult sr=new SendResult();
										sr.setMsgId(id[1]);
										resultMap.get(id[1]).put(sr);
										
									}else
									{
										for(int i=1;i<id.length;i++)
										{
											//System.out.println(id[i]);
											SendResult sr=new SendResult();
											sr.setMsgId(id[i]);
											resultMap.get(id[i]).put(sr);
										}
									}
//									synchronized(lock){
//										lock.notifyAll();
//									}
								} else {							
									// 异步方式接受数据
									if(asyncResults.containsKey((String)info)){
										SendResult sr=new SendResult();
										sr.setMsgId((String)info);
										asyncResults.get((String)info).onResult(sr);;
									}else{
										
									}
								}
							}
							@Override
							public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
							
								logger.error("--生产者的异常关闭--" + cause.getMessage());
								cause.printStackTrace();
								logger.error("重新连接");
								ctx.close();
								connect();
							}
						});
					}
				});
		connect();
	}
	/**
	 * 连接
	 * 
	 */
	public void connect() {
			try {
				future = bootstrap.connect(SIP, 9999).sync();
				logger.error("连接成功");
				// TODO Auto-generated catch block
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error("重新连接");
					connect();
				}
			
			/*
			 * 添加发送任务
			 */
			sendPool.execute(new Runnable(){
				@Override
				public void run() {
					
					while(true){
						long start=System.currentTimeMillis();
//						messageGroup=false;
//						 AtomicInteger waitTime=new AtomicInteger();
						/**
						 * 这部分待完善，现在为了测tps，先不管系统的真实可用性了
						 */
						try {
							sendWait.await(30,TimeUnit.MILLISECONDS);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						/**
						 * 创建本次祖发对象，同时产生borntime
						 */
						MessageSend msgSend=new MessageSend();
						
						while(true){
							logger.debug("start make messageGroup");
							Message msg=null;
							msg = sendQueue.poll();
//							if(!isGroupMsg&&msg==null){
//								break;
//							}
							if (msg==null&&msgSend.getBodys().size()>199){ 
								break;
							}else if (msg==null&&msgSend.getBodys().size()==1){ 
								try {//很有可能只有一条消息，比如函数测试，再确认一下
									msg = sendQueue.poll(10,TimeUnit.MILLISECONDS);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								if(msg==null){
									logger.error("10ms no message produced after last first message");
									break;
								}
							}else if(msg==null){
								continue;
							}
							if(msgSend.getTopic()==null||msgSend.getProperties()==null){
								msgSend.setProperties(msg.getProperties());
								msgSend.setTopic(msg.getTopic());
							}
							msgSend.getBodys().add(msg.getBody());
							msgSend.getSendIds().add(msg.getMsgId());
						}
						if(msgSend.getBodys().size()>0){
							
							//msgSend.setSendId("ackGroup "+String.valueOf(msgSend.getBodys().size()));
//							logger.error((System.currentTimeMillis()-start)+"后生产者一次打包发送"+msgSend.getBodys().size());
							future.channel().writeAndFlush(msgSend);
							msgCount.set(0);
							sendWait=new  CountDownLatch(1);
						}
					}
				}
				
			});
			
	}
	@Override
	public void stop() {
		future.channel().closeFuture();
		future.channel().close();
		group.shutdownGracefully();
		sendPool.shutdown();
	}
	

	@Override
	public void setGroupId(String groupId) {
		// TODO Auto-generated method stub
		this.groupId = groupId;
	}

	@Override
	public void setSIP(String sip) {
		this.SIP = sip;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

}
