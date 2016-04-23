//package com.alibaba.middleware.race.mom;
//
//import io.netty.bootstrap.Bootstrap;
//import io.netty.channel.ChannelFuture;
//import io.netty.channel.ChannelHandlerContext;
//import io.netty.channel.ChannelInitializer;
//import io.netty.channel.ChannelOption;
//import io.netty.channel.SimpleChannelInboundHandler;
//import io.netty.channel.nio.NioEventLoopGroup;
//import io.netty.channel.socket.SocketChannel;
//import io.netty.channel.socket.nio.NioSocketChannel;
//import io.netty.handler.codec.LineBasedFrameDecoder;
//import io.netty.handler.codec.string.StringDecoder;
//
//import java.util.ArrayList;
//import java.util.Map;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.mom.serializer.RpcEncoder;
//import com.alibaba.middleware.race.mom.util.InfoBodyProduceList;
//
//
//public class DefaultProducer1 implements Producer {
//	transient private static Logger logger = LoggerFactory.getLogger(DefaultProducer.class);
//	private String topic;
//	private String groupId;
//	private static String producerId="pro_"+String.valueOf(Math.random()*100);
//	private ChannelFuture future;
//	private NioEventLoopGroup group;
//	private String SIP;
//	Object obj=new Object();
//	private SendCallback callback;
//	private SendResult sr;
//	private Map<String,BlockingQueue<SendResult>> resultMap=new ConcurrentHashMap<String, BlockingQueue<SendResult>>();
//	private BlockingQueue<MessageInfo> sendQueue= new LinkedBlockingQueue<MessageInfo>(100);
//	private ExecutorService sendPool = Executors.newFixedThreadPool(1);
//	private Bootstrap bootstrap;
//	public DefaultProducer1() {startSend();}
//
//	@Override
//	public void asyncSendMessage(Message message, SendCallback callback) {
//		MessageInfo msgInfo = buildMessage(message, callback);
//		//加入同步等待队列  
//		logger.debug("生产者准备发起请求和消息");
//		resultMap.put(message.getMsgId(), new LinkedBlockingQueue<SendResult>(1));
//		future.channel().writeAndFlush(msgInfo);
//	}
//	/**
//	 * 对Message 进行必要的信息设置,测试设置了groupId和topic	producer.setGroupId(PID+code);producer.setTopic(topic);
//	 * @param msg
//	 * @param callback
//	 * @return
//	 */
//	private MessageInfo buildMessage (Message msg, SendCallback callback) {
//		if (callback != null) {
//			this.callback = callback;
//			logger.debug("生产者异步发送消息");
//		}
//		
//		msg.setTopic(topic);
//		msg.setBornTime(System.currentTimeMillis());
//		msg.makeID();
//		return new MessageInfo(msg,producerId,this.groupId);
//	}
//
//	@Override
//	public SendResult sendMessage(Message message) {
//		 MessageInfo msgInfo = buildMessage(message, null);
//		 while(!sendQueue.offer(e)){
//			 
//		 }
//		 
//		 if(sendQueue.put(msgInfo);(msgInfo)){
//			 sendQueue.size() 
//		 }else {
//			 
//			 new MessageInfoQueue(sendQueue,);
//			 sendQueue=new LinkedBlockingQueue<MessageInfo>(100);
//		 }
//			
//			synchronized(obj){
//				try {
//					obj.wait();
//				} catch (InterruptedException e) {
//				}
////		resultMap.put(message.getMsgId(), new LinkedBlockingQueue<SendResult>(1));
////		SendResult sr=null;
////		try {
////			sr=resultMap.get(message.getMsgId()).take();
////		} catch (InterruptedException e) {
////			sr=new SendResult();
////			sr.setStatus(SendStatus.FAIL);
////		}
//		
//		return sr;}
//	}
//	
//	public void buildQueue(){
//		MessageInfoQueue msgInfoQueue=new MessageInfoQueue();
//		while(!sendQueue.isEmpty()){
//			sendQueue.
//			msgInfoQueue.
//		}
//		Message msg=sendQueue.poll();
//		
//		
//		
//		
//	}
//	
//
//	@Override
//	public void setGroupId(String groupId) {
//		// TODO Auto-generated method stub
//		this.groupId = groupId;
//	}
//
//	@Override
//	public void setSIP(String sip) {
//		this.SIP = sip;
//	}
//
//	@Override
//	public void setTopic(String topic) {
//		this.topic = topic;
//	}
//	public void startSend(){
//		sendPool.execute(new Runnable(){
//	
//			@Override
//			public void run() {
//				while(true){
//					System.out.println(sendQueue.size());
//					if(sendQueue.size()==200){
//						future.channel().writeAndFlush(sendQueue);
//						sendQueue.clear();
//					}
//				}			
//			}
//			
//		});
//	}
//	@Override
//	public void start() {
////		try {
////			Thread.sleep(500);
////		} catch (InterruptedException e1) {
////			e1.printStackTrace();
////		}
//		SIP=System.getProperty("SIP");
//		if(SIP==null)
//			SIP="127.0.0.1";
//		System.out.println("connect:"+System.getProperty("SIP"));
//		group = new NioEventLoopGroup();
//		try {
//			bootstrap = new Bootstrap();
//			bootstrap.group(group).channel(NioSocketChannel.class)
//					.option(ChannelOption.SO_REUSEADDR, true).option(ChannelOption.SO_KEEPALIVE, true)
//					.handler(new ChannelInitializer<SocketChannel>() {
//						@Override
//						public void initChannel(SocketChannel channel) throws Exception {
//							channel.pipeline()
//									.addLast(new RpcEncoder())
//									.addLast(new LineBasedFrameDecoder(1024))
//									.addLast(new StringDecoder())
//									.addLast(new SimpleChannelInboundHandler<Object>() {
//							
//								@Override
//								public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//									logger.error("producer失去broker链接");
//									connect();
//								}
//								@Override
//								protected void channelRead0(ChannelHandlerContext arg0, Object info) throws Exception {
//									// main work;
//									String msgId=info.toString();
//									sr=new SendResult();
//									sr.setMsgId(msgId);
//									synchronized(obj){
////										DefaultProducer.this.sr=sr;
//										obj.notifyAll();
////										obj.notify();
//									}
////									if (null == callback) {
////										resultMap.get(msgId).add(sr);
////										return;
////									} else {							
////										// 异步方式接受数据
////										callback.onResult(sr);
////									}
//								}
//								@Override
//								public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
//								
//									logger.error("--生产者的异常关闭--" + cause.getMessage());
//									cause.printStackTrace();
//									ctx.close();
//								}
//							});
//						}
//					});
//			connect();
//		} catch (InterruptedException e) {
//			logger.error("producer 抛出异常  " + e.getMessage());
//		}
//	}
//	public void connect() throws InterruptedException
//	{
//		try{
//			future = bootstrap.connect(SIP, 9999).sync();
//			logger.error("连接成功");
//		}catch(Exception e)
//		{
//			Thread.sleep(1000);
//			logger.error("重新连接");
//			connect();
//		}
//	}
//	@Override
//	public void stop() {
//		group.shutdownGracefully();
//	}
//
//}
