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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.mom.serializer.RpcDecoder;
import com.alibaba.middleware.race.mom.serializer.RpcEncoder;
import com.alibaba.middleware.race.mom.util.InfoBodyConsumer;
import com.alibaba.middleware.race.mom.util.TopicAndFilter;

public class DefaultConsumer implements Consumer, Serializable {
	
	private static final long serialVersionUID = -4454001515077708258L;
	private static Logger logger = LoggerFactory.getLogger(DefaultConsumer.class);
	private String groupId;
	private Map<String, String> filterMap;
	transient private ChannelFuture future;
	transient private NioEventLoopGroup group;
	transient private String SIP;
	private Message msg;
	private InfoBodyConsumer consumerRequestInfo;
	private MessageListener listener;
	private String topic;
	FSTConfiguration fst=FSTConfiguration.getDefaultConfiguration();
	Bootstrap bootstrap;
	boolean reconnect=false;
	@Override
	public void start() {
		prepare();
	}

	
	@Override
	public void prepare() {
		SIP=System.getProperty("SIP");
		if(SIP==null)
			SIP="127.0.0.1";
		System.out.println("consumer connect:"+System.getProperty("SIP"));
		group = new NioEventLoopGroup();
		try {
			bootstrap = new Bootstrap();
			bootstrap.group(group).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_REUSEADDR, true).option(ChannelOption.SO_KEEPALIVE, true)
//			.option(ChannelOption.TCP_NODELAY, true)
//            //
//            .option(ChannelOption.SO_KEEPALIVE, false)
//            //
//            .option(ChannelOption.SO_SNDBUF, 65535)
//            //
//            .option(ChannelOption.SO_RCVBUF,65535)
					.handler(new ChannelInitializer<SocketChannel>() {
						@Override
						public void initChannel(SocketChannel channel) throws Exception {
							channel.pipeline().addLast(new RpcEncoder())
									.addLast(new RpcDecoder())
									.addLast(new SimpleChannelInboundHandler<Object>() {

								@Override
								public void channelInactive(ChannelHandlerContext ctx) throws Exception {
									connect();
								}

								@Override
								public void exceptionCaught(ChannelHandlerContext ctx, Throwable e)
										throws Exception {
									logger.error("消费者异常关闭：");
									e.printStackTrace();
									ctx.close();
									
								}
								@Override
								protected void channelRead0(final ChannelHandlerContext ctx, Object info)
										throws Exception {
										logger.debug("client receive msg");
										Message msg =(Message)info;
										//返回ACK
										final ConsumeResult consumeResult=listener.onMessage(msg);
										//设置谁的 ack 
										consumeResult.setMsgId(msg.getMsgId());
										/*
										 *设置 consumeResult来源
										 */
										consumeResult.setGroupId(groupId);
										consumeResult.setTopicAndFilter(new TopicAndFilter(topic,filterMap));
//										if(Math.random()>0.95){
//											new Thread(new Runnable(){
//
//												@Override
//												public void run() {
//													// TODO Auto-generated method stub
//													try {
//														Thread.sleep(10000);
//														
//														ctx.writeAndFlush(consumeResult);
//													} catch (InterruptedException e) {
//														// TODO Auto-generated catch block
//														e.printStackTrace();
//													}
//												}
//												
//											}).start();;
//										}else 
										ctx.writeAndFlush(consumeResult);
										
								}
							});
						}
					});
					connect();
		} catch (InterruptedException e) {
			logger.error("消费者抛出异常  " + e.getMessage());
		}
	}

	public void connect() throws InterruptedException
	{
		try{
			future = bootstrap.connect(SIP, 9999).sync();
			future.channel().writeAndFlush(consumerRequestInfo);
			logger.error("连接成功");
		}catch(Exception e)
		{
//			Thread.sleep(1000);
			logger.error("重新连接");
			connect();
		}
	}
	@Override
	public void subscribe(String topic, String filter, MessageListener listener) {
		this.topic=topic;
		this.listener = listener;
		if (null != filter && (!"".equals(filter))) {
			filterMap = new HashMap<String, String>();
			String[] temp = filter.split("=",2);
			filterMap.put(temp[0],temp[1]);
			this.setFilterMap(filterMap);
			/*for(int i=0;i<temp.length;i++){
				filterMap.put(temp[i], temp[++i]);
			}*/
		}
		consumerRequestInfo = new InfoBodyConsumer();
		consumerRequestInfo.setFilterMap(filterMap);
		consumerRequestInfo.setTopic(topic);
		consumerRequestInfo.setGroupId(groupId);
	}

	
	@Override
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	
	@Override
	public String getGroupId() {
		return groupId;
	}

	
	@Override
	public Map<String, String> getFilterMap() {
		return filterMap;
	}

	
	@Override
	public void setFilterMap(Map<String, String> filterMap) {
		this.filterMap = filterMap;
	}

	@Override
	public Message getMsg() {
		return msg;
	}

	
	@Override
	public void setMsg(Message msg) {
		this.msg = msg;
	}

	
	@Override
	public InfoBodyConsumer getConsumerRequestInfo() {
		return consumerRequestInfo;
	}

	
	@Override
	public void setConsumerRequestInfo(InfoBodyConsumer consumerRequestInfo) {
		this.consumerRequestInfo = consumerRequestInfo;
	}

	
	@Override
	public String getTopic() {
		return topic;
	}

	
	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

	
	@Override
	public void stop() {
		future.channel().closeFuture();
		future.channel().close();
		group.shutdownGracefully();		
		logger.debug("消费者的释放了线程资源...");
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((filterMap == null) ? 0 : filterMap.hashCode());
		result = prime * result + ((groupId == null) ? 0 : groupId.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DefaultConsumer other = (DefaultConsumer) obj;
		if (filterMap == null) {
			if (other.filterMap != null)
				return false;
		} else if (!filterMap.equals(other.filterMap))
			return false;
		if (groupId == null) {
			if (other.groupId != null)
				return false;
		} else if (!groupId.equals(other.groupId))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}

}