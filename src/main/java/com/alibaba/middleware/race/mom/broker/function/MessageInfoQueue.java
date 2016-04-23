package com.alibaba.middleware.race.mom.broker.function;

import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.alibaba.middleware.race.mom.QueueMessage;

public class MessageInfoQueue {
	private Channel producer;
	private BlockingQueue<QueueMessage> msgQueue;
	private String topic;
	private Map<String, String> properties = new HashMap<String, String>();
	private String msgQueueId;
	
	
	public MessageInfoQueue(BlockingQueue<QueueMessage> msgQueue, String topic,
			Map<String, String> properties) {
		super();
		this.msgQueue = msgQueue;
		this.topic = topic;
		this.properties = properties;
	}
	
	
	
	
	
}
