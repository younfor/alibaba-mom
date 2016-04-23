package com.alibaba.middleware.race.mom.util;

import io.netty.channel.Channel;

import java.net.SocketAddress;

public class ConsumerInfo {
	private Channel channel;
	private SocketAddress consumerAddress;
	
	
	public ConsumerInfo(Channel channel){
		this.channel=channel;
		this.consumerAddress=channel.remoteAddress();
	}
	public Channel channel() {
		return channel;
	}
	
	public SocketAddress consumerAddress() {
		return consumerAddress;
	}
	@Override
	public boolean equals(Object obj) {
		if(this==obj){
			return true;
		}else if(obj instanceof ConsumerInfo){
			ConsumerInfo another=(ConsumerInfo)obj;
			return this.consumerAddress.equals(another.consumerAddress);
		}else return false;
	}
	
	
	
	
}
