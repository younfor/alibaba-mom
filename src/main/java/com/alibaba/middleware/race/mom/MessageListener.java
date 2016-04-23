package com.alibaba.middleware.race.mom;


public interface MessageListener {
	/**
	 * 
	 * @param message
	 * @return
	 */
	ConsumeResult onMessage(Message message);
}
