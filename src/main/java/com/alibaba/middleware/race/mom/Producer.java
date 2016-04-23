package com.alibaba.middleware.race.mom;


public interface Producer {

	void setSIP(String sip);

	void start();

	void setTopic(String topic);

	void setGroupId(String groupId);

	SendResult sendMessage(Message message);

	void asyncSendMessage(Message message, SendCallback callback);

	void stop();

}