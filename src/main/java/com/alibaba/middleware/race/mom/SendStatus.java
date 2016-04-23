package com.alibaba.middleware.race.mom;
/**
 * 发送状态
 *
 */
public enum SendStatus{
	SUCCESS,
	FAIL,
	TIMEOUT/*收到ack超时*/,
	/**
	 * 投递中
	 */
	SEND,
	/**
	 * 重投中
	 */
	RESEND,
	/**
	 * 消费失败重投
	 */
	FAILRESEND,
	/**
	 * 超时重投
	 */
	TIMEOUTRESEND,
	/**
	 * 订阅集群所有消费者不在线
	 */
	GROUPLOST
}
