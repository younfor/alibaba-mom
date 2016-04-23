package com.alibaba.middleware.race.mom.config;

public class Config {
	//每个group对应的队列条数
	public static int maxqueue=1;
	//内存每个队列只能装这么多条消息
	public static int maxMsgNum=30000/maxqueue;
	//每次从磁盘拉取多少条
	public static int maxLoadNum=2000;
	/**
	 * 重投次数限制
	 * 达到配置次数仍然失败，则不再重投
	 */
	public static int maxReSendTime=3;
	/**
	 * 10s超时限制
	 */
	public static int timeoutLimit=10000;
	/**
	 * 磁盘拉因子，当内存使用达到虚拟机内存（-Xms配置）的memoryFullFactor倍时，消息不能再存入内存，从磁盘拉取
	 */
	public static double memoryFullFactor=0.25;
	/**
	 * 无消费者时重复尝试时间 ms,可能系统启动时,订阅信息稍有延迟
	 */
	public static int noConsumerWaitTimeLimit=500;
	
}
