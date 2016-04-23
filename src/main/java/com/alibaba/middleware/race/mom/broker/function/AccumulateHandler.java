package com.alibaba.middleware.race.mom.broker.function;

import java.util.Timer;
import java.util.TimerTask;

import com.alibaba.middleware.race.mom.config.Config;

/**
 * 消息堆积处理,当内存消息堆积过多,将不再内存中缓存消息,堆积减少后从硬盘拉取消息
 * 如果有大量超时消费者集群和正常快速消费集群,大量超时消息将不堆积内存,从磁盘拉取,保证不影响正常集群的消费tps
 * @author young
 *
 */
public class AccumulateHandler {
	public static volatile boolean full =false;
	private static Timer memoryCheckTimer=new Timer();
	/**
	 * 是否可以继续内存添加消息
	 * @param memoryCacheMaxOffset
	 * @return
	 */
	public static void startCheckMemory(){
		memoryCheckTimer.schedule(new TimerTask(){
				@Override
				public void run() {
					if((double)Runtime.getRuntime().freeMemory()/Runtime.getRuntime().totalMemory()<(1-Config.memoryFullFactor)){//可用内存小于虚拟机内存3/4时不能再往内存堆积消息
						full=true;
					}
				}
		},5000,5000);
	}
}
