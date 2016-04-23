package com.alibaba.middleware.race.mom.broker.function;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.mom.broker.group.ConsumerGroup;
import com.alibaba.middleware.race.mom.store.SubscribeStore;
import com.alibaba.middleware.race.mom.store.SubscribeStoreImp;

public class SubsStore {
	private static Logger logger = LoggerFactory.getLogger(SubsStore.class);
	private static SubscribeStore substore=new SubscribeStoreImp();
	private static Timer timer=new Timer();
	/**
	 * 定时任务-存储订阅关系
	 * @param start
	 * @param during
	 */
	public static void subscribeStoreStart(){
		
		
		timer.schedule(new TimerTask(){  
			@Override
			public void run() {
				logger.error("异步-订阅关系/消费进度定时持久化任务");
				 ArrayList<Offset> sublist=new ArrayList<>();
				 for(ConcurrentHashMap<String/*groupid*/,ConsumerGroup/* group */> consumerGroups:ConsumerGroup.topicAndFilter2groups.values()){
					 for (ConsumerGroup consumerGroup :consumerGroups.values()){
						    //System.out.println("key= " + entry.getKey() + " and value= " + entry.getValue());
						 	sublist.addAll(consumerGroup.getConsumeOffsets());
					 }
				}
				 
				substore.write(sublist);
				logger.error("订阅消费进度关系刷盘"+sublist);
			}
		},1000, 1000);  
	}
}
