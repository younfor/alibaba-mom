package com.alibaba.middleware.race.mom.broker.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.mom.SendStatus;
import com.alibaba.middleware.race.mom.broker.group.ConsumerGroup;
import com.alibaba.middleware.race.mom.config.Config;
import com.alibaba.middleware.race.mom.store.ArrayStore;
import com.alibaba.middleware.race.mom.store.ArrayStoreImp;
import com.alibaba.middleware.race.mom.store.OffsetNum;


/**
 * 发送状态扫描器
 * 一.负责检测消费超时,失败等,如果超时或失败加入重投
 * 二.负责内存检测,一旦内存堆积过多超过内存大小警戒线,超时堆积消息将移除,保证系统正常运行以及正常消费tps
 * @author young
 *
 */
public class SendInfoScanner {
	private static Logger logger = LoggerFactory.getLogger(SendInfoScanner.class);
	
	private ConsumerGroup group;
	private Timer timoutScannerTimer;
	/**
	 * 重投配置
	 */
	private int timeoutLimit=10000;
	private int maxReSendTime=10000000;
	/**
	 * 重发线程池大小,也许可以调整大小来削峰谷
	 */
	private int reSendPoolSize=2;
	private ExecutorService reSendPool;
	private boolean reSendStarted=false;
	/**
	 * 重投队列,死信队列,记得刷盘...TO DO.......................................................................
	 */
	private BlockingQueue<MessageInfo/*msgoffset*/> reSendQueue=new LinkedBlockingQueue<MessageInfo/*msgoffset*/>();
	private BlockingQueue<MessageInfo/*msgoffset*/> deadMessageQueue=new LinkedBlockingQueue<MessageInfo/*msgoffset*/>();

	private ArrayStore arrayStore=new ArrayStoreImp();
	private AtomicInteger reSendFailCount=new AtomicInteger();
	
	/**
	 * 重投恢复
	 */
	ExecutorService recoverPool=Executors.newFixedThreadPool(1);
	public SendInfoScanner(ConsumerGroup group){
		this.group=group;
	}
	public void  start(){
		scanSendInfoMap();
	}
	public void scanSendInfoMap(){
			if(timoutScannerTimer==null){
				timoutScannerTimer=new Timer();
			}
			timoutScannerTimer.schedule(new TimerTask(){
			final Map <String,Map<String,ArrayList<OffsetNum>>> topicAndFilter2queues=new HashMap<>();
			Map<String,ArrayList<OffsetNum>> queue2offsets;
			ArrayList<OffsetNum> offsets;
			Map<String,SendInfo> sendInfoMap=group.getSendInfoMap();
			@Override
			public void run() {
				SendInfo sendInfo;
				for(Map.Entry<String , SendInfo> entry:sendInfoMap.entrySet()){
					sendInfo=entry.getValue();
					if(sendInfo.getStatus()==SendStatus.TIMEOUT||sendInfo.getStatus()==SendStatus.FAIL||sendInfo.isTimeout()){
						if(sendInfo.getReSendCount()<maxReSendTime){
//							logger.error("消息"+sendInfo.getMsgId()+sendInfo.getStatus()+"加入重投队列");
							if(!reSendStarted){//第一次此集群出现需要重投
								startReSend();
								reSendStarted=true;
							}
							/*
							 * 内存已经超出,应该移除重投部分部分
							 */
//							if(AccumulateHandler.full){
//							if(sendInfoMap.size()>5000){
//								logger.error("内存超出"+sendInfoMap.size());
//								/*
//								 * 内存不足,存储重投部分offset
//								 */
//								if((queue2offsets=topicAndFilter2queues.get(sendInfo.getTopicAndFilter().toString()))!=null){
//									if((offsets=queue2offsets.get(String.valueOf(sendInfo.getQueueId())))!=null){
//										offsets.add(new OffsetNum().setI(sendInfo.getMsgOffset()));
//									}else{
//										offsets=new ArrayList<OffsetNum>();
//										offsets.add(new OffsetNum().setI(sendInfo.getMsgOffset()));
//										queue2offsets.put(String.valueOf(sendInfo.getQueueId()), offsets);
//									}
//								}else {
//									offsets=new ArrayList<OffsetNum>();
//									offsets.add(new OffsetNum().setI(sendInfo.getMsgOffset()));
//									queue2offsets=new HashMap<>();
//									queue2offsets.put(String.valueOf(sendInfo.getQueueId()), offsets);
//									topicAndFilter2queues.put(sendInfo.getTopicAndFilter().toString(), queue2offsets);
//								}
//								sendInfoMap.remove(entry.getKey());
//								/*
//								 * 死信队列暂时先简单清空....................................
//								 */
//								deadMessageQueue.clear();
//							}else
							{
								reSendQueue.add(sendInfo.getMsgInfo());//加入重投队列
//								logger.error(sendInfo.getMsgInfo().getMsg()+"加入重投队列");
								recoverPool.execute(new Runnable(){
									
									@Override
									public void run() {
										ArrayList<OffsetNum> offsets=null;
										// TODO Auto-generated method stub
										for(int i=0;i<Config.maxqueue;i++){
											if(offsets==null){
												offsets=arrayStore.recover(group.getTopicAndFilter().toString(), String.valueOf(i));
											}else offsets.addAll(arrayStore.recover(group.getTopicAndFilter().toString(), String.valueOf(i)));
										}
										logger.error("内存充足,恢复重投"+offsets);
									}
									
								});
							}
						}else{//三次重投失败
							logger.error("消息"+sendInfo.getMsgInfo().getMsg().getMsgId()+" 给集群"+group+" "+sendInfo.getReSendCount()+" 次重投失败");
							sendInfoMap.remove(sendInfo.getMsgInfo().getMsgInfoId());
							deadMessageQueue.add(sendInfo.getMsgInfo());//加入死信队列
						}
					}else{
						
					}
				}
				logger.error("存储重投的offsets");
				for(Map.Entry<String, Map<String,ArrayList<OffsetNum>>> queue2offsets:topicAndFilter2queues.entrySet()){
					for(Map.Entry<String,ArrayList<OffsetNum>> offsets:queue2offsets.getValue().entrySet()){
						arrayStore.store(offsets.getValue(), queue2offsets.getKey(), offsets.getKey());
					}
				}
				
				
			}
		},timeoutLimit+100,1000);
	}
	
	/**
	 * 重投
	 * @param reSendQueue
	 * @param consumers
	 * @param offset
	 */
	public void startReSend(){
		//第一次开启重投时创建线程池
		reSendPool = Executors.newFixedThreadPool(reSendPoolSize/*Runtime.getRuntime().availableProcessors()+2*/);
		for(int i=0;i<reSendPoolSize;i++){
			reSendPool.execute(new Runnable() {
				@Override
				public void run() {
					MessageInfo msgInfo=null;
					while(true){
//						System.out.println("消费失败条数："+consumeFailCount.get());
//						System.out.println("开始重投，重投队列长度："+reSendQueue.size());
						try {
							msgInfo=reSendQueue.take();
						} catch (InterruptedException e) {
							Thread.interrupted();
						}
						if(msgInfo==null) continue;
							//重投给消费者
							group.consumersBalance().writeAndFlush(msgInfo.getMsg());
							//sendResult 重置
							group.getSendInfoMap().get(msgInfo.getMsgInfoId()).reSend();
							
					}//end while
				}
			});
		}
	}
}
