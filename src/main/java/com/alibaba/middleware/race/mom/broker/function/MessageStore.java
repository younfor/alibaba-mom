package com.alibaba.middleware.race.mom.broker.function;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.mom.config.Config;
import com.alibaba.middleware.race.mom.store.RedoLog;
import com.alibaba.middleware.race.mom.util.TopicAndFilter;

/**
 * 每个生产者集群(组)有一个MessageStore,用于存储此集群(组)(对应一种TopicAndFilter)的消息,也即每个生产者集群(组)一个storeQueues
 * @author youngforever
 *
 */

public class MessageStore {
	private static Logger logger = LoggerFactory.getLogger(MessageStore.class);
	
	/**
	 * 注意这里要和sendqueuenum 一致,全局统一配置
	 */
	private int storeQueueNum=Config.maxqueue;
	/**
	 *支持分布式队列(分布式存储任务),默认一个存储队列
	 */
//	private LinkedList<BlockingQueue<MessageInfo /*msg*/>> storeQueues = new LinkedList<>();
	private LinkedList<BlockingQueue<MessageSend /*msg*/>> storeGroupQueues = new LinkedList<>();
	private int storeThreadPoolSize = storeQueueNum;
	private AtomicInteger[] offsetMakers=new AtomicInteger[storeQueueNum];
	private ExecutorService storePool = Executors.newFixedThreadPool(storeThreadPoolSize);
//	private ExecutorService flushPool = Executors.newSingleThreadExecutor();
	private ExecutorService flushPool = Executors.newFixedThreadPool(storeQueueNum);
//	private MsgStore mstore = /* new MsgStoreImp();// */new MsgStoreImp_MappedBuffer2();
//	private FSTConfiguration fst = FSTConfiguration.getDefaultConfiguration();
	private TopicAndFilter topicAndfilter;
//	private Store store=new Store();
//	Redo redo=new Redo();
//	redotest redo=new redotest();
	RedoLog storer=new RedoLog();
//	CountDownLatch count=new CountDownLatch(1);
	volatile boolean  cando=false;
	private boolean messageGroup=false;
//	private AtomicInteger recvCount=new AtomicInteger();
	
	/**
	 * 用于恢复时,为了衔接故障前的状态
	 * @param topicAndfilter
	 * @param queueMaxOffsets
	 */
	public MessageStore(TopicAndFilter topicAndfilter,Map<Integer /*queueIndex*/,Integer/*maxOffset*/> queueMaxOffsets) {
		this.topicAndfilter = topicAndfilter;
		/*
		 * 创建时初始化队列
		 */
//		for(Map.Entry<Integer,Integer> queueMaxOffset:queueMaxOffsets.entrySet()){
//			this.storeQueues.add(new LinkedBlockingQueue<MessageInfo>());
//			offsetMakers[queueMaxOffset.getKey()]=new AtomicInteger(queueMaxOffset.getValue());
//		}
		for(Map.Entry<Integer,Integer> queueMaxOffset:queueMaxOffsets.entrySet()){
			this.storeGroupQueues.add(new LinkedBlockingQueue<MessageSend>());
			offsetMakers[queueMaxOffset.getKey()]=new AtomicInteger(queueMaxOffset.getValue());
		}
//		startStore();
		startStoreGroup();
	}
	/**
	 * 用于新增
	 * @param topicAndfilter
	 */
	public MessageStore(TopicAndFilter topicAndfilter) {
		this.topicAndfilter = topicAndfilter;
		/*
		 * 创建时初始化队列
		 */
//		for(int i=0;i<storeQueueNum;i++){
//			this.storeQueues.add(new LinkedBlockingQueue<MessageInfo>());
//			offsetMakers[i]=new AtomicInteger();
//		}
		for(int i=0;i<storeQueueNum;i++){
			this.storeGroupQueues.add(new LinkedBlockingQueue<MessageSend>());
			offsetMakers[i]=new AtomicInteger();
		}
//		startStore();
		/*
		 * 采用组发，则不必盯着队列
		 */
//		startStoreGroup();
	}
	/**
	 * 存储队列均衡
	 * @return
	 */
//	private int  storeQueueBalancer=-1;
//	public synchronized BlockingQueue<MessageInfo> storeQueuesBalance(){
//		 storeQueueBalancer++;
//		if(storeQueueBalancer==storeQueueNum){
//			storeQueueBalancer=0;
//		}
//		BlockingQueue<MessageInfo> storeQueue=storeQueues.get(storeQueueBalancer);
//		return storeQueue;
//	}
	private int  storeGroupQueueBalancer=-1;
	public synchronized BlockingQueue<MessageSend> storeQueuesBalance(boolean isGroup){
		storeGroupQueueBalancer++;
		if(storeGroupQueueBalancer==storeQueueNum){
			storeGroupQueueBalancer=0;
		}
		BlockingQueue<MessageSend> storeGroupQueue=storeGroupQueues.get(storeGroupQueueBalancer);
		return storeGroupQueue;
	}
	/**
	 * 加入均衡后的存储队列
	 * @param msg
	 * @return queueId+" "+offset
	 */
//	public  String storeMsg(MessageInfo msgInfo){
////		System.out.println(recvCount.incrementAndGet());
//		
//		BlockingQueue<MessageInfo> storeQueue=storeQueuesBalance();
//		int queueId=storeQueues.indexOf(storeQueue);
//		int offset;
//		synchronized(this){//同步块中进行,保证offset的准确性(存储队列中加入和offet产生的原子性)
////			count.countDown();
//			storeQueue.offer(msgInfo);
//			offset=offsetMakers[queueId].getAndIncrement();
//		}
//		return String.valueOf(queueId)+" "+String.valueOf(offset);
//	}
	/**
	 * 加入均衡后的组存 存储队列
	 * @param msgSend
	 * @return
	 */
	public  String storeMsg(MessageSend msgSend){
		BlockingQueue<MessageSend> storeGroupQueue=storeQueuesBalance(true);
		int queueId=storeGroupQueues.indexOf(storeGroupQueue);
		int offset;
		synchronized(this){//同步块中进行,保证offset的准确性(存储队列中加入和offet产生的原子性)
//			count.countDown();
			//offset从0开始，所以返回的就是当前可用的起始offset
			offset=offsetMakers[queueId].getAndAdd(msgSend.getBodys().size());
			storeGroupQueue.offer(msgSend);
		}
		startStoreGroup();
		return String.valueOf(queueId)+" "+String.valueOf(offset);
	}
//	 public  void storeMsg(MessageInfoQueue msgInfoQueue){
////			System.out.println(recvCount.incrementAndGet());
//			
//			BlockingQueue<MessageInfo> storeQueue=storeQueuesBalance();
////			int queueId=storeQueues.indexOf(storeQueue);
////			int offset;
////				count.countDown();
//				storeQueue.addAll(msgInfoQueue.getMsgInfoqueue());
////				offset=offsetMakers[queueId].getAndIncrement();
////			}
////			return String.valueOf(queueId)+" "+String.valueOf(offset);
//		}
	/**
	 * 一次存储过程
	 */
	public void storeTask(final int queueIndex,ArrayList<byte[]> msgbytes,final StoreSucceedListener storeSucceedListener,final int offset){
		
		flushPool.execute(new Runnable(){
//			
//			@Override
			public void run() {
				cando=false;
				final long start=System.currentTimeMillis();
//				store.flush();
				storer.flushLog();
//				redo.flushLog();
//				try {
//					Thread.sleep(15);
//				} catch (InterruptedException e) {
//				}
//				logger.error("一次flush cost:"+(System.currentTimeMillis()-start));
//				mstore.writeByteNormal( topicAndfilter.toString(), String.valueOf(queueIndex), msgbytes);
				cando=true;
				storeSucceedListener.onStoreSucceed(topicAndfilter,queueIndex,offset);
			}
			
		});

		
	}
	
	/**
	 * 组存任务开始
	 */
	public void startStoreGroup(){
		
		for(final BlockingQueue<MessageSend> storeGroupQueue:storeGroupQueues){
			for(int i=0;i<storeThreadPoolSize/storeQueueNum;i++){//每个队列提交几次任务,占满线程池
				storePool.execute(new Runnable(){
					@Override
					public void run() {
						long start=System.currentTimeMillis();
						AtomicInteger storeCount=new AtomicInteger();
							MessageSend msgSend=null;
							ArrayList<byte[]> msgbytes=new ArrayList<>();
							StoreSucceedListener storeSucceedListener=new StoreSucceedListener();
							/**
							 * 准备一次存储过程,构建对应存储成功监听器
							 */
//							int i=100;
//							try {
//								count.await(20, TimeUnit.MILLISECONDS);
//							} catch (InterruptedException e) {
//								count=new CountDownLatch(100);
//							}
//							boolean flag=false;
//							while(!storeQueue.isEmpty()&&i-->0){
							//队列对应本次存储成功的条数
							int offset=0;
						
							while(!storeGroupQueue.isEmpty()){
								
								msgSend=storeGroupQueue.poll();
								if(msgSend==null) break;
								
								storer.writeLog(msgSend.getTopic(), msgSend.getProperties().toString(),storeGroupQueues.indexOf(storeGroupQueue) , msgSend.getBodys());
								offset=msgSend.getBodys().size();
//								redo.writeLog(topic/*String*/, filter/*String*/,queueindex/*Integer*/,bodys/*(200个byte[])/*ArrayList<byte[]>*/);
								
//								for(byte[] body:msgSend.getBodys()){
////									redo.writeLog2(body);
//									offset++;
//									storeCount.incrementAndGet();
//								}
								
//								i++;
							
//								storeSucceedListener.addAckWaiter(msgSend.getProducer(), msgSend.getMsg().getMsgId());
//								storeSucceedListener.addAckWaiterGroup(msgSend.getProducer(), msgSend.getMsg().getMsgId());
//								storeSucceedListener.addAckWaiterNum(msgSend.getProducer());
//								if(cando&&storeCount.get()!=0) break;
//								flag=true;
//								
								storeSucceedListener.addAckGroupWaiter(msgSend.getProducer(),msgSend.getSendIds());
							}
//							if(flag)
							if(offset!=0){
//								logger.error((System.currentTimeMillis()-start)+" ms后提交新落盘任务,一次flush"+offset+"条");
//								logger.error("一共flush"+storeCount.get()*offset+"条");
								storeTask(storeGroupQueues.indexOf(storeGroupQueue),msgbytes,storeSucceedListener,offset);
								storeCount.set(0);
//								start =System.currentTimeMillis();
//							}else{
//								//0就不提交了，大量废任务进入线程池。。。
//							}
							}
					}
				});
			}
		}
	}
	
	
//	
//	 /**
//	  * 开始存储任务
//	  */
//	public void startStore(){
//	
//		for(final BlockingQueue<MessageInfo> storeQueue:storeQueues){
//			for(int i=0;i<storeThreadPoolSize/storeQueueNum;i++){//每个队列提交几次任务,占满线程池
//				storePool.execute(new Runnable(){
//					@Override
//					public void run() {
//						int count=0;
//						long start=System.currentTimeMillis();
//						AtomicInteger storeCount=new AtomicInteger();
//						while(true){		
//							MessageInfo msgInfo=null;
//							ArrayList<byte[]> msgbytes=new ArrayList<>();
//							StoreSucceedListener storeSucceedListener=new StoreSucceedListener();
//							/**
//							 * 准备一次存储过程,构建对应存储成功监听器
//							 */
////							int i=100;
////							try {
////								count.await(20, TimeUnit.MILLISECONDS);
////							} catch (InterruptedException e) {
////								count=new CountDownLatch(100);
////							}
////							boolean flag=false;
////							while(!storeQueue.isEmpty()&&i-->0){
//							
//							int offset=0;
//						
//							while(true){
////								if(storeCount.incrementAndGet()>98){
////									System.out.println("一百条等待"+(System.currentTimeMillis()-start));
////									break;
////								};
//								
//								try {
//									if(!messageGroup){
//										msgInfo=storeQueue.poll(500, TimeUnit.MILLISECONDS);
//										if(storeCount.get()>1){
//											messageGroup=true;
//										}
//									}else msgInfo=storeQueue.poll();
//									
//								} catch (InterruptedException e) {
//									// TODO Auto-generated catch block
//									e.printStackTrace();
//								}
//								if(msgInfo==null&&!messageGroup&&storeCount.get()==1)break;
//								if(msgInfo==null&&storeCount.get()==0) continue;
//								if(msgInfo==null) continue;
//								offset=msgInfo.getOffset();
//								redo.writeLog(offset+"", msgInfo.getMsg().getTopic(),msgInfo.getMsg().getBody());
////								i++;
//							
////								storeSucceedListener.addAckWaiter(msgInfo.getProducer(), msgInfo.getMsg().getMsgId());
////								storeSucceedListener.addAckWaiterGroup(msgInfo.getProducer(), msgInfo.getMsg().getMsgId());
////								storeSucceedListener.addAckWaiterNum(msgInfo.getProducer());
////								if(cando&&storeCount.get()!=0) break;
////								flag=true;
//								if(storeCount.incrementAndGet()>199){
//									messageGroup=true;
//									break;
//								}
////								
//							}
////							if(flag)
//							if(storeCount.get()==0) continue;
//								count++;
//								messageGroup=true;
////								logger.error((System.currentTimeMillis()-start)+" ms后提交新落盘任务,一次flush"+storeCount.get()+"条");
//								logger.error("一共flush"+storeCount.get()*count+"条");
//								storeTask(storeQueues.indexOf(storeQueue),msgbytes,storeSucceedListener,offset);
//								storeCount.set(0);
//								start =System.currentTimeMillis();
////							}else{
////								//0就不提交了，大量废任务进入线程池。。。
////							}
//						}
//					}
//				});
//			}
//		}
//	}
	
}
