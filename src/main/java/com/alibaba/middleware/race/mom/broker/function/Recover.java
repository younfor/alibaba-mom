package com.alibaba.middleware.race.mom.broker.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.nustaq.serialization.FSTConfiguration;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.broker.group.ProducerGroup;
import com.alibaba.middleware.race.mom.store.MsgStore;
import com.alibaba.middleware.race.mom.store.MsgStoreImp_MappedBuffer2;
import com.alibaba.middleware.race.mom.store.SubscribeStore;
import com.alibaba.middleware.race.mom.store.SubscribeStoreImp;
import com.alibaba.middleware.race.mom.util.TopicAndFilter;

/**
 * broker 故障重启恢复
 * @author youngforever
 *
 */
public class Recover {
	private FSTConfiguration fst = FSTConfiguration.getDefaultConfiguration();
	private SubscribeStore substore=new SubscribeStoreImp();
	private MsgStore mstore = /* new MsgStoreImp();// */new MsgStoreImp_MappedBuffer2();
	private int maxOffset;
	

	public  ConcurrentHashMap<String,Message>  recover(){
		return recoverMessage(makeOffsets(recoveroffsets()));
	}
	
	public ArrayList<Offset>  recoveroffsets(){
		ArrayList<Offset> offsets=substore.read();
		System.out.println(offsets);
		return offsets;
	}
	/**
	 * 集群组 进度重叠交叉等,对其取并集,避免从磁盘重复读取消息
	 * 遍历所有恢复的Offset,做其它恢复的准备和恢复消息去重
	 * @param offsets
	 * @return
	 */
	public List<Offset> makeOffsets(ArrayList<Offset>offsets ){
		HashMap<TopicAndFilter,Offset>makedOffsets=new HashMap<>();
		HashMap<TopicAndFilter/*TopicAndFilter*/,Map<Integer /*queueIndex*/,Integer/*maxOffset*/>>producerGroupRecoverInfos=new HashMap<>();
		Offset makedoffset;
		TopicAndFilter topicAndFilter;
		for(Offset offset:offsets){
			topicAndFilter=new TopicAndFilter(offset.getTopic(),offset.getFilter());
			
			System.out.println(offset);
			/*
			 * 恢复存储进度关系
			 */
			if(producerGroupRecoverInfos.containsKey(topicAndFilter)){
				producerGroupRecoverInfos.get(topicAndFilter).put(offset.getQueueIndex(),offset.getMaxOffset());
			}else{
				Map<Integer /*queueIndex*/,Integer/*maxOffset*/> maxoffsets=new HashMap<>();
				maxoffsets.put(offset.getQueueIndex(), offset.getMaxOffset());
				producerGroupRecoverInfos.put(topicAndFilter, maxoffsets);
			}
			
			if(makedOffsets.containsKey(topicAndFilter)){
				makedoffset=makedOffsets.get(topicAndFilter);
				if(offset.getCurrentoffset()<makedoffset.getCurrentoffset()){
					makedoffset.setCurrentoffset(offset.getCurrentoffset());
				}
				if(offset.getMaxOffset()>makedoffset.getMaxOffset()){
					makedoffset.setMaxOffset(offset.getMaxOffset());
				}
			}else  {
				makedOffsets.put(topicAndFilter,offset);
			}
		}
		ProducerGroup.recover(producerGroupRecoverInfos);
		
		return  Arrays.asList(makedOffsets.values().toArray(new Offset[0]));
	}
	
	public  ConcurrentHashMap<String,Message>  recoverMessage(List<Offset> makedOffsets){
		long startTime = System.currentTimeMillis();
		ConcurrentHashMap<String,Message> msgMap=new ConcurrentHashMap<>(1024*128);
		System.out.println("offsets:"+makedOffsets);
		int recoverNum=0;
		int queueId;
		TopicAndFilter topicAndFilter;
		for(Offset offset:makedOffsets){
			topicAndFilter=new TopicAndFilter(offset.getTopic(),offset.getFilter());
			queueId=offset.getQueueIndex();
//			//恢复topicAndgroup2subScribe
//			LinkedList<Offset> offsetList;
//			if(!topicAndgroup2subScribe.containsKey(topic+"@"+group)){
//				offsetList=new LinkedList<Offset>();
//				topicAndgroup2subScribe.put(topic+"@"+group, offsetList);
//			}else{
//				offsetList=topicAndgroup2subScribe.get(topic+"@"+group);
//			}
//			offsetList.add(Integer.parseInt(queueId),offset);
//			//恢复topicAndgroup2sendMsgQueue
//			LinkedList<BlockingQueue<String>> sendQueuesList;
//			if(!topicAndgroup2sendMsgQueue.containsKey(topic + "@" + group)){
//				sendQueuesList=new LinkedList<BlockingQueue<String>>();
//				topicAndgroup2sendMsgQueue.put(topic + "@" + group, sendQueuesList);
//			}else
//			{
//				sendQueuesList=topicAndgroup2sendMsgQueue.get(topic + "@" + group);
//			}
//			BlockingQueue<String>sendQueue=new LinkedBlockingQueue<String>();
//			sendQueuesList.add(Integer.parseInt(queueId),sendQueue);
			//消息入内存
			int currentOffset=offset.getCurrentoffset();
			int MaxOffset=offset.getMaxOffset();
			Message msg;
			int i=0;//currentOffset偏移量
			List<byte[]> blist = mstore.readByteNormal(topicAndFilter.toString(),String.valueOf(queueId),currentOffset+1, MaxOffset);
			recoverNum+=blist.size();
			System.out.println(blist.size());
			for(byte[] b:blist){
				try{
					msg=(Message)fst.asObject(b);
					System.out.println("恢复消息"+msg);
				}catch(Exception e){
					continue;
				}
				msgMap.put(new TopicAndFilter(msg.getTopic(),msg.getProperties()).toString(), msg);
//				msgid2offset.put(msg.getMsgId(), currentOffset+i);
				i++;
			}
			
		}
		long endTime = System.currentTimeMillis();
		System.out.println("恢复cost：" + (endTime - startTime)+" ,一共恢复"+recoverNum+"条");	
		
		return msgMap;
	}
	public int getMaxOffset() {
		return maxOffset;
	}

}
