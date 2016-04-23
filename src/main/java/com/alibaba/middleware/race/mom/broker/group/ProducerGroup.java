package com.alibaba.middleware.race.mom.broker.group;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.mom.broker.function.MessageInfo;
import com.alibaba.middleware.race.mom.broker.function.MessageInfoQueue;
import com.alibaba.middleware.race.mom.broker.function.MessageSend;
import com.alibaba.middleware.race.mom.broker.function.MessageStore;
import com.alibaba.middleware.race.mom.util.TopicAndFilter;


/**
 * 生产者集群(组)管理
 * 假设存在多个集群生产同一种topicAndfilter的消息,broker端可将集群组视为同一集群,不做集群区分,只以topicAndfilter区分
 * 消息存储以topicAndfilter将生产者消息归类存储
 * ack返回,我们假定必须返回相应生产者(消息的直接生产者),而不是集群中任一生产者
 * 
 * @author youngforever
 */
public class ProducerGroup {
		private static Logger logger = LoggerFactory.getLogger(ProducerGroup.class);
		/**
		 * 每个生产者集群
		 */
//		private Map<String,/*producerId*/Channel/*producer*/>producers = new HashMap<>();
//		private String groupId;
//		private String produceTopic;
//		private Map<String,String> produceFilter;
		private TopicAndFilter produceTopicAndFilter; 
		private MessageStore msgStore ;
		
		/**
		 * 私有构造函数,仅用于恢复时
		 * @param produceTopic
		 * @param produceFilter
		 * @param queueMaxOffsets
		 */
		private ProducerGroup(TopicAndFilter topicAndFilter,Map<Integer /*queueIndex*/,Integer/*maxOffset*/> queueMaxOffsets){
			this.produceTopicAndFilter=topicAndFilter;
			this.msgStore=new MessageStore(produceTopicAndFilter, queueMaxOffsets);
			
		}
		/**
		 * 用于新增生产者集群
		 * @param produceTopic
		 * @param produceFilter
		 * @param queueMaxOffsets
		 */
		public ProducerGroup(String produceTopic,Map<String,String> produceFilter){
//			this.produceTopic=produceTopic;
//			this.produceFilter=produceFilter;
			this.produceTopicAndFilter=new TopicAndFilter(produceTopic,produceFilter);
			this.msgStore=new MessageStore(produceTopicAndFilter);
		}
//		public void addProducer(Channel producer,String producerId){
//			if(!producers.containsKey(producerId)){
//				producers.put(producerId,producer);
//			}else if(!producers.get(producerId).isOpen()){//已经存在此id 的producer,但是关闭了连接. 现在应该时发起了重连
//				producers.put(producerId,producer);
//			}
//		}
		public String toString(){
			return produceTopicAndFilter.toString();
		}
	/**
	 * 非静态部分 为某一具体集群
	 * 静态部分 属于集群组,管理所有生产者集群
	 */
		public static  Map<String/* topicAndfilter */, ProducerGroup/* group */> topicAndFilter2group = new ConcurrentHashMap<>();
		/**
		 * 添加 topicAndFilter的订阅集群
		 * @param topicAndFilter
		 * @param groupId
		 */
		public static ProducerGroup addGroup(TopicAndFilter topicAndFilter){
			ProducerGroup group=topicAndFilter2group.get(topicAndFilter.toString());//集群组
			if(group==null){
				group=new ProducerGroup(topicAndFilter.getTopic(),topicAndFilter.getFilter());
				topicAndFilter2group.put(topicAndFilter.toString(),group);
			}
			return group;
		}
		/**
		 * 仅用于恢复时
		 * @param topicAndFilter
		 * @param queueMaxOffsets
		 * @return
		 */
		private static ProducerGroup addGroup(TopicAndFilter topicAndFilter,Map<Integer /*queueIndex*/,Integer/*maxOffset*/> queueMaxOffsets){
			ProducerGroup group=topicAndFilter2group.get(topicAndFilter.toString());//集群组
			if(group==null){
				group=new ProducerGroup(topicAndFilter,queueMaxOffsets);
				topicAndFilter2group.put(topicAndFilter.toString(),group);
			}
			return group;
		}
		public static ProducerGroup getGroup(TopicAndFilter topicAndFilter){
			return topicAndFilter2group.get(topicAndFilter.toString());
		}
		/**
		 * 注册生产者
		 * @param producer
		 * @param groupId
		 * @param topicAndFilter
		 */
		/*
		 * 采用了 存储成功监听器机制,那么生产者实际上不用保存
		 */
//		public static void addProducer(Channel producer ,String producerId,String producerGroupId,TopicAndFilter topicAndFilter){
//			ProducerGroup group=getGroup(topicAndFilter);
//			if(group!=null){
//				group.addProducer(producer,producerId);
//			}else addGroup(topicAndFilter).addProducer(producer,producerId);
////			System.out.println(topicAndFilter2groups);
//		}
		/**
		 * 
		 * @param topicAndFilter
		 * @param msgInfo
		 * @return offset,消息进入持久化队列后,返回下标做为offset(代表存储顺序,对应文件消息索引,同时用于表示消费进度)
		 */
//		public static String storeMsg(TopicAndFilter topicAndFilter,MessageInfo msgInfo){
//			ProducerGroup producerGroup = getGroup(topicAndFilter);
//			if(producerGroup==null){
//				logger.error("生产者集群(组)尚未注册");
//				producerGroup=addGroup(topicAndFilter);
//			}
//			return producerGroup.msgStore.storeMsg(msgInfo);
//		}	
		/**
		 * 组存处理
		 * @param topicAndFilter
		 * @param msgInfo
		 * @return offset（返回为maxOffset）,消息进入持久化队列后,返回下标做为offset(代表存储顺序,对应文件消息索引,同时用于表示消费进度)
		 */
		public static String storeMsg(TopicAndFilter topicAndFilter,MessageSend msgSend){
			ProducerGroup producerGroup = getGroup(topicAndFilter);
			if(producerGroup==null){
				logger.error("生产者集群(组)尚未注册");
				producerGroup=addGroup(topicAndFilter);
			}
			return producerGroup.msgStore.storeMsg(msgSend);
		}
		/**
		 * 恢复
		 */
		public static void recover(HashMap<TopicAndFilter/*TopicAndFilter*/,Map<Integer /*queueIndex*/,Integer/*maxOffset*/>>producerGroupRecoverInfos){
			for(Map.Entry<TopicAndFilter,Map<Integer /*queueIndex*/,Integer/*maxOffset*/>>queueMaxOffsets :producerGroupRecoverInfos.entrySet()){
				addGroup(queueMaxOffsets.getKey(),queueMaxOffsets.getValue());
				System.out.println(addGroup(queueMaxOffsets.getKey(),queueMaxOffsets.getValue()));
			}
		}
		
/**
 * 集群组.区分集群(先勿删!!!)		
 */
		
//		/**
//		 * 订阅集群组,每种topicAndfilter可能有一个或多个订阅集群
//		 */
//		public static  Map<String/* topicAndfilter */, ConcurrentHashMap<String/*groupid*/,ProducerGroup/* group */>> topicAndFilter2groups = new ConcurrentHashMap<>();
//		/**
//		 * 添加 topicAndFilter的订阅集群
//		 * @param topicAndFilter
//		 * @param groupId
//		 */
//		public static ProducerGroup addGroup(TopicAndFilter topicAndFilter,String groupId){
//			Map<String,ProducerGroup> groups=topicAndFilter2groups.get(topicAndFilter.toString());//集群组
//			ProducerGroup group=new ProducerGroup(topicAndFilter.getTopic(),topicAndFilter.getFilter(),groupId);
//			if(groups==null){
//				groups=new ConcurrentHashMap<String,ProducerGroup>();
//				groups.put(groupId,group);
//				topicAndFilter2groups.put(topicAndFilter.toString(), (ConcurrentHashMap<String, ProducerGroup>) groups);
//			}else if(!groups.containsKey(groupId)){
//					groups.put(groupId, group);
//			}
//			return group;
//		}
//		public static  ConcurrentHashMap<String,ProducerGroup> getGroups(TopicAndFilter topicAndFilter){
//			return topicAndFilter2groups.get(topicAndFilter.toString());
//		}
//		public static ProducerGroup getGroup(TopicAndFilter topicAndFilter,String groupId){
//			Map<String,ProducerGroup> groups=getGroups(topicAndFilter);
//			if(groups!=null){
//				return groups.get(groupId);
//			}else return null;
//		}
//		/**
//		 * 注册生产者
//		 * @param producer
//		 * @param groupId
//		 * @param topicAndFilter
//		 */
//		public static void addProducer(Channel producer ,String producerId,String producerGroupId,TopicAndFilter topicAndFilter){
//			ProducerGroup group=getGroup(topicAndFilter,producerGroupId);
//			if(group!=null){
//				group.addProducer(producer,producerId);
//			}else addGroup(topicAndFilter,producerGroupId).addProducer(producer,producerId);
//			System.out.println(topicAndFilter2groups);
//		}
//		public static void storeMsg(TopicAndFilter topicAndFilter,MessageInfo msgInfo){
//			ProducerGroup producerGroup = getGroup(topicAndFilter,msgInfo.getProducerGroupId());
//			if(producerGroup!=null){
//				producerGroup.msgStoreHanler.storeCache.
//			}else{
//				logger.error("生产者集群尚未注册");
//			}
//		}
//		/**
//		 * 以groupId区分俩个集群
//		 */
//		@Override
//		public boolean equals(Object obj) {
//			if(obj instanceof ProducerGroup){
//				if(((ProducerGroup) obj).getGroupId().equals(this.groupId))return true;
//				else return false;
//			}else return false;
//		}
		

}
