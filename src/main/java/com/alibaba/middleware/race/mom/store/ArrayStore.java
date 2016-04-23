package com.alibaba.middleware.race.mom.store;

import java.util.ArrayList;



/**
 * 数组存储与恢复
 * @param <OffsetNum>
 *
 */
public interface ArrayStore {
	/**
	 * 数组存储
	 * 每次调用以更新的方式（不能追加）
	 * @param offsets 需要存储的OffsetNum [] （一般用Integer[]），存储OffsetNum [] 中所有条目
	 * @param topicAndFilter 这几个参数用于文件夹结构，和消息存储一样（去掉了consumerid项）
	 * @param groupId
	 * @param queueId
	 */
	public boolean store(ArrayList<OffsetNum> arrayList,String topicAndFilter, String queueId);
	/**
	 * 数组恢复
	 * @param topicAndFilter  这几个参数用于文件夹结构，和消息存储一样（去掉了consumerid项）
	 * @param groupId
	 * @param queueId
	 * @return  OffsetNum []，读出所有条目，并装到 OffsetNum []（一般用Integer[]）返回
	 */
	public ArrayList<OffsetNum>  recover(String topicAndFilter,  String queueId);
	
	
}
