package com.alibaba.middleware.race.mom.store;

import java.util.ArrayList;
import java.util.List;

/*
 * By  ChenQi
 * 消息持久化接口
 * */
public interface MsgStore {

	/*
	 * index baseDir/Normal/Topic/GroupId/QueueId/index/ 0,1,2 data
	 * baseDir/Normal/Topic/GroupId/QueueId/data/ 0,1,2
	 * 
	 * 
	 * baseDir/Retry/Group/index/0,1,2 baseDir/Retry/Group/data/0,1,2
	 */
	// 一些参数定义初始化
	public void init();

	// 关闭
	public void close();

	// 存储正常消息, 成功true,失败false
	public boolean writeByteNormal(String topicId, String queueId,
			List<byte[]> msgList);

	// 取消息每个消息的字节码存为list返回
	public List<byte[]> readByteNormal(String topicID, String queueId,
			int offset, int MaxOffset);

	// 存储重试消息, 成功true,失败false
	public boolean writeByteRetry(String groupId, List<byte[]> msgList);

	// 取重试消息每个消息的字节码存为list返回
	public ArrayList<byte[]> readByteRetry(String groupId, int offset,
			int MaxOffset);

	// 组写 为数据一组为单位，在index 与data 文件分开写
	boolean writeByteNormalByOnce(String topicId, String queueId,
			List<byte[]> msgList);

	// 组写 为数据一组为单位，在index 与data 文件分开写
	boolean writeByteRetryByOnce(String groupId, List<byte[]> msgList);

	// 组读 为数据一组为单位，在index 与data 文件分开读
	List<byte[]> readByteNormalByOnce(String topicId, String queueId,
			int offset, int MaxOffset);

	// 组读 为数据一组为单位，在index 与data 文件分开读
	List<byte[]> readByteRetryByOnce(String queueId, int offset, int MaxOffset);

	// 落盘-临时文件缓冲
	/**
	 * 我传你的文件，你按顺序加入你的落盘逻辑，文件里的格式( 消息大小(int)+消息实体(body))
	 * 你需要一条一条读，然后读完一个文件刷一次就行。刷完就删除哈。 记住，可能你还没读完的时候
	 * 我又调用这个api传你另外一个文件，所以你需要加入一个链表。 按顺序用完就删，不断的取。
	 * 
	 * @param filename
	 */
	public void writeMappedFileTemp(String filename);

	// test use
	public void testprintlong();

	public void loadAndRecover();
}
