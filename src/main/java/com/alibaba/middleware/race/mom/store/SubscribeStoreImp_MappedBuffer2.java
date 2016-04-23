package com.alibaba.middleware.race.mom.store;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.middleware.race.mom.broker.function.Offset;

public class SubscribeStoreImp_MappedBuffer2 implements SubscribeStore {
	private static Logger logger = LoggerFactory
			.getLogger(SubscribeStoreImp_MappedBuffer2.class);
	private static String subscribeFilePath = StoreConfig.baseDir
			+ StoreConfig.subscribeDir;
	private static ConcurrentHashMap<String, FileStoreManager> fileStoreManagerMap = new ConcurrentHashMap<String, FileStoreManager>();

	@Override
	public ArrayList<Offset> read() {
		FileStoreManager fileStoreManager = fileStoreManagerMap
				.get(subscribeFilePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(subscribeFilePath);
			// 为恢复
			fileStoreManagerMap.put(subscribeFilePath, fileStoreManager);
		}
		return (ArrayList<Offset>) fileStoreManager.getSubscribe();

	}

	@Override
	public boolean write(ArrayList<Offset> subInfoList) {
		// 对应的文件管理器
		FileStoreManager fileStoreManager = fileStoreManagerMap
				.get(subscribeFilePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(subscribeFilePath);
			// 为恢复
			fileStoreManagerMap.put(subscribeFilePath, fileStoreManager);
		}
		// 将写位置重置为16
		fileStoreManager.resetFilePosition();

		ArrayList<byte[]> dataList = new ArrayList<byte[]>(subInfoList.size());
		int count = 1;
		for (Offset tmp : subInfoList) {
			logger.debug("保存订阅关系 i+== " + (count++) + " , " + tmp.getTopic());

			dataList.add(JSON.toJSONBytes(tmp));
		}

		//return  fileStoreManager.saveMessage(dataList);
		return  fileStoreManager.saveMessageByOnce(dataList);

	}

	/*
	 * private void deleteOldSubscribeFile() { deleteFile(subscribeFilePath +
	 * "/index/0", true); deleteFile(subscribeFilePath + "/data/0", true); }
	 * 
	 * public void deleteFile(String file, boolean isWriteOverride) { File f =
	 * new File(file); if (isWriteOverride) { try { if (f.exists()) { // 删除老文件
	 * f.delete(); } } catch (Exception e) { // TODO Auto-generated catch block
	 * logger.error(e.getMessage()); } } }
	 */
}
