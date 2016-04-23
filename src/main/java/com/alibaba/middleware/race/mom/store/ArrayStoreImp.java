package com.alibaba.middleware.race.mom.store;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class ArrayStoreImp implements ArrayStore {
	private static String filePath = StoreConfig.baseDir + StoreConfig.arrayDir;
	private static Logger logger = LoggerFactory.getLogger(ArrayStoreImp.class);
	private static ConcurrentHashMap<String, FileStoreManager> fileStoreManagerMap = new ConcurrentHashMap<String, FileStoreManager>();

	@Override
	public boolean store(ArrayList<OffsetNum> arrayList, String topicAndFilter,
			String queueId) {
		// TODO Auto-generated method stub
		// 对应的文件管理器
				String arrayFilePath = filePath + "/" + topicAndFilter+"/"+queueId;
				FileStoreManager fileStoreManager = fileStoreManagerMap
						.get(arrayFilePath);
				if (fileStoreManager == null) {
					fileStoreManager = new FileStoreManager(arrayFilePath);
					// 为恢复
					fileStoreManagerMap.put(arrayFilePath, fileStoreManager);
				}
				// 将写位置重置为16
				fileStoreManager.resetFilePosition();

				ArrayList<byte[]> dataList = new ArrayList<byte[]>(arrayList.size());
				int count = 1;
				for (OffsetNum tmp : arrayList) {
					logger.debug("保存订阅关系 i+== " + (count++) + " , " + tmp.i);
					dataList.add(JSON.toJSONBytes(tmp));
				}

				return fileStoreManager.saveMessage(dataList);
	}

	@Override
	public ArrayList<OffsetNum> recover(String topicAndFilter, String queueId) {
		String arrayFilePath = filePath + "/" + topicAndFilter+"/"+queueId;
		FileStoreManager fileStoreManager = fileStoreManagerMap
				.get(arrayFilePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(arrayFilePath);
			// 为恢复
			fileStoreManagerMap.put(arrayFilePath, fileStoreManager);
		}
		return (ArrayList<OffsetNum>) fileStoreManager.getOffsetNum();
	}

}
