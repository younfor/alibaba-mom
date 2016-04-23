package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 *     baseDir/Normal/Topic/Group/Consumer/Queue/(index.file,data.file)
 *     baseDir/Retry/Group/(index.file,data.file)
 *     题目没有consumerID就默认生成和groupID一样的
 * */

public class MsgStoreImp_MappedBuffer2 implements MsgStore {
	private static Logger logger = LoggerFactory
			.getLogger(MsgStoreImp_MappedBuffer2.class);
	private static ConcurrentHashMap<String, FileStoreManager> fileStoreManagerMap = new ConcurrentHashMap<String, FileStoreManager>();

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		fileStoreManagerMap.clear();
	}

	@Override
	public boolean writeByteNormal(String topicId, String queueId,
			List<byte[]> msgList) {
		// 存储文件路径
		String filePath = StoreConfig.baseDir + StoreConfig.normalDir + topicId
				+ "/" + queueId;
		// 对应的文件管理器
		FileStoreManager fileStoreManager = fileStoreManagerMap.get(filePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(filePath);
			fileStoreManagerMap.put(filePath, fileStoreManager);
			logger.debug("建立filestoremanager= " + filePath);
		}
		return fileStoreManager.saveMessage(msgList);

	}

	@Override
	public void testprintlong() {
		System.out.println("FileStoreManager.savemessage"
				+ FileStoreManager.savemessage);
		System.err.println("FileStoreManager.savemessagebyonce"
				+ FileStoreManager.savemessagebyonce);
	}

	@Override
	public boolean writeByteNormalByOnce(String topicId, String queueId,
			List<byte[]> msgList) {
		// 存储文件路径
		String filePath = StoreConfig.baseDir + StoreConfig.normalDir + topicId
				+ "/" + queueId;
		// 对应的文件管理器
		FileStoreManager fileStoreManager = fileStoreManagerMap.get(filePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(filePath);
			fileStoreManagerMap.put(filePath, fileStoreManager);
			logger.debug("建立filestoremanager= " + filePath);
		}
		return fileStoreManager.saveMessageByOnce(msgList);

	}

	@Override
	public boolean writeByteRetry(String groupId, List<byte[]> msgList) {
		// TODO Auto-generated method stub
		// 存储文件路径
		String filePath = StoreConfig.baseDir + StoreConfig.retryDir + groupId;
		// 对应的文件管理器
		FileStoreManager fileStoreManager = fileStoreManagerMap.get(filePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(filePath);
			fileStoreManagerMap.put(filePath, fileStoreManager);
		}
		return fileStoreManager.saveMessage(msgList);
	}

	@Override
	public boolean writeByteRetryByOnce(String groupId, List<byte[]> msgList) {
		// TODO Auto-generated method stub
		// 存储文件路径
		String filePath = StoreConfig.baseDir + StoreConfig.retryDir + groupId;
		// 对应的文件管理器
		FileStoreManager fileStoreManager = fileStoreManagerMap.get(filePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(filePath);
			fileStoreManagerMap.put(filePath, fileStoreManager);
		}
		return fileStoreManager.saveMessageByOnce(msgList);
	}

	@Override
	public ArrayList<byte[]> readByteRetry(String groupId, int offset,
			int MaxOffset) {
		// 存储文件路径
		String filePath = StoreConfig.baseDir + StoreConfig.retryDir + groupId;
		// 对应的文件管理器
		FileStoreManager fileStoreManager = fileStoreManagerMap.get(filePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(filePath);
			fileStoreManagerMap.put(filePath, fileStoreManager);
		}
		return (ArrayList) fileStoreManager.getMessage(offset, MaxOffset);

	}

	@Override
	public List<byte[]> readByteNormal(String topicId, String queueId,
			int offset, int MaxOffset) {
		// 存储文件路径
		String filePath = StoreConfig.baseDir + StoreConfig.normalDir + topicId
				+ "/" + queueId;
		// 对应的文件管理器
		FileStoreManager fileStoreManager = fileStoreManagerMap.get(filePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(filePath);
			fileStoreManagerMap.put(filePath, fileStoreManager);
			logger.debug("读数据建立filestoremanage");
		}
		logger.debug("读数据的起始地址为" + offset + "   " + MaxOffset);
		return (ArrayList) fileStoreManager.getMessage(offset, MaxOffset);
	}

	@Override
	public List<byte[]> readByteNormalByOnce(String topicId, String queueId,
			int offset, int MaxOffset) {
		// 存储文件路径
		String filePath = StoreConfig.baseDir + StoreConfig.normalDir + topicId
				+ "/" + queueId;
		// 对应的文件管理器
		FileStoreManager fileStoreManager = fileStoreManagerMap.get(filePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(filePath);
			fileStoreManagerMap.put(filePath, fileStoreManager);
			logger.debug("读数据建立filestoremanage");
		}
		logger.debug("读数据的起始地址为" + offset + "   " + MaxOffset);
		return (ArrayList) fileStoreManager.getMessageByOnce(offset, MaxOffset);
	}

	@Override
	public List<byte[]> readByteRetryByOnce(String queueId, int offset,
			int MaxOffset) {
		// 存储文件路径
		String filePath = StoreConfig.baseDir + StoreConfig.normalDir + "/"
				+ queueId;
		// 对应的文件管理器
		FileStoreManager fileStoreManager = fileStoreManagerMap.get(filePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(filePath);
			fileStoreManagerMap.put(filePath, fileStoreManager);
			logger.debug("读数据建立filestoremanage");
		}
		logger.debug("读数据的起始地址为" + offset + "   " + MaxOffset);
		return (ArrayList) fileStoreManager.getMessageByOnce(offset, MaxOffset);

	}

	@Override
	public void writeMappedFileTemp(String filename) {
		// TODO Auto-generated method stub

	}

	@Override
	public void loadAndRecover() {
		// 恢复正常消息队列
		String filePath = StoreConfig.baseDir + StoreConfig.normalDir;
		ArrayList<String[]> rsPath = this.scanFilePathForNormal(filePath);
		if (rsPath == null)
			return;
		for (String[] path : rsPath) {
			// 存储文件路径
			logger.error(filePath.toString());
			filePath = StoreConfig.baseDir + StoreConfig.normalDir+path[0] + "/" + path[1];
			logger.error(filePath.toString());
			// 对应的文件管理器
			FileStoreManager fileStoreManager = fileStoreManagerMap
					.get(filePath);
			if (fileStoreManager == null) {
				fileStoreManager = new FileStoreManager(filePath, "testrecover");
				fileStoreManagerMap.put(filePath, fileStoreManager);
			}
		}
		// 恢复订阅关系
		filePath = StoreConfig.baseDir + StoreConfig.subscribeDir;
		logger.error("subFfilepath="+filePath);
		FileStoreManager fileStoreManager = fileStoreManagerMap.get(filePath);
		if (fileStoreManager == null) {
			fileStoreManager = new FileStoreManager(filePath, "testrecover");
			fileStoreManagerMap.put(filePath, fileStoreManager);
		}
		//恢复retry关系
		//filePath = StoreConfig.baseDir + StoreConfig.;
	}

	public ArrayList<String[]> scanFilePathForNormal(String filePath) {
		ArrayList<String[]> path = new ArrayList<String[]>();
		File[] fileList = new File(filePath).listFiles();
		if (fileList == null)
			return null;
		for (File tempFile : fileList) {
			if (tempFile.isDirectory()) {
				for (File temp : tempFile.listFiles()) {
					String[] tq = new String[2];
					String fs = System.getProperty("file.separator");
					System.out.println(temp.toString() + "    fs= " + fs);
					String t = temp.toString();
					int i = t.lastIndexOf(fs);
					tq[1] = t.substring(i + 1);
					int j = t.lastIndexOf(fs, i - 1);
					tq[0] = t.substring(j + 1, i);
					System.out.println(i + "-----" + j);
					path.add(tq);
				}
			}
		}
		for (String[] t : path) {
			System.out.println(t[0] + " / " + t[1]);
		}
		return path;
	}

}
