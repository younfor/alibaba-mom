package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMappedFile {
	// 文件编号
	private long indexNum;
	// 文件大小
	private long fileSize;
	// 文件数目
	private long fileCount = 0;
	// index文件
	private MappedFileInfo indexMappedFile;

	// 文件的fileChannel
	private FileChannel fileChannel;
	// 文件的MappedByteBuffer
	private MappedByteBuffer mappedByteBuffer;
	// 文件夹的路径
	private String filePath;
	// 保存已经打开的读文件列表
	ConcurrentHashMap<String, MappedFileInfo> readIndexFileMap = new ConcurrentHashMap<String, MappedFileInfo>();

	private static Logger logger = LoggerFactory
			.getLogger(IndexMappedFile.class);

	public long[] getLastWriteData() {
		long[] result=null;
		long pos = this.indexMappedFile.getWritePosition().get();
		if (pos == 16) {
			//初始状态
           return result;
		}
		this.indexMappedFile.getMappedByteBuffer().position((int) (this.indexMappedFile
				.getWritePosition().get() - 16));
		long offset = this.indexMappedFile.getMappedByteBuffer().getLong();

		logger.error("从index 读出data 的位置offset====" + offset);
		int size = this.indexMappedFile.getMappedByteBuffer().getInt();
		int dataFileNum = this.indexMappedFile.getMappedByteBuffer().getInt();
		result = new long[] { offset, size, dataFileNum };
		return result;
	}

	/**
	 * 构造函数
	 * 
	 * @param filePath
	 */
	public IndexMappedFile(String filePath) {
		this.filePath = filePath;
		this.initFileInfo();
		/*
		 * this.mappedByteBuffer = this.indexMappedFile.getMappedByteBuffer();
		 * this.fileChannel = this.indexMappedFile.getFileChannel();
		 */
	}

	/**
	 * 获取每个文件的名称地址
	 * 
	 */
	private ArrayList<Integer> indexNumArray;


	/**
	 * 创建或者获取当前使用的index文件 暂时以文件的数目来确定存储文件
	 */
	private void initFileInfo() {
		indexNumArray = Comm.getFileNum(this.filePath);
		if (this.indexNumArray == null || this.indexNumArray.size() == 0) {
			logger.warn("create index file");
			this.indexMappedFile = new MappedFileInfo(this.filePath + "/0");
			this.indexNum = 0;
			this.fileCount = 1;
			this.indexNumArray.add(0);
		} else {
			// 判断文件的最大编号
			int max = indexNumArray.get(indexNumArray.size() - 1);
			logger.error("max index " + max);
			this.indexNum = max;
			this.indexMappedFile = new MappedFileInfo(this.filePath + "/"
					+ this.indexNum);
			this.fileCount = indexNumArray.size();
		}

		logger.error("inti file createFile 创建了一个indexfile + this.indexnum==="
				+ this.indexNum);
		readIndexFileMap.put(this.filePath + "/" + this.indexNum,
				this.indexMappedFile);
	}


	/**
	 * 把数据在data文件的写入信息保存到index文件中
	 * 
	 * @param dataOffset
	 *            在data文件中的位置
	 * @param size
	 *            写入data文件中数据的大小
	 * @param dataFileNum
	 *            写入data文件的名称
	 * @return
	 */
	public boolean addData(long dataOffset, int size, int dataFileNum) {
		// 保存当写入失败是上次写的位置
		long lastWritePosition = this.indexMappedFile.getWritePosition().get();
		// 依次写入data数据信息
		boolean[] addDataFlag = new boolean[3];
		addDataFlag[0] = this.indexMappedFile.saveData(dataOffset);
		addDataFlag[1] = this.indexMappedFile.saveData(size);
		addDataFlag[2] = this.indexMappedFile.saveData(dataFileNum);
		if (!(addDataFlag[0] && addDataFlag[1] && addDataFlag[2])) {
			logger.error("增加数据时，当前index文件空间不够  将新建index文件");
			// 恢复index的写位置
			this.indexMappedFile.setWritePosition(new AtomicLong(
					lastWritePosition));
			this.indexMappedFile.setWritePositionToFile(lastWritePosition);
			this.indexMappedFile.flush();
			// 新建index文件
			// 新建index文件,文件名为开始记录
			logger.error("index 增加数据时，当前index文件空间不够  将新建index文件 old this.indexNum="
					+ this.indexNum
					+ "  /lastWritePosition=="
					+ lastWritePosition);
			this.indexNum += ((lastWritePosition / 16) - 1);
			logger.error("index 增加数据时，当前index文件空间不够  将新建index文件 new this.indexNum="
					+ this.indexNum);
			// 加入数组中，默认是加到数据末尾
			indexNumArray.add((int) this.indexNum);
			this.indexMappedFile = new MappedFileInfo(this.filePath + "/"
					+ this.indexNum);
			logger.error("index 增加数据时，当前index文件空间不够  将新建index文件  his.indexNum"
					+ this.indexNum);
			this.fileCount++;
			// 重新写入文件
			return this.addData(dataOffset, size, dataFileNum);
		}
		logger.debug("增加index数据时 成功");
		return true;
	}

	/**
	 * 把数据在data文件的写入信息保存到index文件中
	 * 
	 * @param dataOffset
	 *            在data文件中的位置
	 * @param size
	 *            写入data文件中数据的大小
	 * @param dataFileNum
	 *            写入data文件的名称
	 * @return
	 */
	public boolean addDataByOnce(long[] dataArray, int dataFileNum) {

		// 保存当写入失败是上次写的位置
		long lastWritePosition = this.indexMappedFile.getWritePosition().get();
		// 依次写入data数据信息
		boolean addDataFlag = this.indexMappedFile.saveDataByOnce(dataArray,
				dataFileNum);
		if (!addDataFlag) {
			logger.error("增加数据时，当前index文件空间不够  将新建index文件");
			// 恢复index的写位置
			this.indexMappedFile.setWritePosition(new AtomicLong(
					lastWritePosition));
			this.indexMappedFile.setWritePositionToFile(lastWritePosition);
			this.indexMappedFile.flush();
			// 新建index文件,文件名为开始记录
			this.indexNum += ((lastWritePosition / 16) - 1);
			// 加入数组中，默认是加到数据末尾
			indexNumArray.add((int) this.indexNum);

			logger.error("create index file name indexnum== " + this.indexNum);
			logger.error("index 增加数据时，当前index文件空间不够  将新建index文件  indexNumArray.SIZE()"
					+ indexNumArray.size());
			this.indexMappedFile = new MappedFileInfo(this.filePath + "/"
					+ this.indexNum);
			this.fileCount++;
			// 重新写入文件
			return this.addDataByOnce(dataArray, dataFileNum);
		}
		logger.debug("增加index数据时 成功");
		return true;
		// this.indexMappedFile.flush();
	}

	/**
	 * 将文件buffer落盘保持到磁盘
	 */
	public long flush() {
		return this.indexMappedFile.flush();
	}

	/**
	 * 获取信息，防护给data文件读取数据， 获取指定位置的一个单位(8+4+4)
	 * 
	 */
	public long[] getData(int countStart) {
		// 获取需要打开index文件名
		if (indexNumArray == null || indexNumArray.size() == 0) {
			logger.error(" 没有 index file 存在！countStart= " + countStart);
			return null;
		} else {
			// 判断文件的最大编号
			int fileCount = indexNumArray.size();
			int readIndexFileNum = 0;
			for (int i = (fileCount - 1); i >= 0; i--) {
				// logger.debug("get data create index  filedir count tempNum= "
				// + readIndexFileNum);
				int t = indexNumArray.get(i);
				if (t > countStart) {
					continue;
				} else {
					readIndexFileNum = t;
					break;
				}
			}
			// 转化到当前文件的文件内地址

			logger.debug("转化到当前文件的文件内地址 readIndexFileNum___" + readIndexFileNum);
			MappedFileInfo readForIndexFile = readIndexFileMap
					.get(this.filePath + "/" + readIndexFileNum);

			if (readForIndexFile == null) {
				readForIndexFile = new MappedFileInfo(this.filePath + "/"
						+ readIndexFileNum);
				readIndexFileMap.put(this.filePath + "/" + readIndexFileNum,
						readForIndexFile);
				logger.error("创建读index文件 ：" + this.filePath + "/"
						+ readIndexFileNum);
			}
			// logger.debug("读index文件 ：" + this.filePath + "/" + fileNum);
			// 将countStart转化为指定index文件的相对地址，注意要偏移16位
			int tempPosition = (countStart - readIndexFileNum) * 16 + 16;
			MappedByteBuffer readMappedByteFile = readForIndexFile
					.getMappedByteBuffer();
			readMappedByteFile.position((int) tempPosition);
			logger.error("在读出index 数据的 位置   tempPosition====" + tempPosition);
			// 获取与data文件记录相关信息
			long offset = readMappedByteFile.getLong();
			logger.error("从index 读出data 的位置offset====" + offset);
			int size = readMappedByteFile.getInt();
			int dataFileNum = readMappedByteFile.getInt();
			long[] result = new long[] { offset, size, dataFileNum };
			return result;
		}

	}

	/**
	 * 获取信息，防护给data文件读取数据， 获取指定位置的一个单位(8+4+4)
	 * 
	 */
	public List<long[]> getIndexDataByOnce(int countStart, int countEnd) {
		List<long[]> result = new ArrayList<long[]>();
		for (int i = countStart; i <= countEnd; i++) {
			logger.error("int i = countStart; i <= countEnd" + i
					+ "  int countStart, int countEnd== " + countStart + " , "
					+ countEnd);
			result.add(this.getData(i));
		}
		return result;
	}

	/**
	 * 获取从读位置到写位置的所有的数据
	 * 
	 * @return
	 */
	public byte[] getData() {
		return this.indexMappedFile.getData();

	}

	public void destory() {
		// this.indexMappedFile
	}

	/**
	 * 重新设置写位置
	 */
	public void resetFilePosition() {
		this.indexMappedFile.setWritePositionToFile(16);
		this.indexMappedFile.setWritePosition(new AtomicLong(16));
	}
}
