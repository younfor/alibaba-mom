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

public class DataMappedFile {
	// 文件编号
	private long dataNum;
	// 文件大小
	private long fileSize;
	// 文件数目
	private long fileCount = 0;
	// data文件
	private MappedFileInfo dataMappedFile;
	// 文件的fileChannel
	private FileChannel fileChannel;
	// 文件的MappedByteBuffer
	private MappedByteBuffer mappedByteBuffer;
	// 文件夹的路径
	private String filePath;
	private static Logger logger = LoggerFactory
			.getLogger(DataMappedFile.class);
	// 保存已经打开的读文件列表
	private static ConcurrentHashMap<String, MappedFileInfo> readDataFileMap = new ConcurrentHashMap<String, MappedFileInfo>();
	public long dataWriterPositon;

	/**
	 * 构造函数
	 * 
	 * @param filePath
	 */
	public DataMappedFile(String filePath) {
		this.filePath = filePath;
		this.initFileInfo();
		/*
		 * this.mappedByteBuffer = this.dataMappedFile.getMappedByteBuffer();
		 * this.fileChannel = this.dataMappedFile.getFileChannel();
		 */

	}

	/**
	 * 把数据从data文件的读出 带msg大小的标识
	 * 
	 * @param Offset
	 *            在data文件中的位置
	 * @param size
	 *            写入data文件中数据的大小
	 * @param dataFileNum
	 *            写入data文件的名称
	 * @return
	 * 
	 */
	public List<long[]> checkLastMsg(long[] indexData) {
		// 保存已经开打的再读data文件
		List<long[]> rs = new ArrayList<long[]>();
		int offset, size, dataFileNum;
		if (indexData == null) {
			// 初始状态，index文件没有数据
			logger.error("indexData == null");
			dataFileNum = 0;
			offset = 16;
			size = 0;
			logger.error(" int offset, int size, int dataFileNum=" + offset
					+ " " + size + "  " + dataFileNum);
		} else {
			offset = (int) indexData[0];
			size = (int) indexData[1];
			dataFileNum = (int) indexData[2];
			logger.error("int offset, int size, int dataFileNum=" + offset
					+ " " + size + "  " + dataFileNum);

		}
		MappedFileInfo readForDataFile = readDataFileMap.get(this.filePath
				+ "/" + dataFileNum);
		if (readForDataFile == null) {
			readForDataFile = new MappedFileInfo(this.filePath + "/"
					+ dataFileNum);
			readDataFileMap.put(this.filePath + "/" + dataFileNum,
					readForDataFile);
		}

		MappedByteBuffer readMappedByteFile = readForDataFile
				.getMappedByteBuffer();
		int writePos = (int) readForDataFile.getWritePosition().get();
		logger.error("int writePos = (int) readForDataFile.getWritePosition().get();="
				+ writePos);
		// 定位到读位置，然后获取byte数据
		int currentPos = offset + size + 4;
		// 当前data文件中
		if (currentPos < writePos) {
			do {
				readMappedByteFile.position(currentPos);
				long nextSize = readMappedByteFile.getInt();
				long[] t = new long[3];
				t[0] = currentPos;
				t[1] = nextSize;
				t[2] = dataFileNum;
				logger.error("t[0] = currentPos;t[1] = nextSize;t[2]=dataFileNum;"
						+ t.toString());
				rs.add(t);
				currentPos += nextSize + 4;
			} while (currentPos < writePos);
			// 判断是否还有下一个data文件,并进行恢复
			ArrayList<Integer> fileNameList = Comm.getFileNum(this.filePath);
			for (int i=0; i < fileNameList.size(); i++) {
                   if(fileNameList.get(i)>dataFileNum){
                	   currentPos=16;
                	   dataFileNum=fileNameList.get(i);
                	   do {
           				readMappedByteFile.position(currentPos);
           				long nextSize = readMappedByteFile.getInt();
           				long[] t = new long[3];
           				t[0] = currentPos;
           				t[1] = nextSize;
           				t[2] = dataFileNum;
           				logger.error("t[0] = currentPos;t[1] = nextSize;t[2]=dataFileNum;"
           						+ t.toString());
           				rs.add(t);
           				currentPos += nextSize + 4;
           			} while (currentPos < writePos);
                   }
			}
		}
		return rs;
	}

	/**
	 * 获取每个文件的名称地址
	 * 
	 */
	private ArrayList<Integer> dataNumArray;

	/**
	 * 创建或者获取当前使用的index文件 暂时以文件的数目来确定存储文件
	 */
	private void initFileInfo() {
		dataNumArray = Comm.getFileNum(this.filePath);
		if (this.dataNumArray == null || this.dataNumArray.size() == 0) {
			logger.warn("create index file");
			this.dataMappedFile = new MappedFileInfo(this.filePath + "/0");
			this.dataNum = 0;
			this.fileCount = 1;
			this.dataNumArray.add(0);
		} else {
			// 判断文件的最大编号
			int max = dataNumArray.get(dataNumArray.size() - 1);
			logger.warn("max index " + max);
			this.dataNum = max;
			this.dataMappedFile = new MappedFileInfo(this.filePath + "/"
					+ this.dataNum);
			this.fileCount = dataNumArray.size();
		}

		logger.error("initFileInfo 创建了一个datafile + this.datanum==="
				+ this.dataNum);
		readDataFileMap.put(this.filePath + "/" + this.dataNum,
				this.dataMappedFile);
	}

	/**
	 * 保存data数据到data文件
	 * 
	 * @param data
	 * @return
	 */
	public long addData(byte[] data) {
		// 保存当写入失败是上次写的位置
		long lastWritePosition = this.dataMappedFile.getWritePosition().get();
		boolean addDataFlag = this.dataMappedFile.saveData(data);
		if (!addDataFlag) {
			logger.error("增加数据时，当前data文件空间不够 ,将新建data文件");
			// 恢复data的写位置
			this.dataMappedFile.setWritePosition(new AtomicLong(
					lastWritePosition));
			this.dataMappedFile.setWritePositionToFile(lastWritePosition);
			this.dataMappedFile.flush();
			// 新建data文件
			this.dataMappedFile = new MappedFileInfo(this.filePath + "/"
					+ this.fileCount);
			this.dataNum = this.fileCount;
			dataNumArray.add((int) this.dataNum);
			Collections.sort(dataNumArray);
			this.fileCount++;
			// 重新写入文件
			return this.addData(data);
		}
		return lastWritePosition;
		// this.dataMappedFile.flush();
	}

	/**
	 * 保存data数据到data文件
	 * 
	 * @param data
	 * @return
	 */
	public long addData_new(byte[] data) {
		// 保存当写入失败是上次写的位置
		long lastWritePosition = this.dataMappedFile.getWritePosition().get();
		// 新存储data
		boolean addDataFlag = this.dataMappedFile.saveData(data);
		if (!addDataFlag) {
			logger.error("增加数据时，当前data文件空间不够  将新建data文件");
			// 恢复data的写位置
			this.dataMappedFile.setWritePosition(new AtomicLong(
					lastWritePosition));
			this.dataMappedFile.setWritePositionToFile(lastWritePosition);
			this.dataMappedFile.flush();
			// 新建data文件
			this.dataMappedFile = new MappedFileInfo(this.filePath + "/"
					+ this.fileCount);
			this.dataNum = this.fileCount;
			dataNumArray.add((int) this.dataNum);
			Collections.sort(dataNumArray);
			this.fileCount++;
			// 重新写入文件
			return this.addData(data);
		}
		return lastWritePosition;
		// this.dataMappedFile.flush();
	}


	/**
	 * 保存dataList数据到data文件
	 * 
	 * @param data
	 * @return
	 */
	public long[] addDataByOnce(List<byte[]> dataList) {
		// 保存当写入失败是上次写的位置
		long lastWritePosition = this.dataMappedFile.getWritePosition().get();
		long[] result = this.dataMappedFile.saveDataByOnce(dataList);

		if (result == null) {
			logger.error("增加数据时，当前data文件空间不够  将新建data文件");
			// 恢复data的写位置
			this.dataMappedFile.setWritePosition(new AtomicLong(
					lastWritePosition));
			this.dataMappedFile.setWritePositionToFile(lastWritePosition);
			this.dataMappedFile.flush();
			// 新建data文件
			this.dataMappedFile = new MappedFileInfo(this.filePath + "/"
					+ this.fileCount);
			this.dataNum = this.fileCount;
			// 加入数据并排序
			dataNumArray.add((int) this.dataNum);
			Collections.sort(dataNumArray);
			this.fileCount++;
			// 重新写入文件
			logger.error("增加数据时，当前data文件空间不够  将新建data文件" + dataNum);
			return this.addDataByOnce(dataList);
		}
		return result;
		// this.dataMappedFile.flush();
	}

	/**
	 * 将文件buffer刷新落盘保持到磁盘
	 */
	public void flush() {
		this.dataMappedFile.flush();
	}

	/**
	 * 从指定文件中获取byte[]数据
	 * 
	 * @param offset
	 *            指定文件读取位置 size 读取大小 dataFileName 数据文件
	 * @return byte[]数据
	 */

	public byte[] getData(long[] dataPos) {
		return getData((int) dataPos[0], (int) dataPos[1], (int) dataPos[2]);
	}

	/**
	 * 获取一组数据
	 * 
	 */
	public List<byte[]> getDataByOnce(List<long[]> dataIndexList) {
		List<byte[]> result = new ArrayList<byte[]>();
		for (int i = 0; i < dataIndexList.size(); i++) {
			long[] dataPos = dataIndexList.get(i);
			result.add(getData((int) dataPos[0], (int) dataPos[1],
					(int) dataPos[2]));
		}
		return result;

	}

	/**
	 * 把数据从data文件的读出
	 * 
	 * @param Offset
	 *            在data文件中的位置
	 * @param size
	 *            写入data文件中数据的大小
	 * @param dataFileNum
	 *            写入data文件的名称
	 * @return
	 * 
	 */
	public byte[] getData(int offset, int size, int dataFileNum) {
		// 保存已经开打的再读data文件
		MappedFileInfo readForDataFile = readDataFileMap.get(this.filePath
				+ "/" + dataFileNum);
		if (readForDataFile == null) {
			readForDataFile = new MappedFileInfo(this.filePath + "/"
					+ dataFileNum);
			readDataFileMap.put(this.filePath + "/" + dataFileNum,
					readForDataFile);
		}

		MappedByteBuffer readMappedByteFile = readForDataFile
				.getMappedByteBuffer();
		// 定位到读位置，然后获取byte数据
		readMappedByteFile.position(offset + 4);
		byte[] tempData = new byte[size];
		readMappedByteFile.get(tempData);
		return tempData;
	}

	/**
	 * 把数据从data文件的读出 带msg大小的标识
	 * 
	 * @param Offset
	 *            在data文件中的位置
	 * @param size
	 *            写入data文件中数据的大小
	 * @param dataFileNum
	 *            写入data文件的名称
	 * @return
	 * 
	 */
	public byte[] getData_new(int offset, int size, int dataFileNum) {
		// 保存已经开打的再读data文件
		System.out.println("int offset, int size, int dataFileNum=" + offset
				+ " " + size + "  " + dataFileNum);
		MappedFileInfo readForDataFile = readDataFileMap.get(this.filePath
				+ "/" + dataFileNum);
		if (readForDataFile == null) {
			readForDataFile = new MappedFileInfo(this.filePath + "/"
					+ dataFileNum);
			readDataFileMap.put(this.filePath + "/" + dataFileNum,
					readForDataFile);
		}

		MappedByteBuffer readMappedByteFile = readForDataFile
				.getMappedByteBuffer();
		// 定位到读位置，然后获取byte数据
		readMappedByteFile.position(offset + 4);
		byte[] tempData = new byte[size];
		readMappedByteFile.get(tempData);
		return tempData;
	}

	/**
	 * 获取从读位置到写位置的所有的数据
	 * 
	 * @return
	 */
	public byte[] getData() {
		return this.dataMappedFile.getData();

	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public long getFileCount() {
		return fileCount;
	}

	public void setFileCount(long fileCount) {
		this.fileCount = fileCount;
	}

	public MappedFileInfo getDataMappedFile() {
		return dataMappedFile;
	}

	public void setDataMappedFile(MappedFileInfo dataMappedFile) {
		this.dataMappedFile = dataMappedFile;
	}

	public FileChannel getFileChannel() {
		return fileChannel;
	}

	public void setFileChannel(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	public MappedByteBuffer getMappedByteBuffer() {
		return mappedByteBuffer;
	}

	public void setMappedByteBuffer(MappedByteBuffer mappedByteBuffer) {
		this.mappedByteBuffer = mappedByteBuffer;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public long getDataNum() {
		return dataNum;
	}

	public void setDataNum(long dataNum) {
		this.dataNum = dataNum;
	}

	public void destory() {
		// TODO Auto-generated method stub
		// this.mappedByteBuffer
	}

	/**
	 * 重新设置写位置
	 */
	public void resetFilePosition() {
		this.dataMappedFile.setWritePositionToFile(16);
		this.dataMappedFile.setWritePosition(new AtomicLong(16));
	}
}
