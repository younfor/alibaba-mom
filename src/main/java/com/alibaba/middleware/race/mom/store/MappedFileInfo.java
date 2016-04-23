package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappedFileInfo {
	// 文件大小
	private static long MAX_FILE_SIZE = 1024 * 1024 * 1024;
	// 文件读写位置
	private AtomicLong writePosition = new AtomicLong(0);
	private AtomicLong readPosition = new AtomicLong(0);
	// 上一次刷的文件位置
	private AtomicLong lastFlushFilePosition = new AtomicLong(0);
	// 最后一次刷数据的时间
	private long lastFlushTime = System.currentTimeMillis();

	// 文件路径和名称全称
	private String filePathAndName;
	// 文件的读取方式
	private RandomAccessFile raf;
	private MappedByteBuffer mappedByteBuffer;
	// 映射的FileChannel对象
	private FileChannel fileChannel;
	private static Logger logger = LoggerFactory
			.getLogger(MappedFileInfo.class);

	/**
	 * 构建一个mappedFile文件
	 * 
	 * @param filePathAndName
	 *            传入文件路径和名称
	 */
	public MappedFileInfo(String filePathAndName) {
		this.filePathAndName = filePathAndName;
		createRandomAccessFile();
		getFileMappedByteBuffer(0, MAX_FILE_SIZE);
		setInitPosition();
	}

	/**
	 * 构建一个mappedFile文件
	 * 
	 * @param filePathAndName
	 *            文件路径和名称
	 * @param fileSize
	 *            文件大小
	 */
	public MappedFileInfo(String filePathAndName, long fileSize) {
		this.filePathAndName = filePathAndName;
		this.createRandomAccessFile();
		this.getFileMappedByteBuffer(0, fileSize);
		this.setInitPosition();
	}

	/**
	 * 初始化文件的读写位置变量 读位置（8）+写位置（8） 根据文件的前16个字节获取当前读和写的位置
	 * 
	 */
	private void setInitPosition() {
		// 文件前16字节存放读地址和写地址
		this.mappedByteBuffer.position(0);
		long position;
		// 如果position=0则为起始地址16
		if ((position = this.mappedByteBuffer.getLong()) == 0) {
			this.readPosition.set(16);
		} else {
			this.readPosition.set(position);
		}
		// 更新读位置
		this.mappedByteBuffer.position(0);
		this.mappedByteBuffer.putLong(this.readPosition.get());
		// logger.debug("this.readPosition= "+this.readPosition);
		// 如果position=0则为起始地址16
		if ((position = this.mappedByteBuffer.getLong()) == 0) {
			this.writePosition.set(16);
			this.lastFlushFilePosition.set(16);
		} else {
			this.writePosition.set(position);
			this.lastFlushFilePosition.set(16);
		}
		// 更新写位置
		this.mappedByteBuffer.position(8);
		this.mappedByteBuffer.putLong(this.writePosition.get());
		// logger.debug("this.writePosition= "+this.writePosition.get());
	}

	/**
	 * 建立和确保文件存在
	 */
	private synchronized void createRandomAccessFile() {
		File f = new File(filePathAndName);
		try {
			if (!f.exists()) {
				if (!f.getParentFile().exists()) {
					f.getParentFile().mkdirs();
				}
				f.createNewFile();
			}
			this.raf = new RandomAccessFile(f, "rw");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 打开文件映射
	 * 
	 * @param startPos
	 * @param fileSize
	 */
	private void getFileMappedByteBuffer(long startPos, long fileSize) {
		try {
			this.fileChannel = this.raf.getChannel();
			this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE,
					startPos, fileSize);
		} catch (Exception e) {
			logger.error("内存映射沒有建立");
			logger.error(e.getMessage());
		}
	}

	/**
	 * 保存一组index数据 ，index文件调用
	 * 
	 * @param data
	 * @return
	 */
	public boolean saveDataByOnce(long[] dataArray, int fileNum) {
		long lastposition = this.writePosition.get();
		long currentPosition = lastposition;
		if ((currentPosition + (dataArray.length / 3) * 16) > this.MAX_FILE_SIZE) {
			// 恢复到存数据的开始
			logger.warn("文件空间不够了，需要新建文件");
			return false;
		}
		for (int i = 0; i < dataArray.length; i++) {
			// 一定要定位写的位置
			mappedByteBuffer.position((int) currentPosition);
			mappedByteBuffer.putLong(dataArray[i]);
			i++;
			mappedByteBuffer.putInt((int) dataArray[i]);
			i++;
			mappedByteBuffer.putInt(fileNum);
			currentPosition += 16;
		}
		// 完成后，设置写的写位置
		this.writePosition.set(currentPosition);
		this.lastFlushFilePosition.set(currentPosition);
		mappedByteBuffer.position(8);
		mappedByteBuffer.putLong(currentPosition);
		return true;
	}

	/**
	 * 保存一组data数据
	 * 
	 * @param data
	 * @return
	 */
	public long[] saveDataByOnce(List<byte[]> dataList) {
		long lastposition = this.writePosition.get();
		long currentPosition = lastposition;
		int count = dataList.size();
		// 保存返回结果
		long[] result = new long[count * 3];
		for (int i = 0; i < count; i++) {
			byte[] data = dataList.get(i);
			if ((currentPosition + data.length + 4) > this.MAX_FILE_SIZE) {
				// 恢复到存数据的开始
				logger.warn("保存消息的数据文件空间不够了！");
				currentPosition = lastposition;
				result = null;
				break;
			} else {
				// 表示有空余空间
				if (i == 0) {
					// 加快速度
					mappedByteBuffer.position((int) currentPosition);
				}
				result[i * 3] = currentPosition;
				result[3 * i + 1] = data.length;
				mappedByteBuffer.putInt(data.length);
				mappedByteBuffer.put(data);
				currentPosition += data.length + 4;
			}
		}
		// 设置写的写位置
		this.writePosition.set(currentPosition);
		this.lastFlushFilePosition.set(currentPosition);
		mappedByteBuffer.position(8);
		mappedByteBuffer.putLong(currentPosition);

		return result;
	}

	/**
	 * 保存单条数据
	 * 
	 * @param data
	 * @return
	 */
	public boolean saveData(byte[] data) {
		long position = this.writePosition.get();
		logger.debug("save data[] this.writePosition.get() " + position);
		// 表示有空余空间
		if ((position + data.length + 4) <= this.MAX_FILE_SIZE) {
			mappedByteBuffer.position((int) position);
			mappedByteBuffer.putInt(data.length);
			mappedByteBuffer.put(data);
			this.writePosition.addAndGet(data.length + 4);
			this.lastFlushFilePosition.addAndGet(data.length + 4);
			// 更新文件前16字节writePostion
			mappedByteBuffer.position(8);
			mappedByteBuffer.putLong(this.writePosition.get());
			return true;
		}
		return false;
	}

	/**
	 * 保存单条数据 data文件 带长度位置的msg存储
	 * 
	 * @param data
	 * @return
	 */
	public boolean saveData_new(byte[] data) {
		int length = data.length;
		long position = this.writePosition.get();
		logger.error("save data[] this.writePosition.get() " + position);
		logger.error("save data[] length()  " + length);
		// 表示有空余空间4byte镖师长度
		if ((position + length + 4) <= this.MAX_FILE_SIZE) {
			mappedByteBuffer.position((int) position);
			mappedByteBuffer.putInt(length);
			mappedByteBuffer.put(data);
			this.writePosition.addAndGet(length + 4);
			this.lastFlushFilePosition.addAndGet(length + 4);
			// 更新文件前16字节writePostion
			mappedByteBuffer.position(8);
			mappedByteBuffer.putLong(position + length + 4);
			mappedByteBuffer.position(8);
			// logger.error(" 写入 mappedByteBuffer.position(8)=="+mappedByteBuffer.getLong());
			return true;
		}
		return false;
	}

	/**
	 * 设置文件的首写地址
	 * 
	 * @param pos
	 */
	public void setWritePositionToFile(long pos) {
		mappedByteBuffer.position(8);
		mappedByteBuffer.putLong(pos);
	}

	public boolean saveData(long data) {
		long position = this.writePosition.get();
		logger.debug("save long this.writePosition.get() " + position);
		// 表示有空余空间
		if ((position + 8) <= this.MAX_FILE_SIZE) {
			mappedByteBuffer.position((int) position);
			mappedByteBuffer.putLong(data);
			this.writePosition.addAndGet(8);
			this.lastFlushFilePosition.addAndGet(8);
			// 更新文件前16字节writePostion
			mappedByteBuffer.position(8);
			mappedByteBuffer.putLong(this.writePosition.get());
			return true;
		}
		return false;
	}

	public boolean saveData(int data) {
		long position = this.writePosition.get();
		logger.debug("save int this.writePosition.get() " + position);
		// 表示有空余空间
		if ((position + 4) <= this.MAX_FILE_SIZE) {
			mappedByteBuffer.position((int) position);
			mappedByteBuffer.putInt(data);
			this.writePosition.addAndGet(4);
			// 更新文件前16字节writePostion
			mappedByteBuffer.position(8);
			mappedByteBuffer.putLong(this.writePosition.get());
			return true;
		}
		return false;
	}

	/**
	 * 进行刷新，将数据写入磁盘
	 * 
	 * @return
	 */
	public long flush() {
		this.mappedByteBuffer.force();
		lastFlushFilePosition.set(this.writePosition.get());
		this.lastFlushFilePosition = this.writePosition;
		return this.lastFlushFilePosition.get();
	}

	/**
	 * 获取指定的位置的文件字节数据 暂时不用
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public byte[] getData(long start, long end) {
		if (start < 0 || start > this.writePosition.get()) {
			return null;
		}
		if (end > this.writePosition.get()) {
			end = this.writePosition.get();
		}
		int size = (int) (end - start);
		byte[] temp = new byte[size];
		this.mappedByteBuffer.position((int) start);
		this.mappedByteBuffer.get(temp);
		return temp;
	}

	/**
	 * 获取从读位置到写位置的所有数据作为一个数组 恢复订阅关系使用
	 * 
	 * @return
	 */
	public byte[] getData() {
		int size = (int) (this.writePosition.get() - this.readPosition.get());
		logger.debug(this.writePosition.get() + "   , "
				+ this.readPosition.get());
		byte[] temp = new byte[size];
		this.mappedByteBuffer.position((int) this.readPosition.get());
		this.mappedByteBuffer.get(temp);
		return temp;
	}

	public AtomicLong getWritePosition() {
		return writePosition;
	}

	public void setWritePosition(AtomicLong writePosition) {
		this.writePosition = writePosition;
	}

	public AtomicLong getReadPosition() {
		return readPosition;
	}

	public void setReadPosition(AtomicLong readPosition) {
		this.readPosition = readPosition;
	}

	public AtomicLong getLastFlushFilePosition() {
		return lastFlushFilePosition;
	}

	public void setLastFlushFilePosition(AtomicLong lastFlushFilePosition) {
		this.lastFlushFilePosition = lastFlushFilePosition;
	}

	public long getLastFlushTime() {
		return lastFlushTime;
	}

	public void setLastFlushTime(long lastFlushTime) {
		this.lastFlushTime = lastFlushTime;
	}

	public String getFilePathAndName() {
		return filePathAndName;
	}

	public void setFilePathAndName(String filePathAndName) {
		this.filePathAndName = filePathAndName;
	}

	public RandomAccessFile getRaf() {
		return raf;
	}

	public void setRaf(RandomAccessFile raf) {
		this.raf = raf;
	}

	public MappedByteBuffer getMappedByteBuffer() {
		return mappedByteBuffer;
	}

	public void setMappedByteBuffer(MappedByteBuffer mappedByteBuffer) {
		this.mappedByteBuffer = mappedByteBuffer;
	}

	public FileChannel getFileChannel() {
		return fileChannel;
	}

	public void setFileChannel(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	public static long getMaxFileSize() {
		return MAX_FILE_SIZE;
	}

}
