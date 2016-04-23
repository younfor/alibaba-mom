package com.alibaba.middleware.race.mom.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.middleware.race.mom.broker.function.Offset;

public class FileStoreManager {
	private static Logger logger = LoggerFactory
			.getLogger(FileStoreManager.class);
	// 索引文件
	private IndexMappedFile indexMappedFile;
	// 数据文件
	private DataMappedFile dataMappedFile;

	private String filePath;
	// 最后一次刷数据的时间
	private long lastFlushTime = System.currentTimeMillis();
	private long lastIndexFlushTime = System.currentTimeMillis();
	// 读写锁保证线程安全
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	// 统计已经写入但为落盘到磁盘的消息数目
	private static AtomicLong countMsg = new AtomicLong(0);
	// 落盘时，同步等待index和data文件的缓存落盘到磁盘
	private CountDownLatch countDownLatch;
	// index和data文件多线程并行落盘
	private ExecutorService flushPool = Executors.newFixedThreadPool(1);
	private ExecutorService flushIndexPool = Executors.newFixedThreadPool(1);

	public FileStoreManager(String filePath) {
		this.filePath = filePath;
		this.indexMappedFile = new IndexMappedFile(filePath + "/index");
		this.dataMappedFile = new DataMappedFile(filePath + "/data");
		// logger.debug("初始化filestoremanager成功！" + filePath);
	}

	public FileStoreManager(String filePath, String test) {
		this.filePath = filePath;
		this.indexMappedFile = new IndexMappedFile(filePath + "/index");
		this.dataMappedFile = new DataMappedFile(filePath + "/data");
		// logger.debug("初始化filestoremanager成功！" + filePath);
		this.recover();
	}

	void recover() {
		long[] lastWrittePosition = this.indexMappedFile.getLastWriteData();
		// logger.error("long[] lastWrittePosition=="+lastWrittePosition.toString());
		List<long[]> rs = this.dataMappedFile.checkLastMsg(lastWrittePosition);
		if (rs == null || rs.size() == 0) {
			return;
		} else {
			for (long[] temp : rs) {
				logger.error("新恢复增加indexdata   " + temp[0] + "  /  " + temp[1]
						+ "  /  " + temp[2]);
				this.indexMappedFile.addData(temp[0], (int) temp[1],
						(int) temp[2]);
			}
		}
	}

	/**
	 * 保存一个字节到index和data文件中
	 * 
	 * @param msg
	 */
	private void saveMessage(byte[] msg) {
		lock.writeLock().lock();
		try {
			// 在data文件中增加记录
			int dataWritePosition = (int) this.dataMappedFile.addData(msg);
			int size = msg.length;
			int fileNum = (int) this.dataMappedFile.getDataNum();
			// logger.debug("dataWritePosition,size,fileNum= " +
			// dataWritePosition
			// + "," + size + "," + fileNum);

			// 写入index文件中
			boolean flag = this.indexMappedFile.addData(dataWritePosition,
					size, fileNum);
			if (!flag) {
				logger.warn("没有存成功，未知原因，在index文件中");
			}
			logger.debug("成功将字节写入文件");
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * 保存数据 提供给其他调用的接口，必须是以List包装的字节数据，传入的参数最好用ArrayList
	 */
	static long savemessage = 0;

	public boolean saveMessage(List<byte[]> msgList) {
		long t1 = System.currentTimeMillis();
		for (int i = 0; i < msgList.size(); i++) {
			byte[] temp = msgList.get(i);
			countMsg.addAndGet(1);
			saveMessage(temp);
		}

		flush();
		long t2 = System.currentTimeMillis();
		// savemessage += (t2 - t1);
		logger.error("flush time t2-t1= " + (t2 - t1));
		return true;
	}

	/**
	 * 保存数据 提供给其他调用的接口，必须是以List包装的字节数据，传入的参数最好用ArrayList
	 */

	static long savemessagebyonce = 0;

	public boolean saveMessageByOnce(List<byte[]> msgList) {
		long t1 = System.currentTimeMillis();

		// 获取data文件存储结果
		logger.debug("开始组进入写data文件 msgList.size()==" + msgList.size());
		long[] dataResult = this.dataMappedFile.addDataByOnce(msgList);

		long dataFileNum = this.dataMappedFile.getDataNum();
		logger.debug("开始进入写index文件 dataResult.length= " + dataResult.length);

		boolean indexFlag = this.indexMappedFile.addDataByOnce(dataResult,
				(int) dataFileNum);

		flush();
		long t2 = System.currentTimeMillis();
		logger.error("flush time t2-t1= " + (t2 - t1));
		// savemessagebyonce += (t2 - t1);
		return indexFlag;
	}

	/**
	 * 获取第0条到写的位置条数据, 恢复订阅数据用的函数
	 * 
	 * @return
	 */
	public List<Offset> getSubscribe() {
		ByteBuffer indexByte = ByteBuffer.wrap(this.indexMappedFile.getData());
		ByteBuffer dataByte = ByteBuffer.wrap(this.dataMappedFile.getData());
		int count = indexByte.capacity() / 16;
		List<Offset> subscribeInfoList = new ArrayList<Offset>(count);
		for (int i = 0; i < count; i++) {
			indexByte.position(i * 16);
			long dataStart = indexByte.getLong();
			int size = indexByte.getInt();
			dataByte.position((int) dataStart - 16 + 4);
			byte[] temp = new byte[size];
			dataByte.get(temp);
			subscribeInfoList
					.add((Offset) JSON.parseObject(temp, Offset.class));
		}
		return subscribeInfoList;
	}

	/**
	 * 获取第startCount条到endCount条数据 采用一组数据一组数据的读
	 * 
	 * @param startCount
	 * @param endCount
	 * @return
	 */
	public ArrayList<byte[]> getMessageByOnce(int offset, int maxOffset) {
		logger.warn("开始组获取数据了");
		List<long[]> dataIndexList = this.indexMappedFile.getIndexDataByOnce(
				offset, maxOffset);
		return (ArrayList<byte[]>) this.dataMappedFile
				.getDataByOnce(dataIndexList);

	}

	/**
	 * 获取第startCount条到endCount条数据
	 * 
	 * @param startCount
	 * @param endCount
	 * @return
	 */
	public List<byte[]> getMessage(int startCount, int endCount) {
		lock.readLock().lock();
		List<byte[]> msgList = null;
		try {
			msgList = new ArrayList<byte[]>(endCount - startCount + 1);
			// logger.debug(" return msgList.size() endCount - startCount + 1 = "
			// + (endCount - startCount + 1));
			for (int i = startCount; i <= endCount; i++) {
				// logger.debug("i= " + i);
				long[] returnIndexData = this.indexMappedFile.getData(i);
				// logger.debug(" returnIndexData= " + returnIndexData[0] +
				// " , "
				// + returnIndexData[1] + " , " + returnIndexData[2] + " , ");
				byte[] temp = this.dataMappedFile.getData(returnIndexData);
				// logger.debug("temp.size()= " + temp.length);
				msgList.add(temp);
			}
		} catch (Exception e) {
		} finally {
			lock.readLock().unlock();
		}
		return msgList;
	}

	/**
	 * 多线程并行落盘
	 */
	public void flush() {
		lock.writeLock().lock();
		countDownLatch = new CountDownLatch(2);

		flushIndexPool.execute(new Thread() {
			@Override
			public void run() {
				long t1 = System.currentTimeMillis();

				// 索引文件异步
				try {
					FileStoreManager.this.indexMappedFile.flush();
					countDownLatch.countDown();
				} finally {
					// countDownLatch.countDown();
				}
				long t2 = System.currentTimeMillis();
				// System.out.println(" force index t2-t1==" + (t2 - t1));
			}
		});

		flushPool.execute(new Thread() {
			@Override
			public void run() {
				long t1 = System.currentTimeMillis();
				try {
					FileStoreManager.this.dataMappedFile.flush();
				} finally {
					countDownLatch.countDown();
				}
				long t2 = System.currentTimeMillis();
				// System.out.println(" force data t2-t1==" + (t2 - t1));
			}
		});

		try {
			countDownLatch.await();
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			lock.writeLock().unlock();
		}

		// 测试用的
		// flushPool.shutdown();
	}

	/**
	 * 多线程并行落盘
	 */
	/*
	 * public void flush() { lock.writeLock().lock(); countDownLatch = new
	 * CountDownLatch(2);
	 * 
	 * flushIndexPool.execute(new Thread() {
	 * 
	 * @Override public void run() { long t1 = System.currentTimeMillis();
	 * 
	 * // 索引文件异步 try { FileStoreManager.this.indexMappedFile.flush();
	 * countDownLatch.countDown(); } finally { // countDownLatch.countDown(); }
	 * long t2 = System.currentTimeMillis(); //
	 * System.out.println(" force index t2-t1==" + (t2 - t1)); } });
	 * 
	 * flushPool.execute(new Thread() {
	 * 
	 * @Override public void run() { long t1 = System.currentTimeMillis(); try {
	 * FileStoreManager.this.dataMappedFile.flush(); } finally {
	 * countDownLatch.countDown(); } long t2 = System.currentTimeMillis(); //
	 * System.out.println(" force data t2-t1==" + (t2 - t1)); } });
	 * 
	 * try { countDownLatch.await(); } catch (Exception e) {
	 * logger.error(e.getMessage()); } finally { lock.writeLock().unlock(); }
	 * 
	 * // 测试用的 // flushPool.shutdown(); }
	 */

	/**
	 * 释放资源
	 */
	public void destory() {
		this.indexMappedFile.destory();
		this.dataMappedFile.destory();
	}

	public void resetFilePosition() {
		this.indexMappedFile.resetFilePosition();
		this.dataMappedFile.resetFilePosition();
	}

	/**
	 * 获取第0条到写的位置条数据, 恢复OffsetNum数据用的函数
	 * 
	 * @return
	 */
	public List<OffsetNum> getOffsetNum() {
		ByteBuffer indexByte = ByteBuffer.wrap(this.indexMappedFile.getData());
		ByteBuffer dataByte = ByteBuffer.wrap(this.dataMappedFile.getData());
		int count = indexByte.capacity() / 16;
		List<OffsetNum> offsetNumList = new ArrayList<OffsetNum>(count);
		for (int i = 0; i < count; i++) {
			indexByte.position(i * 16);
			long dataStart = indexByte.getLong();
			int size = indexByte.getInt();
			dataByte.position((int) dataStart - 16 + 4);
			byte[] temp = new byte[size];
			dataByte.get(temp);
			offsetNumList.add((OffsetNum) JSON.parseObject(temp,
					OffsetNum.class));
		}
		return offsetNumList;
	}

}
