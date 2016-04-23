//package com.alibaba.middleware.race.mom.store;
//
//import java.io.File;
//import java.io.FileDescriptor;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.nio.channels.FileChannel.MapMode;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///*
// *     baseDir/Normal/Topic/Group/Consumer/Queue/(index.file,data.file)
// *     baseDir/Retry/Group/(index.file,data.file)
// *     题目没有consumerID就默认生成和groupID一样的
// * */
//
//public class MsgStoreImp_MappedBuffer implements MsgStore {
//	private static Logger logger = LoggerFactory.getLogger(MsgStoreImp_MappedBuffer.class);
//	public static final int OS_PAGE_SIZE = 1024 * 4;
//	// 当前JVM中映射的虚拟内存总大小
//	/*private static final AtomicLong TotalMapedVitualMemory = new AtomicLong(0);
//	// 当前JVM中mmap句柄数量
//	private static final AtomicInteger TotalMapedFiles = new AtomicInteger(0);*/
//	// 文件大小
//	private final static long MAX_FILE_SIZE = 1024 * 1024 * 1024;
//	// 文件写入位置
//	private AtomicLong indexWritePosition = new AtomicLong(0);
//	private AtomicLong dataWritePosition = new AtomicLong(0);
//	// 最后一次刷数据的时间
//	private long lastFlushTime = System.currentTimeMillis();
//	// 上一次刷的文件位置
//	private AtomicLong lastFlushFilePosition = new AtomicLong(0);
//	// 最大的脏数据量，系统必须触发一次强制刷
//	private long MAX_INDEX_BYTE=16;
//	private long MAX_FLUSH_DATA_SIZE = 16 *1; // indexsize=16byte  时间和字节共同
//	// 最大的时间间隔，系统必须触发一次强制刷
//	private long MAX_FLUSH_TIME_GAP = 25; //30ms 因为一次记录写入时间基本在30ms 误差一两条
//	RandomAccessFile indexFile, dataFile;
//	FileDescriptor indexFileDescriptor;
//	FileDescriptor dataFileDescriptor;
//	ConcurrentHashMap<String, RandomAccessFile> fileHashMap = new ConcurrentHashMap<String, RandomAccessFile>();
//	ConcurrentHashMap<RandomAccessFile, MappedByteBuffer> mappedByteBufferHashMap = new ConcurrentHashMap<RandomAccessFile, MappedByteBuffer>();
//	MappedByteBuffer dataMappedByteBuffer, indexMappedByteBuffer;
//
//	@Override
//	public void init() {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void close() {
//		// TODO Auto-generated method stub
//		try {
//
//			if (indexFile != null) {
//				indexFile.close();
//			}
//			if (dataFile != null) {
//				dataFile.close();
//			}
//
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//
//		}
//	}
//
//	public synchronized void getPositionForIndexAndData() {
//		try {
//			// 开始定位到第一条没消费记录
//			if (this.indexWritePosition.get() == 0) {
//				long i = 0;
//				long length = indexFile.length();
//				// logger.debug("indexfile.length= " + length);
//				while (i < length) {
//					// 定位到未读的标示8+4+4
//					indexMappedByteBuffer.position((int) (i + 12));
//					int temp = indexMappedByteBuffer.getInt();
//					if (temp == -1) {
//						i += 16;
//						continue;
//					}
//					break;
//				}
//
//				if (i < 16) {
//					this.dataWritePosition.set(0);
//					this.indexWritePosition.set(0);
//				} else {
//					indexMappedByteBuffer.position((int) (i - 16));
//					this.dataWritePosition.set(indexMappedByteBuffer.getLong() + indexMappedByteBuffer.getInt());
//					this.indexWritePosition.set(i);
//				}
//
//				logger.debug("this.idnexwirtepostion== ,this.dataWritePostion== " + this.indexWritePosition.get() + ", "
//						+ this.dataWritePosition.get());
//			}
//
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//
//	public synchronized void getMappedByteBuffer(String topicId, String groupId, String consumerId, String queueId) {
//		if (topicId == null || topicId.isEmpty()) {
//			indexFile = getFile(null, groupId, null, null, StoreConfig.indexFile);
//			dataFile = getFile(null, groupId, null, null, StoreConfig.dataFile);
//		} else {
//			if (consumerId == null || consumerId.isEmpty()) {
//				consumerId = groupId;
//			}
//			indexFile = getFile(topicId, groupId, consumerId, queueId, StoreConfig.indexFile);
//			dataFile = getFile(topicId, groupId, consumerId, queueId, StoreConfig.dataFile);
//		}
//
//		indexMappedByteBuffer = MapedFile(indexFile, 0, MAX_FILE_SIZE);
//		dataMappedByteBuffer = MapedFile(dataFile, 0, MAX_FILE_SIZE);
//	}
//
//	public boolean saveMessage(String topicId, String groupId, String consumerId, String queueId,
//			List<byte[]> msgList) {
//
//		getMappedByteBuffer(topicId, groupId, consumerId, queueId);
//		getPositionForIndexAndData();
//		byte[] tempMsg = null;
//		for (int i = 0; i < msgList.size(); i++) {
//			tempMsg = msgList.get(i);
//			// 写入 index
//			if (i == 0) {
//				indexMappedByteBuffer.position((int) this.indexWritePosition.get());
//			}
//
//			indexMappedByteBuffer.putLong(this.dataWritePosition.get());
//			indexMappedByteBuffer.putInt(tempMsg.length);
//			indexMappedByteBuffer.putInt(-1);
//			this.indexWritePosition.addAndGet(16);
//			// this.indexWritePosition.set(indexMappedByteBuffer.position());
//			// 写入data
//			if (i == 0) {
//				dataMappedByteBuffer.position((int) this.dataWritePosition.get());
//			}
//
//			dataMappedByteBuffer.put(tempMsg);
//			// this.dataWritePosition += msg.length;
//			this.dataWritePosition.addAndGet(tempMsg.length);
//			// this.dataWritePosition.set(dataMappedByteBuffer.position());
//		}
//		if (this.dataWritePosition.get() >= MAX_FILE_SIZE || this.indexWritePosition.get() >= MAX_FILE_SIZE) {
//			// this.dataWritePosition -= msg.length;
//			// this.indexWritePosition -= 16;
//			this.dataWritePosition.addAndGet(-(tempMsg.length));
//			this.indexWritePosition.addAndGet(-16);
//			logger.debug("文件内存空间不够！");
//			flush();
//			return false;
//		}
//
//		long st1=System.currentTimeMillis();
//		flush();
//
//		return true;
//
//	}
//
//	public boolean saveMessage(String topicId, String groupId, String consumerId, String queueId, byte[] msg) {
//
//		getMappedByteBuffer(topicId, groupId, consumerId, queueId);
//		getPositionForIndexAndData();
//		// 写入 index
//		indexMappedByteBuffer.position((int) this.indexWritePosition.get());
//		indexMappedByteBuffer.putLong(this.dataWritePosition.get());
//		indexMappedByteBuffer.putInt(msg.length);
//		indexMappedByteBuffer.putInt(-1);
//		this.indexWritePosition.addAndGet(16);
//		// 写入data
//		dataMappedByteBuffer.position((int) this.dataWritePosition.get());
//		dataMappedByteBuffer.put(msg);
//		// this.dataWritePosition += msg.length;
//		this.dataWritePosition.addAndGet(msg.length);
//		if (this.dataWritePosition.get() >= MAX_FILE_SIZE || this.indexWritePosition.get() >= MAX_FILE_SIZE) {
//			// this.dataWritePosition -= msg.length;
//			// this.indexWritePosition -= 16;
//			this.dataWritePosition.addAndGet(-msg.length);
//			this.indexWritePosition.addAndGet(-16);
//			logger.debug("文件内存空间不够！");
//			flush();
//			return false;
//		}
//		flush();
//		return true;
//
//	}
//	CountDownLatch countDownLatch;
//	ExecutorService flushPool = Executors.newFixedThreadPool(2);
//	public synchronized void flush() {
//		countDownLatch=new  CountDownLatch(2);
//		flushPool.execute(new Thread(){
//			@Override
//			public void run() {
//				// TODO Auto-generated method stub
//			//	long t1=System.currentTimeMillis();
//				try{
//					MsgStoreImp_MappedBuffer.this.indexMappedByteBuffer.force();
//				}finally{
//				countDownLatch.countDown();
//				}
//			//	long t2=System.currentTimeMillis();
//			//	System.out.println(" force index t2-t1=="+(t2-t1));
//			}
//		});
//		flushPool.execute(new Thread(){
//			@Override
//			public void run() {
//				// TODO Auto-generated method stub
//				//long t1=System.currentTimeMillis();
//				try{
//					MsgStoreImp_MappedBuffer.this.dataMappedByteBuffer.force();
//				}finally{
//					countDownLatch.countDown();
//					
//				}
//			//	long t2=System.currentTimeMillis();
//				//System.out.println(" force data t2-t1=="+(t2-t1));
//			}
//		});
//		try {
//			countDownLatch.await();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		// flushPool.shutdown();
//		this.lastFlushTime = System.currentTimeMillis();
//		this.lastFlushFilePosition = this.indexWritePosition;
//	}
//
//	@Override
//	public boolean writeByteNormal(String topicId, String groupId, String consumerId, String queueId, byte[] msg) {
//		// TODO Auto-generated method stub
//
//		return saveMessage(topicId, groupId, consumerId, queueId, msg);
//
//	}
//
//	public boolean writeByteNormal(String topicId, String groupId, String consumerId, String queueId,
//			List<byte[]> msgList) {
//		// TODO Auto-generated method stub
//		return saveMessage(topicId, groupId, consumerId, queueId, msgList);
//	}
//
//	public synchronized MappedByteBuffer MapedFile(RandomAccessFile raf, long startPos, long fileSize) {
//		MappedByteBuffer mappedByteBuffer = mappedByteBufferHashMap.get(raf);
//		if (mappedByteBuffer != null) {
//			return mappedByteBuffer;
//		}
//		try {
//			FileChannel fileChannel = raf.getChannel();
//			mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, startPos, fileSize);
//			/*TotalMapedVitualMemory.addAndGet(fileSize);
//			TotalMapedFiles.incrementAndGet();*/
//		} catch (Exception e) {
//			logger.debug("内存映射沒有建立");
//			e.printStackTrace();
//		}
//		mappedByteBufferHashMap.put(raf, mappedByteBuffer);
//		return mappedByteBuffer;
//	}
//
//	private RandomAccessFile getFile(String topicId, String groupId, String consumerId, String queueId,
//			String fileName) {
//		String file;
//		if (topicId == null || topicId.isEmpty()) {
//			file = StoreConfig.baseDir + StoreConfig.retryDir + groupId + "/" + fileName;
//		} else {
//			file = StoreConfig.baseDir + StoreConfig.normalDir + topicId + "/" + groupId + "/" + consumerId + "/"
//					+ queueId + "/" + fileName;
//		}
//		// 查找hashmap
//		RandomAccessFile raf = fileHashMap.get(file);
//		if (raf != null) {
//			return raf;
//		}
//		File f = new File(file);
//		try {
//			if (!f.exists()) {
//				if (!f.getParentFile().exists()) {
//					f.getParentFile().mkdirs();
//				}
//				f.createNewFile();
//			}
//			raf = new RandomAccessFile(f, "rw");
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		fileHashMap.put(file, raf);
//		return raf;
//	}
//
//	/*
//	 * 根据请求参数传回List<Message>
//	 * 
//	 * 
//	 */
//	public ArrayList<byte[]> getMessage(String topicId, String groupId, String consumerId, String queueId, int offset,
//			int maxOffset) {
//		ArrayList<byte[]> resultList = new ArrayList<byte[]>();
//		if (topicId == null || topicId.isEmpty()) {
//			indexFile = getFile(null, groupId, null, null, StoreConfig.indexFile);
//			dataFile = getFile(null, groupId, null, null, StoreConfig.dataFile);
//		} else {
//			if (consumerId == null || consumerId.isEmpty()) {
//				consumerId = groupId;
//			}
//			indexFile = getFile(topicId, groupId, consumerId, queueId, StoreConfig.indexFile);
//			dataFile = getFile(topicId, groupId, consumerId, queueId, StoreConfig.dataFile);
//		}
//
//		long len = 0;
//		try {
//			len = indexFile.length() / 16;
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//
//		while (offset <= maxOffset && offset < len) {
//			// long+int+int =16
//			try {
//				int indexoffset = offset * 16;
//				indexFile.seek(indexoffset);
//				long dataOffset = indexFile.readLong();
//				int dataSize = indexFile.readInt();
//				// 根据dataoffset读出 msg
//				dataFile.seek(dataOffset);
//				byte[] msgBody = new byte[dataSize];
//				dataFile.read(msgBody);
//				// 读出内容将index文件的type类型设置为已经读“1”
//				indexFile.seek(indexoffset + 12);
//				indexFile.writeInt(1);
//				resultList.add(msgBody);
//				offset += 1;
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				return resultList;
//			}
//		}
//
//		return resultList;
//
//	}
//
//	@Override
//	public ArrayList<byte[]> readByteNormal(String topicId, String groupId, String consumerId, String queueId,
//			int offset, int maxOffset) {
//		// TODO Auto-generated method stub
//		return getMessage(topicId, groupId, consumerId, queueId, offset, maxOffset);
//
//	}
//
//	@Override
//	public boolean writeByteRetry(String groupId, byte[] msg) {
//		// TODO Auto-generated method stub
//		return saveMessage(null, groupId, null, null, msg);
//	}
//
//	@Override
//	public ArrayList<byte[]> readByteRetry(String groupId, int offset, int maxOffset) {
//
//		return getMessage(null, groupId, null, null, offset, maxOffset);
//
//	}
//
//	public static void main(String[] args) {
//
//	}
//
//	@Override
//	public List<byte[]> readByteByOffsets(String topicID, String group,
//			String consumer, String queueID, Integer[] offsets) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//}
