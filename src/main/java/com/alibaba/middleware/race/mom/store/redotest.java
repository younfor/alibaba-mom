package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class redotest {
	String redoFile = System.getProperty("user.home") + File.separator
			+ "store";
	public MappedByteBuffer mbb;
	public File file;
	public AtomicInteger fileName = new AtomicInteger(0);
	public int filesize = 1024*1024;
	BlockingQueue<MappedByteBuffer> mmbStoreQue = new ArrayBlockingQueue<MappedByteBuffer>(
			10);

	BlockingQueue<MappedByteBuffer> mmbRecoverQue = new ArrayBlockingQueue<MappedByteBuffer>(
			10);

	public void writeLog2(ByteBuffer smByte) {
		
		if (mbb.position() + smByte.limit() > filesize) {
			try {
				mmbRecoverQue.put(this.mbb);
				this.mbb = this.mmbStoreQue.poll(3, TimeUnit.MILLISECONDS);
				if (this.mbb == null) {
					this.createMbb();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// ByteBuffer by = ByteBuffer.allocate(smByte.length);
		mbb.putInt(smByte.limit());
		mbb.put(smByte);
	}

	public void readRecoverLog2() {

		MappedByteBuffer readMbb = null;
		while (mmbRecoverQue.size() != 0) {
			try {
				readMbb = this.mmbRecoverQue.take();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// 恢复数据
			int size = readMbb.getInt();
			while (size != 0) {
				byte[] msg = new byte[size];
				readMbb.get(msg);
				{
					// 开始写indexdata文件
				}
				size = readMbb.getInt();
			}
		}
	}

	public void flushLog() {
		mbb.force();
	}

	public redotest() {
		this.createMbb();
	}

	private void createMbb() {
		for (int i = 0; i < 10; i++) {
			try {
				mmbStoreQue.put(getFileMappedByteBuffer(redoFile
						+ File.separator + fileName.getAndIncrement(), 0,
						filesize));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			this.mbb = mmbStoreQue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private MappedByteBuffer getFileMappedByteBuffer(String filePathAndName,
			int startPos, int fileSize) {
		File f = new File(filePathAndName);
		try {
			if (!f.exists()) {
				if (!f.getParentFile().exists()) {
					f.getParentFile().mkdirs();
				}
				f.createNewFile();
			}
			return mbb = new RandomAccessFile(f, "rw").getChannel().map(
					MapMode.READ_WRITE, startPos, fileSize);

		} catch (Exception e) {
			return null;
		}
	}

	/*
	 * public MappedByteBuffer getNewFile() throws FileNotFoundException,
	 * IOException { String newfilename = new String("" +
	 * (Long.parseLong(file.getName()) + filesize)); file = new
	 * File(System.getProperty("user.home") + File.separator + "store" +
	 * File.separator + newfilename); mbb = new RandomAccessFile(file,
	 * "rw").getChannel().map( FileChannel.MapMode.READ_WRITE, 0, filesize);
	 * return mbb; }
	 */
	public static void main(String args[]) throws IOException {
		final redotest redo = new redotest();
		char FLAG=0;
		
		for(int j=0;j<500;j++)
		{
			final long begin = System.currentTimeMillis();
			for (int i = 0; i < 200; i++) {
				String data="topic"+i+FLAG+"fitler"+FLAG+"hello mom Im baby";
				ByteBuffer smByte=ByteBuffer.allocate(64);
				smByte.put(data.getBytes());
				redo.writeLog2(smByte);
			}
			long flushtime=System.currentTimeMillis();
			redo.flushLog();
			System.out.println("writetime:"+(flushtime-begin)+" flushtime:"
					+ (System.currentTimeMillis() - flushtime));
		}

	}
}
