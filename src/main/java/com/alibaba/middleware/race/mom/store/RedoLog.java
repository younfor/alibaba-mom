package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;

import com.alibaba.fastjson.JSON;

public class RedoLog {
	private static String redoFile = StoreConfig.baseDir
			+ "oo33o" + "r2222edo";
	public MappedByteBuffer mbb;
	public int fileSize = repeatNum*(msgSize+1);
	public static int repeatNum = 512;
	public static int msgSize = 64;
	public char FLAG = 0;
//god bless
	public RedoLog() {
		this.mbb = this.getFileMappedByteBuffer(redoFile, 0, fileSize);
	}

	// msg=head +body :
	public boolean writeLog(String topic, String filter, int queueIndex,
			ArrayList<byte[]> bodys) {
//		LogBean lb=new LogBean(topic, filter, 0,bodys);
//		byte[] data=JSON.toJSONBytes(lb);
		for (int i = 0; i < bodys.size(); i++) {
			String head = topic + FLAG + filter + FLAG + queueIndex;
			byte[] headByte = head.getBytes();
			// head.size head body.szie body
			ByteBuffer msgByteBuffer = ByteBuffer.allocate(msgSize);
			 
			msgByteBuffer.putInt(headByte.length);
			msgByteBuffer.put(headByte);
			msgByteBuffer.putInt(bodys.get(i).length);
			msgByteBuffer.put(bodys.get(i));
			boolean flag = this.writeLog(msgByteBuffer);
			if (!flag) {
				return false;
			}
		}
		return true;
		//this.flushLog();
		
	}
		
	private boolean writebytes(byte[] smByte)
	{
		if (mbb.position() + smByte.length > fileSize) {
			return false;
		}
		mbb.putInt(smByte.length);
		mbb.put(smByte);
		return true;
	}
	private boolean writeLog(ByteBuffer smByte) {
		if (mbb.position() + smByte.limit() > fileSize) {
			return false;
		}
		mbb.putInt(smByte.limit());
		smByte.flip();
		mbb.put(smByte);
		return true;
	}

	public void flushLog() {
		mbb.force();
		if (mbb.position() > repeatNum * msgSize)
			mbb.position(0);
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

	public static void main(String args[]) throws IOException {
		RedoLog redo = new RedoLog();
		char FLAG = 0;
		for (int j = 0; j < 500; j++) {
			 long begin = System.currentTimeMillis();
			for (int i = 0; i < 200; i++) {
				/*
				 * String data = "topic" + i + FLAG + "fitler" + FLAG +
				 * "hello mom Im baby"; ByteBuffer smByte =
				 * ByteBuffer.allocate(redo.msgSize);
				 * smByte.put(data.getBytes()); redo.writeLog(smByte);
				 */
				ArrayList<byte[]> list = new ArrayList<byte[]>();
				list.add("1234".getBytes());
                 
				redo.writeLog("topic", "filter", 0, list);
			}
			long flushtime = System.currentTimeMillis();
			redo.flushLog();
			System.out.println("writetime:" + (flushtime - begin)
					+ " flushtime:" + (System.currentTimeMillis() - flushtime));
		}

	}
	class LogBean
	{
		String topic;
		String filter;
		int queueid;
		ArrayList<byte[]> list=new ArrayList<>();
		
		public LogBean(String topic, String filter, int queueid, ArrayList<byte[]> list) {
			super();
			this.topic = topic;
			this.filter = filter;
			this.queueid = queueid;
			this.list = list;
		}
		public int getQueueid() {
			return queueid;
		}
		public void setQueueid(int queueid) {
			this.queueid = queueid;
		}
		public LogBean(String topic, String filter, ArrayList<byte[]> list) {
			super();
			this.topic = topic;
			this.filter = filter;
			this.list = list;
		}
		public String getTopic() {
			return topic;
		}
		public void setTopic(String topic) {
			this.topic = topic;
		}
		public String getFilter() {
			return filter;
		}
		public void setFilter(String filter) {
			this.filter = filter;
		}
		public ArrayList<byte[]> getList() {
			return list;
		}
		public void setList(ArrayList<byte[]> list) {
			this.list = list;
		}
		
	}
}
