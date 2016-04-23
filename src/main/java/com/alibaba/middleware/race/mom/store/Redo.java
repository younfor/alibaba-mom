package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.fastjson.JSON;

public class Redo {
	public static String redoFile = System.getProperty("user.home")
			+ File.separator +"0";
	public MappedByteBuffer mbb;
	public File file;
	public RandomAccessFile raf;
	public AtomicLong filename = new AtomicLong(1);
	public int filesize = 1024*1024 * 1024;
	private RedoBean rb=new RedoBean();
	

	public void writeLog(String msgid,String topicAndfilter,byte[] body)
	{
		rb.getBody().add(body);
		rb.getMsgId().add(msgid);
		rb.getTopicAfilter().add(topicAndfilter);
	}
	public void commitLog(RedoBean rb) {
		byte[] smByte = JSON.toJSONBytes(rb);
		if (mbb.position() + smByte.length > filesize) {
			filename.getAndIncrement();
			try {
				mbb = new RandomAccessFile(getNewFile(), "rw").getChannel().map(
						FileChannel.MapMode.READ_WRITE, 0, filesize);;
			} catch (IOException e) {
			}
		}
		mbb.putInt(smByte.length);
		mbb.put(smByte);
	}

	public void flushLog() {
		commitLog(rb);
		mbb.force();
		rb=new RedoBean();
	}

	public Redo() {
		file = new File(redoFile);
		ensureDirOK(file.getParent());
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(file, "rw");
			FileChannel fileChannel = raf.getChannel();
			mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, filesize);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public File getNewFile() throws FileNotFoundException,
			IOException {
		String newfilename = new String(""
				+ (Long.parseLong(file.getName()) + filesize));
		file = new File(System.getProperty("user.home") + File.separator
				+ "store" + File.separator + newfilename);
		return file;
	}

	public static void main(String args[]) throws IOException {
		for(int j=0;j<100;j++)
		{
			Redo redo = new Redo();
			long begin=System.currentTimeMillis();
			for(int i=0;i<200;i++)
			{
				redo.writeLog("msg-"+i, "topic-and-filter-y", "hello mom I'm body".getBytes());
			}
			redo.flushLog();
			System.out.println(System.currentTimeMillis()-begin);
		}

		
	}
	public  void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
               f.mkdirs();
            }
        }
    }
	class RedoBean implements Serializable{

		/**
		 * 存储的实体
		 */
		private static final long serialVersionUID = -5597911942382576497L;
		List<String> topicAfilter=new ArrayList<>();
		List<String> msgId=new ArrayList<>();
		List<byte[]> body=new ArrayList<>();
		public List<String> getTopicAfilter() {
			return topicAfilter;
		}
		public void setTopicAfilter(List<String> topicAfilter) {
			this.topicAfilter = topicAfilter;
		}
		public List<String> getMsgId() {
			return msgId;
		}
		public void setMsgId(List<String> msgId) {
			this.msgId = msgId;
		}
		public List<byte[]> getBody() {
			return body;
		}
		public void setBody(List<byte[]> body) {
			this.body = body;
		}
		
	}
}
