package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.middleware.race.mom.Message;
/**
 *测试用的代码，是这样提供功能吗？
 *没有考虑内存映射，优化，锁，并发，只是功能，
 *大家看一下，这些接口提供了你们想的信息吗？
 * 不明白，offset ，我暂时理解为在index中的实际位置，根据这个位置可以读一个long值代表datafile中的实际数据的地址。
 */
public class MsgFileUtil {
	Logger logger = LoggerFactory.getLogger(MsgFileUtil.class);
	static String fileRoot = System.getProperty("user.home");
	static String fileSpt = System.getProperty("file.separator");

	private RandomAccessFile getFile(String topic, String groupId, String queueId, String fileName) throws IOException {
		String fileStr;
		if (queueId == null || queueId.equals("") || queueId.isEmpty()) {
			// fileStr = fileRoot + fileSpt + "store" + fileSpt + "normal" +
			// fileSpt + topic + fileSpt + queueId + fileSpt +fileName;
			fileStr = fileRoot + "/store/normal/" + topic + "/" + queueId + "/" + fileName;
		} else {
			// fileStr = fileRoot + fileSpt + "store" + fileSpt + "retry" +
			// fileSpt + groupId + fileSpt + fileName;
			fileStr = fileRoot + "/store/retry/" + groupId + "/" + fileName;
		}
		File f = new File(fileStr);
		if (!f.exists()) {
			if (!f.getParentFile().exists()) {
				f.getParentFile().mkdirs();
			}
			f.createNewFile();
		}
		return new RandomAccessFile(f, "rw");
	}

	/*
	 * 给定 索引文件的地址offset，返回对应data文件的msg
	 *
	 */
	public List<Message> getMsg(String topic, String queueId, String groupId, Long offset) throws IOException {
		if (offset < 0) {
			return null;
		}
		return getMsg(topic, queueId, groupId, offset, offset);
	}

	/*
	 * 根据请求参数传回List<Message>
	 * 
	 * 
	 */
	public List<Message> getMsg(String topic, String queueId, String groupId, Long offset, Long maxOffset)
			throws IOException {
		List<Message> resultMsg = new LinkedList<Message>();
		RandomAccessFile indexFile, dataFile;
		// 根据是否有queueId判定文件位置
		if (queueId == null || queueId.equals("") || queueId.isEmpty()) {
			indexFile = getFile(topic, null, queueId, "index.file");
			dataFile = getFile(topic, null, queueId, "data.file");
		} else {
			indexFile = getFile(null, groupId, null, "index.file");
			dataFile = getFile(null, groupId, null, "data.file");
		}

		// 找到index文件的offset读出真正的data文件的offset

		while (offset <= maxOffset && offset < indexFile.length()) {
			indexFile.seek(offset);
			long dataOffset = indexFile.readLong();
			int dataSize = indexFile.readInt();
			// 根据dataoffset读出 msg
			dataFile.seek(dataOffset);
			byte[] msgBody = new byte[dataSize];
			dataFile.read(msgBody);
			// 读出内容将index文件的type类型设置为已经读“1”
			indexFile.seek(offset + 12);
			indexFile.writeInt(1);
			Message msgTemp = JSON.parseObject(msgBody, Message.class);
			resultMsg.add(msgTemp);
			offset += 16;
		}
		return resultMsg;

	}

	/*
	 * 返回一个队列中所有消息
	 */
	public List<Message> getMsg(String topic, String queueId, String groupId) throws IOException {
		long offset = 0;
		long maxOffset = Long.MAX_VALUE;
		return getMsg(topic, queueId, groupId, offset, maxOffset);
	}

	/*
	 * 给定 indexfile unit { offen(long),length(int),type(int)}
	 */
	public boolean setMsg(String topic, String queueId, String groupId, Message msg) throws Exception {
		RandomAccessFile indexFile, dataFile;
		if (queueId == null || queueId.equals("") || queueId.isEmpty()) {
			indexFile = getFile(topic, null, queueId, "index.file");
			dataFile = getFile(topic, null, queueId, "data.file");
		} else {
			indexFile = getFile(null, groupId, null, "index.file");
			dataFile = getFile(null, groupId, null, "data.file");
		}
		byte[] msgByte = JSON.toJSONBytes(msg);
		long dataPos = dataFile.length();
		long indexPos = indexFile.length();
		//System.out.println("indexpos,datapos=" + indexPos + "," + dataPos);
		// 写入index
		indexFile.seek(indexPos);
		indexFile.writeLong(dataPos);
		indexFile.writeInt(msgByte.length);
		indexFile.writeInt(0);
		// 写入data
		dataFile.seek(dataPos);
		dataFile.write(msgByte);
		//System.out.println("indexpos,datapos=" + indexFile.length() + "," + dataFile.length());
		/*
		 * //恢复数据 test dataPos=dataFile.length(); indexPos=indexFile.length();
		 * for(long i=0;i<indexFile.length();){ indexFile.seek(i); long
		 * offset=indexFile.readLong(); int size=indexFile.readInt(); int
		 * type=indexFile.readInt(); System.out.println("offset="+offset+
		 * " ,size="+size+" ,type="+type); dataFile.seek(offset); byte[]
		 * bufferTemp=new byte[size]; dataFile.read(bufferTemp);
		 * msg=JSON.parseObject(bufferTemp,Message.class);
		 * System.out.println(msg.toString()); i=indexFile.getFilePointer(); }
		 */
		return true;
	}

	public static void main(String[] args) throws Exception {
		Message msg = new Message();
//		msg.setMsgId("msgID");
		msg.setBody("dddddfgf".getBytes());
		msg.setBornTime(System.currentTimeMillis());
		long startTime = System.currentTimeMillis();
		new MsgFileUtil().setMsg("topic", "queueId", null, msg);
		long endTime = System.currentTimeMillis();
		System.out.println("统计 存一条数据 时间 endTime-startTime= " + (endTime - startTime));

		startTime = System.currentTimeMillis();
		List<Message> msgList =  new MsgFileUtil().getMsg("topic", "queueId", null, 0L);
		endTime = System.currentTimeMillis();
		System.out.println("统计 取一条数据 时间 endTime-startTime= " + (endTime - startTime));
		// test
		System.out.println("main " + msgList.get(0).toString());
	}

}
