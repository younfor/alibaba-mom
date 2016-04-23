//package com.alibaba.middleware.race.mom.store;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Random;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.middleware.race.mom.Message;
//import com.alibaba.middleware.race.mom.broker.function.Offset;
//
//public class Test {
//	String topic = "topic1", group = "group1", consumer = "group1";
//	int queue = 0;
//	BlockingQueue<Message> q = new LinkedBlockingQueue<>();
//	// 进度
//	int offset = 0, maxoffset = 9;
//
//	static class Msg {
//		String a = "a";
//		int b = 3;
//	}
//
//	public static void main(String args[]) {
//		Test t = new Test();
//		// 内存测试
//		// t.init();
//		// t.show();
//		// 存储恢复测试
//		// t.q.clear();
//		// t.init();
//		// t.store();
//		// t.recover();
//		// t.q.clear();
//
//		// t.init();
//		/*
//		 * t.store2_new(); t.recover2_new();
//		 */
//		 t.q.clear();
//		 t.init();
//		 t.store2byonce();
//		 t.recover2();
//
//		// t.testSub();
//		// t.testArrayList();
//		t.testFile2();
//		//t.testFile();
//		//t.scanFilePath(StoreConfig.baseDir+StoreConfig.subscribeDir);
//	}
//
//	public void testFile2() {
//		mstore2.loadAndRecover();
//	}
//	
//	public void testFile() {
//		// 对正常消息
//		String filePath = StoreConfig.baseDir + StoreConfig.normalDir;
//		ArrayList<String[]> rsPath=this.scanFilePathForNormal(filePath);
//		for(String[] path:rsPath){
//			System.out.println();
//		}
//		filePath=StoreConfig.baseDir + StoreConfig.subscribeDir;
//		System.out.println(filePath);
//		String[] rs=scanFilePath(filePath);
//	}
//
//	public String[] scanFilePath(String filePath) {
//		ArrayList<String> path = new ArrayList<String>();
//		String[] fileList = new File(filePath).list();
//		if (fileList == null)
//		{
//			System.out.println("null");
//			return null;
//		}
//			
//		
//		for (String t : fileList) {
//			System.out.println(t);
//		}
//		return fileList;
//	}
//	public ArrayList<String[]> scanFilePathForNormal(String filePath) {
//		ArrayList<String[]> path = new ArrayList<String[]>();
//		File[] fileList = new File(filePath).listFiles();
//		for (File tempFile : fileList) {
//			if (tempFile.isDirectory()) {
//				for (File temp : tempFile.listFiles()) {
//					String[] tq = new String[2];
//					String fs = System.getProperty("file.separator");
//					System.out.println(temp.toString() + "    fs= " + fs);
//					String t = temp.toString();
//					int i = t.lastIndexOf(fs);
//					tq[1] = t.substring(i + 1);
//					int j = t.lastIndexOf(fs, i - 1);
//					tq[0] = t.substring(j + 1, i);
//					System.out.println(i + "-----" + j);
//					path.add(tq);
//				}
//			}
//		}
//		for (String[] t : path) {
//			System.out.println(t[0] + " / " + t[1]);
//		}
//		return path;
//	}
//
//	public void show() {
//		// 显示
//		for (int i = 0; i < 10; i++) {
//			Message m = q.poll();
//			if (i < offset)
//				System.out.println("已消费:" + m.getMsgId());
//			else
//				System.out.println("未消费:" + m.getMsgId());
//		}
//	}
//
//	MsgStore mstore2 = new MsgStoreImp_MappedBuffer2();
//
//	public void store2() {
//
//		List<byte[]> msgList = new ArrayList<byte[]>(10);
//		for (int i = 0; i < 10; i++) {
//			Message msg = q.poll();
//			byte[] msgbyte = JSON.toJSONBytes(msg);// 这里用fjson序列化一下
//			msgList.add(msgbyte);
//		}
//		long startTime = System.currentTimeMillis();
//		mstore2.writeByteNormal(topic, queue + "", msgList);
//		long endTime = System.currentTimeMillis();
//		System.out.println("写入msgList数据时间 endTime-StartTime="
//				+ (endTime - startTime));
//	}
//
//	
//	public void store2byonce() {
//
//		List<byte[]> msgList = new ArrayList<byte[]>(10);
//		for (int i = 0; i < 10; i++) {
//			Message msg = q.poll();
//			byte[] msgbyte = JSON.toJSONBytes(msg);// 这里用fjson序列化一下
//			msgList.add(msgbyte);
//		}
//		long startTime = System.currentTimeMillis();
//		mstore2.writeByteNormalByOnce(topic, queue + "", msgList);
//		long endTime = System.currentTimeMillis();
//		System.out.println("写入msgList数据时间 endTime-StartTime="
//				+ (endTime - startTime));
//	}
//
//	public void recover2() {
//		long startTime = System.currentTimeMillis();
//		ArrayList<byte[]> blist = (ArrayList<byte[]>) mstore2.readByteNormal(
//				topic, queue + "", offset, maxoffset);
//		long endTime = System.currentTimeMillis();
//		System.out.println("  读出数据时间 endTime-StartTime="
//				+ (endTime - startTime));
//
//		for (byte[] b : blist) {
//			Message msg = JSON.parseObject(b, Message.class);// obj 把 byte转为对象
//			System.out.println("未消费:" + msg.getMsgId());
//		}
//	}
//
//
//	public void init() {
//		// queue: 头-> 0 1 2 3 4(offset) 5 6 7 8 9(maxoffset)
//		for (int i = 0; i < 10; i++) {
//			Message msg = new Message();
//			msg.setTopic(topic);
//			msg.setBornTime(System.currentTimeMillis());
//			msg.setBody(new String("测试哦" + new Random().nextLong()).getBytes());
//			System.out.println("init msg id " + msg.getMsgId());
//			q.add(msg);
//		}
//	}
//
//	public void destory() {
//		mstore2.close();
//	}
//
//	public void testSub() {
//		// Offset sb1 = new Offset();
//		// sb1.setTopicId("MOM-RACE-21486");
//		// sb1.setGroupId("CID_21486");
//		// sb1.setQueueId("0");
//		// sb1.setCurrentoffset(1);
//		// sb1.setCacheOffset(1);
//		// sb1.setMaxOffset(1);
//		// Offset sb2 = new Offset();
//		// sb2.setTopicId("ccc");
//		// sb2.setGroupId("dfadfasdlfasdfas");
//		// System.out.println("size(sb1):" + (JSON.toJSONBytes(sb1)).length);
//		// System.out.println("size(sb2):" + (JSON.toJSONBytes(sb2)).length);
//		SubscribeStoreImp_MappedBuffer2 ssi = new SubscribeStoreImp_MappedBuffer2();
//		ArrayList<Offset> ls = new ArrayList<Offset>();
//
//		// ls.add(sb1);
//		// ls.add(sb2);
//		// ssi.write(ls);
//		System.out.println("开始读出订阅关系");
//		ls = ssi.read();
//		for (Offset sb : ls) {
//			System.out.println(sb.toString());
//		}
//
//	}
//
//	public void testArrayList() {
//		OffsetNum sb1 = new OffsetNum();
//		sb1.setI(1111);
//		OffsetNum sb2 = new OffsetNum();
//		sb2.setI(22222);
//		System.out.println("size(sb1):" + (JSON.toJSONBytes(sb1)).length);
//		System.out.println("size(sb2):" + (JSON.toJSONBytes(sb2)).length);
//		ArrayStoreImp ssi = new ArrayStoreImp();
//		ArrayList<OffsetNum> ls = new ArrayList<OffsetNum>();
//
//		ls.add(sb1);
//		ls.add(sb2);
//		ssi.store(ls, "topicAndFilter", "0");
//		System.out.println("开始读出订阅关系");
//		ls = ssi.recover("topicAndFilter", "0");
//		for (OffsetNum sb : ls) {
//			System.out.println(sb.i);
//		}
//	}
//}
