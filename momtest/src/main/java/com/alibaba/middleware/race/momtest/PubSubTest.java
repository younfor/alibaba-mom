package com.alibaba.middleware.race.momtest;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.Consumer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;
import com.alibaba.middleware.race.mom.Producer;
import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;

/**
 * 多个订阅者，按照不同的条件，订阅同一个topic的消息
 * 通过调整代码和重启broker或者client来模拟所有功能点
 * 
 * @author longji
 *
 */
public class PubSubTest {
	private static Charset charset = Charset.forName("utf-8");
	private static Random random = new Random();
	private static String seed = "22";// String.valueOf(random.nextInt(10000));
	private static String TOPIC = "MOM-RACE-" + seed;
	private static String PID = "PID_" + seed;
	private static String CID = "CID_" + seed;
	private static String BODY = "hello mom " + seed;
	private static String AREA = "area" + seed;
	private static String CATID = "cid" + seed;
	private static String BIZID = "bid";
	private static List<String> toA = new ArrayList<String>();
	private static List<String> toB = new ArrayList<String>();
	private static List<String> toC = new ArrayList<String>();
	private static ConcurrentHashMap<String, List<Boolean>> amap = new ConcurrentHashMap<String, List<Boolean>>();
	private static ConcurrentHashMap<String, List<Boolean>> bmap = new ConcurrentHashMap<String, List<Boolean>>();
	private static ConcurrentHashMap<String, List<Boolean>> cmap = new ConcurrentHashMap<String, List<Boolean>>();
	private static volatile boolean isRunning=true;
	public static void main(String[] args) throws InterruptedException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		System.setProperty("SIP", "127.0.0.1");
		//这些会记录所有的发送记录和消费记录，用于对账
		String toAs = FileUtil.read("toA");
		String toBs = FileUtil.read("toB");
		String toCs = FileUtil.read("toC");
		String amaps = FileUtil.read("amap");
		String bmaps = FileUtil.read("bmap");
		String cmaps = FileUtil.read("cmap");
		if (toAs != null) {
			toA = JSONArray.parseArray(toAs, String.class);
		}
		if (toBs != null) {
			toB = JSONArray.parseArray(toBs, String.class);
		}
		if (toCs != null) {
			toC = JSONArray.parseArray(toCs, String.class);
		}
		if (amaps != null) {
			amap = JSON.parseObject(amaps, ConcurrentHashMap.class);
		}
		if (bmaps != null) {
			bmap = JSON.parseObject(bmaps, ConcurrentHashMap.class);
		}
		if (cmaps != null) {
			cmap = JSON.parseObject(cmaps, ConcurrentHashMap.class);
		}

		Consumer consumerA = (Consumer) Class.forName("com.alibaba.middleware.race.mom.DefaultConsumer").newInstance();
		consumerA.setGroupId(CID + "A");
		// /A 消费所有消息
		consumerA.subscribe(TOPIC, "", new MessageListener() {

			@Override
			public ConsumeResult onMessage(Message message) {
				ConsumeResult result = new ConsumeResult();
				if (!isRunning) {
					return result;
				}
				if (!TOPIC.equals(message.getTopic())) {
					System.out.println("wrong topic-" + message.getTopic());
				}
				result.setStatus(ConsumeStatus.SUCCESS);

				List<Boolean> value = new ArrayList<Boolean>();
				List<Boolean> oldValue = amap.putIfAbsent(message.getProperty(BIZID), value);
				if (oldValue != null) {
					oldValue.add(Boolean.TRUE);
				} else {
					value.add(Boolean.TRUE);
				}
				return result;
			}
		});
		consumerA.start();

		Consumer consumerB = (Consumer) Class.forName("com.alibaba.middleware.race.mom.DefaultConsumer").newInstance();
		consumerB.setGroupId(CID + "B");
		// /B 消费AREA+seed+"="+seed
		consumerB.subscribe(TOPIC, AREA + "=" + seed, new MessageListener() {

			@Override
			public ConsumeResult onMessage(Message message) {
				ConsumeResult result = new ConsumeResult();
				if (!isRunning) {
					return result;
				}
				if (!TOPIC.equals(message.getTopic())) {
					System.out.println("wrong topic-" + message.getTopic());
				}
				if (!seed.equals(message.getProperty(AREA))) {
					System.out.println("wrong area-" + message.getProperty(AREA));
				}
				result.setStatus(ConsumeStatus.SUCCESS);

				List<Boolean> value = new ArrayList<Boolean>();
				List<Boolean> oldValue = bmap.putIfAbsent(message.getProperty(BIZID), value);
				if (oldValue != null) {
					oldValue.add(Boolean.TRUE);
				} else {
					value.add(Boolean.TRUE);
				}
				return result;
			}
		});
		consumerB.start();
		Consumer consumerC = (Consumer) Class.forName("com.alibaba.middleware.race.mom.DefaultConsumer").newInstance();
		consumerC.setGroupId(CID + "C");
		// /C 消费CATID+seed+"="+seed
		consumerC.subscribe(TOPIC, CATID + "=" + seed, new MessageListener() {

			@Override
			public ConsumeResult onMessage(Message message) {
				ConsumeResult result = new ConsumeResult();
				if (!isRunning) {
					return result;
				}
				//重启broker来观察，是否能否自动重连
				System.out.println(message.getProperty(BIZID));
				if (!TOPIC.equals(message.getTopic())) {
					System.out.println("wrong topic-" + message.getTopic());
				}
				if (!seed.equals(message.getProperty(CATID))) {
					System.out.println("wrong CATID-" + message.getProperty(CATID));
				}
				//此处注释模拟消费失败
				result.setStatus(ConsumeStatus.FAIL);
				//result.setStatus(ConsumeStatus.SUCCESS);
				List<Boolean> value = new ArrayList<Boolean>();
				List<Boolean> oldValue = cmap.putIfAbsent(message.getProperty(BIZID), value);
				if (oldValue != null) {
					oldValue.add(result.getStatus()==ConsumeStatus.SUCCESS?true:false);
				} else {
					value.add(result.getStatus()==ConsumeStatus.SUCCESS?true:false);
				}
				return result;
			}
		});
		//本行可以注释，来模拟c不在线
		consumerC.start();

		Producer producer = (Producer) Class.forName("com.alibaba.middleware.race.mom.DefaultProducer").newInstance();
		producer.setGroupId(PID);
		producer.setTopic(TOPIC);
		producer.start();
		//此处控制发送量
		for (int i = 0; i < 10; i++) {
			Message message = new Message();
			message.setBody((BODY + i).getBytes(charset));
			message.setTopic(TOPIC);
			String bizId = UniqId.getInstance().getUniqIDHashString();
			message.setProperty(BIZID, bizId);
			toA.add(bizId);
			if (i % 2 == 0) {
				message.setProperty(AREA, seed);
				toB.add(bizId);
			}
			if (i % 8 == 0) {
				message.setProperty(CATID, seed);
				toC.add(bizId);
			}
			SendResult result = producer.sendMessage(message);
			if (result.getStatus() == SendStatus.FAIL) {
				System.out.println("fail:" + bizId);
			}
		}
		//改变大小，来模拟客户端在线，broker重启，客户端重连
		Thread.sleep(5000L);
		isRunning=false;
		Thread.sleep(2000L);
		FileUtil.write("toA", JSON.toJSONString(toA));
		FileUtil.write("toB", JSON.toJSONString(toB));
		FileUtil.write("toC", JSON.toJSONString(toC));
		FileUtil.write("amap", JSON.toJSONString(amap));
		FileUtil.write("bmap", JSON.toJSONString(bmap));
		FileUtil.write("cmap", JSON.toJSONString(cmap));
		System.out.println(toA.size()+"--A--"+amap.size());
		System.out.println(toB.size()+"--B--"+bmap.size());
		System.out.println(toC.size()+"--C--"+cmap.size());
		for (String string : toA) {
			if (!amap.containsKey(string)) {
				System.out.println("a miss " + string);
			} else if (amap.get(string).size() > 1) {
				List<Boolean> ackList = amap.get(string);
				for (int i = 0; i < ackList.size(); i++) {
					if (ackList.get(i).booleanValue() && i < ackList.size()-1) {
						System.out.println("a reapeat  " + string);
					}
				}
			}

		}
		for (String string : toB) {
			if (!bmap.containsKey(string)) {
				System.out.println("b miss " + string);
			} else if (bmap.get(string).size() > 1) {
				List<Boolean> ackList = bmap.get(string);
				for (int i = 0; i < ackList.size(); i++) {
					if (ackList.get(i).booleanValue() && i < ackList.size()-1) {
						System.out.println("b reapeat  " + string);
					}
				}
			}
		}
		for (String string : toC) {
			if (!cmap.containsKey(string)) {
				System.out.println("c miss " + string);
			} else if (cmap.get(string).size() > 1) {
				List<Boolean> ackList = cmap.get(string);
				for (int i = 0; i < ackList.size(); i++) {
					if (ackList.get(i).booleanValue() && i < ackList.size()-1) {
						System.out.println("c reapeat  " + string);
					}
				}
			}
		}
		System.out.println("finish ");
		System.exit(0);
	}
}
