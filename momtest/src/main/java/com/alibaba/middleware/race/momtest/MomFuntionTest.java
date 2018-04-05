package com.alibaba.middleware.race.momtest;

import java.nio.charset.Charset;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.Consumer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;
import com.alibaba.middleware.race.mom.Producer;
import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;

public class MomFuntionTest {
	private static String TOPIC="MOM-RACE-";
	private static String PID="PID_";
	private static String CID="CID_";
	private static String BODY="hello mom ";
	private static String AREA="area_";
	private static Charset charset=Charset.forName("utf-8");
	private static Random random=new Random();
	private static Queue<String> sendMsg=new LinkedBlockingQueue<String>();
	private static Queue<String> recvMsg=new LinkedBlockingQueue<String>();

	private static TestResult testResult=new TestResult();  
	public static void main(String[] args) {
		System.setProperty("SIP","127.0.0.1");
		testBasic();
		if (!testResult.isSuccess()) {
			System.out.println(testResult);
			FileIO.write(testResult.toString());
			Runtime.getRuntime().exit(0);
		}
		sendMsg.clear();
		recvMsg.clear(); 
		testFilter();
		System.out.println(testResult);
		FileIO.write(testResult.toString());
		Runtime.getRuntime().exit(0);

	}
	private static void testBasic() {
		int code=random.nextInt(100000);
		final ConsumeResult consumeResult=new ConsumeResult();
		consumeResult.setStatus(ConsumeStatus.SUCCESS);
		final String topic=TOPIC+code;
		try {
			String ip=System.getProperty("SIP");
			Consumer consumer=(Consumer) Class.forName("com.alibaba.middleware.race.mom.DefaultConsumer").newInstance();
			consumer.setGroupId(CID+code);
			consumer.subscribe(topic, "", new MessageListener() {
				
				@Override
				public ConsumeResult onMessage(Message message) {
					if (!message.getTopic().equals(topic)) {
						testResult.setSuccess(false);
						testResult.setInfo("expect topic:"+topic+", actual topic:"+message.getTopic());
					}
					if (System.currentTimeMillis()-message.getBornTime()>1000) {
						testResult.setSuccess(false);
						testResult.setInfo("msg "+message.getMsgId()+" delay "+(System.currentTimeMillis()-message.getBornTime())+" ms");
					}
					recvMsg.add(message.getMsgId());
					return consumeResult;
				}
			});
			consumer.start();
			Producer producer=(Producer) Class.forName("com.alibaba.middleware.race.mom.DefaultProducer").newInstance();
			producer.setGroupId(PID+code);
			producer.setTopic(topic);
			producer.start();
			Message msg=new Message();
			msg.setBody(BODY.getBytes(charset));
			msg.setProperty("area", "hz"+code);
			SendResult result=producer.sendMessage(msg);
			if (result.getStatus()!=SendStatus.SUCCESS) {
				testResult.setSuccess(false);
				testResult.setInfo(result.toString());
				return;
			}
			sendMsg.add(result.getMsgId());
			Thread.sleep(5000);
			if (!testResult.isSuccess()) {
				System.out.println(testResult);
				return ;
			}
			checkMsg(sendMsg, recvMsg);
			producer.stop();
			consumer.stop();
		} catch (Exception e) {
			testResult.setSuccess(false);
			testResult.setInfo(e.getMessage());
		}
	}
	private static void testFilter() {
		int code=random.nextInt(100000);
		final ConsumeResult consumeResult=new ConsumeResult();
		consumeResult.setStatus(ConsumeStatus.SUCCESS);
		final String topic=TOPIC+code;
		final String k=AREA+code;
		final String v="hz_"+code;
		try {
			String ip=System.getProperty("SIP");
			Consumer consumer=(Consumer) Class.forName("com.alibaba.middleware.race.mom.DefaultConsumer").newInstance();
			consumer.setGroupId(CID+code);
			consumer.subscribe(topic, k+"="+v, new MessageListener() {
				
				@Override
				public ConsumeResult onMessage(Message message) {
					if (!message.getTopic().equals(topic)) {
						testResult.setSuccess(false);
						testResult.setInfo("expect topic:"+topic+", actual topic:"+message.getTopic());
					}
					if (System.currentTimeMillis()-message.getBornTime()>1000) {
						testResult.setSuccess(false);
						testResult.setInfo("msg "+message.getMsgId()+" delay "+(System.currentTimeMillis()-message.getBornTime())+" ms");
					}
					if (message.getProperty(k)==null||!message.getProperty(k).equals(v)) {
						testResult.setSuccess(false);
						testResult.setInfo("msg "+message.getMsgId()+" expect k"+k+"  value is "+ v+", but actual value is "+message.getProperty(k));
					}
					recvMsg.add(message.getMsgId());
					return consumeResult;
				}
			});
			consumer.start();
			Producer producer=(Producer) Class.forName("com.alibaba.middleware.race.mom.DefaultProducer").newInstance();
			producer.setGroupId(PID+code);
			producer.setTopic(topic);
			producer.start();
			Message msg=new Message();
			msg.setBody(BODY.getBytes(charset));
			msg.setProperty(k, v);
			SendResult result=producer.sendMessage(msg);
			if (result.getStatus()!=SendStatus.SUCCESS) {
				testResult.setSuccess(false);
				testResult.setInfo(result.toString());
				return;
			}
			sendMsg.add(result.getMsgId());
			msg=new Message();
			msg.setBody(BODY.getBytes(charset));
			result=producer.sendMessage(msg);
			if (result.getStatus()!=SendStatus.SUCCESS) {
				testResult.setSuccess(false);
				testResult.setInfo(result.toString());
				return;
			}
			Thread.sleep(5000);
			if (!testResult.isSuccess()) {
				return;
			}
			checkMsg(sendMsg, recvMsg);
			return;


		} catch (Exception e) {
			testResult.setSuccess(false);
			testResult.setInfo(e.getMessage());
		}
	}
	private static void checkMsg(Queue<String> sendMsg,Queue<String> recvMsg){
		if (sendMsg.size()>recvMsg.size()) {
			testResult.setSuccess(false);
			testResult.setInfo("send msg num is "+sendMsg.size()+",but recv msg num is "+recvMsg.size());
			return ;
		}
		if ((recvMsg.size()-sendMsg.size())/sendMsg.size()>0.001) {
			testResult.setSuccess(false);
			testResult.setInfo("repeat rate too big "+(recvMsg.size()-sendMsg.size())/sendMsg.size());
			return ;
		}
		for (String send : sendMsg) {
			boolean find=false;
			for (String recv : recvMsg) {
				if (recv.equals(send)) {
					find=true;
					break;
				}
			}
			if (!find) {
				testResult.setSuccess(false);
				testResult.setInfo("msg "+send+" is miss");
				return ;
			}
		}

	}
}
