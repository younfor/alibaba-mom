package com.alibaba.middleware.race.momtest;

import java.nio.charset.Charset;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.Consumer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;
import com.alibaba.middleware.race.mom.Producer;
import com.alibaba.middleware.race.mom.SendCallback;
import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;

public class MomStressTest {
	private static String TOPIC="MOM-RACE-";
	private static String PID="PID_";
	private static String CID="CID_";
	private static String BODY="hello mom ";
	private static String AREA="area_";
	private static Charset charset=Charset.forName("utf-8");
	private static Random random=new Random();
	private static AtomicLong sendCount=new AtomicLong();
	private static AtomicLong recvCount=new AtomicLong();
	private static AtomicLong totalRT=new AtomicLong();
	private static AtomicLong totalDelay=new AtomicLong();
	private static int c=Integer.valueOf(System.getProperty("C","30"));
	private static ExecutorService executorService=Executors.newFixedThreadPool(c);

	private static TestResult testResult=new TestResult();
	public static void main(String[] args) {
		testBasic();
		if (!testResult.isSuccess()) {
			System.out.println(testResult);
			FileIO.write(testResult.toString());
			Runtime.getRuntime().exit(0);
		}
		System.out.println(testResult);
		FileIO.write(testResult.toString());
		Runtime.getRuntime().exit(0);
	}
	private static void testBasic() {
		final int code=random.nextInt(100000);
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
					long delay=System.currentTimeMillis()-message.getBornTime();
/*					if (delay>1000) {
						testResult.setSuccess(false);
						testResult.setInfo("msg "+message.getMsgId()+" delay "+(System.currentTimeMillis()-message.getBornTime())+" ms");
					}*/
					totalDelay.addAndGet(delay);
					recvCount.incrementAndGet();
					return consumeResult;
				}
			});
			consumer.start();
			final Producer producer=(Producer) Class.forName("com.alibaba.middleware.race.mom.DefaultProducer").newInstance();
			producer.setGroupId(PID+code);
			producer.setTopic(topic);
			producer.start();
			long start=System.currentTimeMillis();
			for (int i = 0; i < c; i++) {
				executorService.execute(new Runnable() {
					
					@Override
					public void run() {
						while (true) {
							try {
								Message msg=new Message();
								msg.setBody(BODY.getBytes(charset));
								msg.setProperty("area", "hz"+code);
								final long startRt=System.currentTimeMillis();
								SendResult result=producer.sendMessage(msg);
								if (result.getStatus()==SendStatus.SUCCESS) {
									sendCount.incrementAndGet();
									totalRT.addAndGet(System.currentTimeMillis()-startRt);
								}
								/*producer.asyncSendMessage(msg, new SendCallback() {
									
									@Override
									public void onResult(SendResult result) {
										if (result.getStatus()==SendStatus.SUCCESS) {
											sendCount.incrementAndGet();
											totalRT.addAndGet(System.currentTimeMillis()-startRt);
										}
									}
								});*/
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						
					}
				});
			}
			Thread.sleep(30000);
			if (!testResult.isSuccess()) {
				return ;
			}
			long totalTime=System.currentTimeMillis()-start;
			long sendTps=sendCount.get()*1000/totalTime;
			long recvTps=recvCount.get()*1000/totalTime;
			long rt=totalRT.get()/sendCount.get();
			long delay=totalDelay.get()/recvCount.get();
			testResult.setInfo("send tps:"+sendTps+", recv tps:"+recvTps+", send rt:"+rt+", avg delay:"+delay);
			
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
					if (!message.getProperty(k).equals(v)) {
						testResult.setSuccess(false);
						testResult.setInfo("msg "+message.getMsgId()+" expect k"+k+"  value is "+ v+", but actual value is "+message.getProperty(k));
					}
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
				return ;
			}


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
