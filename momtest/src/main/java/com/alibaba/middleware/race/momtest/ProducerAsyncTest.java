package com.alibaba.middleware.race.momtest;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.Producer;
import com.alibaba.middleware.race.mom.SendCallback;
import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;

public class ProducerAsyncTest {
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		Producer producer=(Producer) Class.forName("com.alibaba.middleware.race.mom.DefaultProducer").newInstance();
		producer.setGroupId("PG-test");
		producer.setTopic("T-test");
		producer.start();
		Message message=new Message();
		message.setBody("Hello MOM".getBytes());
		message.setProperty("area", "us");
		//调用此方法，当前线程不阻塞
		producer.asyncSendMessage(message, new SendCallback() {
			
			@Override
			public void onResult(SendResult result) {
				if (result.getStatus().equals(SendStatus.SUCCESS)) {
					System.out.println("send success:"+result.getMsgId());
				}
			}
		});
		
	}
}
