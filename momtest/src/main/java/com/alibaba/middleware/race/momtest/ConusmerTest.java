package com.alibaba.middleware.race.momtest;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.Consumer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;

public class ConusmerTest {
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		Consumer consumer = (Consumer) Class.forName("com.alibaba.middleware.race.mom.DefaultConsumer").newInstance();
		//设置消费者id，groupid相同的消费者，broker会视为同一个消费者集群，每条消息只会投递给集群中的一台机器
		consumer.setGroupId("CG-test");
		//发起订阅操作，broker只能投递 topic为T-test，并且area属性为us的消息给消费者
		consumer.subscribe("T-test", "area=us"/*如果改属性为null或者空串，那么表示接收这个topic下的所有消息*/, new MessageListener() {

			@Override
			public ConsumeResult onMessage(Message message) {
				assert "T-test".equals(message.getTopic()) && "us".equals(message.getProperty("area"));
				System.out.println("consume success:" + message.getMsgId());
				ConsumeResult result = new ConsumeResult();
				//设置消费结果，如果成功，那么broker不再投递
				result.setStatus(ConsumeStatus.SUCCESS);
				//如果设置为失败，那么broker会尽快重试投递，直至返回成功。
				//result.setStatus(ConsumeStatus.FAIL);
				//消费失败要设置失败原因，broker可以获取到这个信息
				//result.setInfo("fail detail or reason");
				return result;
			}
		});
		consumer.start();
	}
}
