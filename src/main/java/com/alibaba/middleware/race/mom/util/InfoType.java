package com.alibaba.middleware.race.mom.util;
/**
 * Info消息的类型说明 
 * Author ilyy510
 * */
public enum InfoType {
	PRODUCER_TO_BROKER_SENG_MESSAGE,
	BROKER_TO_PRODUCER_CONFIRM_MESSAGE,
	BROKER_TO_CONSUMER_PUSH_MESSAGE,
    CONSUMER_TO_BROKER_CONFIRM_MESSAGE,
    CONSUMER_TO_BROKER_CONNECT
}
