package com.alibaba.middleware.race.mom.util;

import java.io.Serializable;

/*
 * 封装所有的请求和响应信息
 * type 请求或者响应类型
 * body 对应的数据
 * describle 类型描述信息
 * Author ilyy510
 * */
public class Info implements Serializable{
/**
	 * 
	 */
	private static final long serialVersionUID = -2529148421210651297L;
InfoType type;
byte[] body;
public InfoType getType() {
	return type;
}
public void setType(InfoType type) {
	this.type = type;
}
public byte[] getBody() {
	return body;
}
public void setBody(byte[] body) {
	this.body = body;
}
public static long getSerialversionuid() {
	return serialVersionUID;
}



}
