package com.alibaba.middleware.race.mom.store;

public class StoreConfig {
	// 主目录
	public static String fileRoot = System.getProperty("user.home");
	// public static String fileSpt = System.getProperty("file.separator");
	public static String baseDir = fileRoot + "/store/";
	// 正常消息文件
	public static String normalDir = "normal/";
	// 重试消息文件
	public static String retryDir = "retry/";
	// 订阅关系
	public static String subscribeDir = "subscribe/";
	// 
	public static String arrayDir = "array/";
	
	// 索引文件名
	/*
	 * 默认 offset(8byte) size(4byte) extra(4byte)
	 */
	public static int offsetByte = 8, sizeByte = 4, extraByte = 4;
	public static String indexFile = "index.file";
	// 消息文件名
	public static String dataFile = "data.file";
	// 订阅关系文件名
	public static String subDataFile = "subData.file";
	public static String subIndexFile = "subIndex.file";
}
