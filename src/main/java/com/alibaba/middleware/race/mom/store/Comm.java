package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Comm {
	/**
	 * 获取每个文件的名称地址
	 * 
	 */
	private static Logger logger = LoggerFactory
			.getLogger(Comm.class);
	

	public  static ArrayList<Integer>  getFileNum(String filePath) {
		ArrayList<Integer> fileNumArray=new ArrayList<Integer>();
		logger.error("filePath==="+filePath);
		File indexFile = new File(filePath);
		if (indexFile.isDirectory()) {
			String[] fileList = indexFile.list();
			int fileCount = fileList.length;
			if (fileCount == 0) {
				logger.warn(" 没有 index file 存在！");
			} else {
				// 遍历文件编号并保存
				for (int i = 0; i < fileCount; i++) {
					int t = Integer.parseInt(fileList[i]);
					fileNumArray.add(t);
					logger.error(" 查找index目录下的所有文件并记录为一个数组中    fileList[i]= "+fileList[i]);
				}
			}
			Collections.sort(fileNumArray);
		}
		return fileNumArray;
	}
}
