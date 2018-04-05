package com.alibaba.middleware.race.momtest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class FileIO {
	private static File file;
	private static FileWriter fw;
	static{
		file=new File(System.getProperty("file", "result"));
		try {
			fw=new FileWriter(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void write(String text) {
		try {
			fw.write(text);
			fw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
