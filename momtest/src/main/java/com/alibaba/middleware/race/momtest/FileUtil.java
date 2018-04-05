package com.alibaba.middleware.race.momtest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class FileUtil {
	public static String read(String fileName) {
		try {
			File dir=new File("check");
			if (!dir.exists()) {
				dir.mkdirs();
			}
			File file=new File("check/"+fileName);
			FileReader fReader=new FileReader(file);
			BufferedReader bufferedReader=new BufferedReader(fReader);
			String text=bufferedReader.readLine();
			bufferedReader.close();
			fReader.close();
			return text;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	public static void write(String fileName,String text){
		try {
			File dir=new File("check");
			if (!dir.exists()) {
				dir.mkdirs();
			}
			File file=new File("check/"+fileName);
			if (file.exists()) {
				//file.delete();
				file=new File("check/"+fileName);
			}
			FileWriter fileWriter=new FileWriter(file);
			fileWriter.write(text);
			fileWriter.flush();
			fileWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		String text="hello sdfadsf";
		String fileName="tmp";
		FileUtil.write(fileName, text);
		System.out.println(FileUtil.read(fileName));
		FileUtil.write(fileName, "heloo");
		System.out.println(FileUtil.read(fileName));

	}
}
