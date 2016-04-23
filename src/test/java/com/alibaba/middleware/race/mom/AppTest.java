package com.alibaba.middleware.race.mom;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest {
	private static ExecutorService executorService=Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	public  static void main(String args[]) throws InterruptedException{
		long start=System.currentTimeMillis();
		for (int i = 0; i <Runtime.getRuntime().availableProcessors(); i++) {
			executorService.execute(new Runnable() {
				int i=0;
				@Override
				public void run() {
					while (true) {
						try {
							System.out.println(i++);
							Thread.sleep(3000);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}			
				}
			});
		}
		Thread.sleep(3000);
		if (true) {
			System.out.println("over");
			return ;
		}
	}
}
    
