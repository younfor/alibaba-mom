package com.alibaba.middleware.race.momtest;

import java.util.Date;

public class TestResult {
	private boolean isSuccess=true;
	private String info;
	public boolean isSuccess() {
		return isSuccess;
	}
	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}
	public String getInfo() {
		return info;
	}
	public void setInfo(String info) {
		this.info = info;
	}
	@Override
	public String toString() {
		return "result:"+isSuccess+", info:"+info;
	}
	public static void main(String[] args) {
		System.out.println(new Date(1436787640930L));
	}
}
