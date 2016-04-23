package com.alibaba.middleware.race.mom.store;

import java.util.ArrayList;

import com.alibaba.middleware.race.mom.broker.function.Offset;

/*
 *  by ChenQi
 *  订阅关系持久化
 * */
public interface SubscribeStore {
		//读出所有的订阅关系
		public ArrayList<Offset> read();
		//存所有的订阅关系
		public boolean write(ArrayList<Offset> subInfoList);
}
