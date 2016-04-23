package com.alibaba.middleware.race.mom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

class Group {
	long id;
	String name;
	HashMap<String, String> properties = new HashMap<String, String>();
	public String getProperty(String key) {
		return properties.get(key);
	}
	/**
	 * 设置消息属性
	 * @param key
	 * @param value
	 */
	public void setProperty(String key, String value) {
		properties.put(key, value);
	}
	/**
	 * 删除消息属性
	 * @param key
	 */
	public void removeProperty(String key) {
		properties.remove(key);
	}
	public Map<String, String> getProperties() {
		return properties;
	}
	
	List<User> user = new ArrayList<User>();


	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<User> getUser() {
		return user;
	}

	public void setUser(List<User> user) {
		this.user = user;
	}
}

class User {
	long id;
	String name;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}

public class fstjson {
	public static void main(String[] args) {
		Group group = new Group();
		group.setId(0L);
		group.setName("admin");
		group.setProperty("1", "1");
		User guestUser = new User();
		guestUser.setId(2L);
		guestUser.setName("guest");

		User rootUser = new User();
		rootUser.setId(3L);
		rootUser.setName("root");

		group.getUser().add(guestUser);
		group.getUser().add(rootUser);
		String jsonString = JSON.toJSONString(group);
		System.out.println(jsonString);
	}
}
