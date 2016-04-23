package com.alibaba.middleware.race.mom.store;

import java.io.Serializable;

public class OffsetNum implements Serializable {
	/**
	 * offset 的数值
	 */
	private static final long serialVersionUID = -352056806960750564L;
	int i;
	public int getI() {
		return i;
	}
	public OffsetNum setI(int i) {
		this.i = i;
		return this;
	}
	public String toString(){
		return String.valueOf(i);
	}
}
