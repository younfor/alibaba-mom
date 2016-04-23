package com.alibaba.middleware.race.mom.util;

public class InfoUtil {
	public static byte[] arraycat(byte[] buf1,byte[] buf2)
	{
		byte[] bufret=null;
		int len1=0;
		int len2=0;
		if(buf1!=null)
		len1=buf1.length;
		if(buf2!=null)
		len2=buf2.length;
		if(len1+len2>0)
		bufret=new byte[len1+len2];
		if(len1>0)
		System.arraycopy(buf1,0,bufret,0,len1);
		if(len2>0)
		System.arraycopy(buf2,0,bufret,len1,len2);
		return bufret;
	}
}
