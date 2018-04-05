package com.alibaba.middleware.race.momtest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;



/**
 * @author huangshang
 * 
 */
public class UniqId {

    private static char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    private static Map<Character, Integer> rDigits = new HashMap<Character, Integer>(16);
    static {
        for (int i = 0; i < digits.length; ++i) {
            rDigits.put(digits[i], i);
        }
    }

    private static UniqId me = new UniqId();
    private String hostAddr;
    private final Random random = new Random();
    private final static ThreadLocal<MessageDigest> mHasher = new ThreadLocal<MessageDigest>();
    private final UniqTimer timer = new UniqTimer();

//    private final ReentrantLock opLock = new ReentrantLock();


    private UniqId() {
        try {
            InetAddress addr = InetAddress.getLocalHost();

            this.hostAddr = addr.getHostAddress();
        }
        catch (IOException e) {
            this.hostAddr = String.valueOf(System.currentTimeMillis());
        }

        if (this.hostAddr==null||hostAddr.isEmpty() || "127.0.0.1".equals(this.hostAddr)) {
            this.hostAddr = String.valueOf(System.currentTimeMillis());
        }


    }


    /**
     * ��ȡUniqIDʵ��
     * 
     * @return UniqId
     */
    public static UniqId getInstance() {
        return me;
    }


    /**
     * ��ò����ظ��ĺ�����
     * 
     * @return
     */
    public long getUniqTime() {
        return this.timer.getCurrentTime();
    }


    /**
     * ���UniqId
     * 
     * @return uniqTime-randomNum-hostAddr-threadId
     */
    public String getUniqID() {
        StringBuffer sb = new StringBuffer();
        long t = this.timer.getCurrentTime();

        sb.append(t);

        sb.append("-");

        sb.append(this.random.nextInt(8999) + 1000);

        sb.append("-");
        sb.append(this.hostAddr);

        sb.append("-");
        sb.append(Thread.currentThread().hashCode());


        return sb.toString();
    }


    /**
     * ��ȡMD5֮���uniqId string
     * 
     * @return uniqId md5 string
     */
    public String getUniqIDHashString() {
        return this.hashString(this.getUniqID());
    }


    /**
     * ��ȡMD5֮���uniqId
     * 
     * @return byte[16]
     */
    public byte[] getUniqIDHash() {
        return this.hash(this.getUniqID());
    }

	private static MessageDigest getHasher() {
		if (mHasher.get() != null) {
			return mHasher.get();
		} else {
			MessageDigest tempHasher = null;
			try {
				tempHasher = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException nex) {
				// FIXME shenxun: in this case , tempHasher will be null , then
				// all the other methods will throw NPE .
				tempHasher = null;
			}
			mHasher.set(tempHasher);
		}
		return mHasher.get();
	}
    /**
     * ���ַ�������md5
     * 
     * @param str
     * @return md5 byte[16]
     */
    public byte[] hash(String str) {
        try {
            byte[] bt = getHasher().digest(str.getBytes("UTF-8"));
            if (null == bt || bt.length != 16) {
                throw new IllegalArgumentException("md5 need");
            }
            return bt;
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException("unsupported utf-8 encoding", e);
        }
    }


    /**
     * �Զ��������ݽ���md5
     * 
     * @param str
     * @return md5 byte[16]
     */
    public byte[] hash(byte[] data) {
            byte[] bt = getHasher().digest(data);
            if (null == bt || bt.length != 16) {
                throw new IllegalArgumentException("md5 need");
            }
            return bt;
    }


    /**
     * ���ַ�������md5 string
     * 
     * @param str
     * @return md5 string
     */
    public String hashString(String str) {
        byte[] bt = this.hash(str);
        return this.bytes2string(bt);
    }


    /**
     * ���ֽ�������md5 string
     * 
     * @param str
     * @return md5 string
     */
    public String hashBytes(byte[] str) {
        byte[] bt = this.hash(str);
        return this.bytes2string(bt);
    }


    /**
     * ��һ���ֽ�����ת��Ϊ�ɼ����ַ���
     * 
     * @param bt
     * @return
     */
    public String bytes2string(byte[] bt) {
        if (bt == null) {
            return null;
        }
        int l = bt.length;

        char[] out = new char[l << 1];

        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = digits[(0xF0 & bt[i]) >>> 4];
            out[j++] = digits[0x0F & bt[i]];
        }
        return new String(out);
    }


    /**
     * ���ַ���ת��Ϊbytes
     * 
     * @param str
     * @return byte[]
     */
    public byte[] string2bytes(String str) {
        if (null == str) {
            throw new NullPointerException("��������Ϊ��");
        }
        if (str.length() != 32) {
            throw new IllegalArgumentException("�ַ������ȱ�����32");
        }
        byte[] data = new byte[16];
        char[] chs = str.toCharArray();
        for (int i = 0; i < 16; ++i) {
            int h = rDigits.get(chs[i * 2]).intValue();
            int l = rDigits.get(chs[i * 2 + 1]).intValue();
            data[i] = (byte) ((h & 0x0F) << 4 | l & 0x0F);
        }
        return data;
    }

    /**
     * ʵ�ֲ��ظ���ʱ��
     * 
     * @author dogun
     */
    private static class UniqTimer {
        private final AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());


        public long getCurrentTime() {
            return this.lastTime.incrementAndGet();
        }
    }
}
