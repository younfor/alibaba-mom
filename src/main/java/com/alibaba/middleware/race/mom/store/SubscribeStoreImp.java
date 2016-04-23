package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import com.alibaba.fastjson.JSON;
import com.alibaba.middleware.race.mom.broker.function.Offset;

public class SubscribeStoreImp implements SubscribeStore {

	@Override
	public ArrayList<Offset> read() {
		// TODO Auto-generated method stub
		ArrayList<Offset> rs = new ArrayList<Offset>();
		RandomAccessFile subIndexFile = getSubFile(StoreConfig.subIndexFile, false);
		RandomAccessFile subDataFile = getSubFile(StoreConfig.subDataFile, false);
		try {
			long indexLength = subIndexFile.length();
			long index = 0;
			long dataPos = 0;
			int dataSize = 0;
			byte[] subByte;
			while (index < indexLength) {
				subIndexFile.seek(index);
				dataPos = subIndexFile.readLong();
				dataSize = subIndexFile.readInt();
				subDataFile.seek(dataPos);
				subByte = new byte[dataSize];
				subDataFile.read(subByte);
				rs.add(((Offset) JSON.parseObject(subByte, Offset.class)));
				index += 16;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return rs;
		}finally{
			try {
				subIndexFile.close();
				subDataFile.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}
		return rs;
	}

	@Override
	public boolean write(ArrayList<Offset> subInfoList) {
		// TODO Auto-generated method stub
		RandomAccessFile subIndexFile = getSubFile(StoreConfig.subIndexFile, true);
		RandomAccessFile subDataFile = getSubFile(StoreConfig.subDataFile, true);
		try {
			long dataPos = 0;
			long indexPos = 0;

			// 每次重头写
			for (Offset sb : subInfoList) {
				indexPos = subIndexFile.length();
				dataPos = subDataFile.length();
				// 写入index
				subIndexFile.seek(indexPos);
				subIndexFile.writeLong(dataPos);
				byte[] sbByte = JSON.toJSONBytes(sb);
				subIndexFile.writeInt(sbByte.length);
				subIndexFile.writeInt(0);
				// 写入data
				subDataFile.seek(dataPos);
				subDataFile.write(sbByte);
				System.out.println("indexpos,datapos=" + subIndexFile.length() + "," + subDataFile.length());
			}

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}finally{
			try {
				subIndexFile.close();
				subDataFile.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}
		return true;
	}

	public RandomAccessFile getSubFile(String fileName, boolean isWriteMethod) {
		String file;
		RandomAccessFile raf = null;
		file = StoreConfig.baseDir + StoreConfig.subscribeDir + fileName;
		System.out.println("file=" + file);
		File f = new File(file);
		if (isWriteMethod) {
			try {
				if (!f.exists()) {
					if (!f.getParentFile().exists()) {
						f.getParentFile().mkdirs();
					}
					f.createNewFile();
				} else {
					// 删除老文件
					f.delete();
					f.createNewFile();
				}
				raf = new RandomAccessFile(f, "rw");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				if (!f.exists()) {
					if (!f.getParentFile().exists()) {
						f.getParentFile().mkdirs();
					}
					f.createNewFile();
				}
				raf = new RandomAccessFile(f, "rw");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return raf;
	}

}
