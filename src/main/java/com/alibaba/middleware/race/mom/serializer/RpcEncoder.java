
package com.alibaba.middleware.race.mom.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.nustaq.serialization.FSTConfiguration;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.CharsetUtil;

public class RpcEncoder extends MessageToByteEncoder {

	private FSTConfiguration fst=FSTConfiguration.getDefaultConfiguration();
    public RpcEncoder() {

    }
	@Override
	public void encode(ChannelHandlerContext ctx, Object in, ByteBuf out) throws Exception {
	try{
		//对象
		if (!String.class.isInstance(in)) {
			byte[] data =fst.asByteArray(in);
			//写入内容
			out.writeInt(data.length);
			out.writeBytes(data);
			
		}
		//字符串
		else 
		{
			String data=in.toString();
			out.writeBytes(data.getBytes());
		}
	}catch(Exception e)
	{
		e.printStackTrace();
	}
	}
}
