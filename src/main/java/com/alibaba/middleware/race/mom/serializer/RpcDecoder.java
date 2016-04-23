package com.alibaba.middleware.race.mom.serializer;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;

import org.nustaq.serialization.FSTConfiguration;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class RpcDecoder extends ByteToMessageDecoder {

   
	private FSTConfiguration fst=FSTConfiguration.getDefaultConfiguration();
    @Override
    public final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      try{
    	if (in.readableBytes() < 4) {
            return;
        }
        in.markReaderIndex();
        int dataLength = in.readInt();
        if (dataLength < 0) {
            ctx.close();
        }
        if (in.readableBytes() < dataLength) {
            in.resetReaderIndex();
            return ;
        }
        byte[] data = new byte[dataLength];
        in.readBytes(data);
        out.add(fst.asObject(data));
        
      }catch(Exception e)
      {
    	  e.printStackTrace();
      }
     
    }
}



