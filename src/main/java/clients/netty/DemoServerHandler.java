package clients.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;


public class DemoServerHandler extends ChannelInboundHandlerAdapter { // (1)

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
        System.out.println("read data:");
        try {
            ByteBuf buf = (ByteBuf) msg;

            byte[] req = new byte[buf.readableBytes()];
            buf.readBytes(req);
            String body = new String(req,"UTF-8");
            System.out.println ("body:"+body);
            KafKaProducerDemo.inBoundKafka ("netty","netty-key",body);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        System.out.println("exception xxx");
        cause.printStackTrace();
        ctx.close();
    }
}