package clients.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 执行步骤:
 * 1. 启动ZooKeeper/Kafka
 * 2. 启动NettyServerDemo
 * 3. telnet localhost 9999
 *    模拟输入消息
 *
 */
public class NettyServerDemo {
    public static void main(String[] args) {
        EventLoopGroup masterGroup = new NioEventLoopGroup ();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bs = new ServerBootstrap ();
            bs.group(masterGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler (new ChannelInitializer<SocketChannel> () {
                        protected void initChannel (SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline ().addLast (new DemoServerHandler ());
                        }
                    });
            ChannelFuture cf = bs.bind(9999).sync();
            cf.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            masterGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

}