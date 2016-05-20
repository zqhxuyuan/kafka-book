package base.nio.socket;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by zhengqh on 16/5/4.
 */
public class Client {

    public static void main(String[] args) throws Exception {
        SocketChannel sc = SocketChannel.open();
        sc.connect(new InetSocketAddress("localhost", 9999));
        sc.write(ByteBuffer.wrap(new String("I'm a client").getBytes()));
    }
}
