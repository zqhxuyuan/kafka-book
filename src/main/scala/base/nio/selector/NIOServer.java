package base.nio.selector;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Created by zhengqh on 16/5/4.
 */
public class NIOServer {
    // 通道管理器
    private Selector selector;

    public void initServer(int port) throws Exception {
        // 获得一个ServerSocket通道
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        // 设置通道为 非阻塞
        serverChannel.configureBlocking(false);
        // 将该通道对于的serverSocket绑定到port端口
        serverChannel.socket().bind(new InetSocketAddress(port));
        // 获得一个通道管理器
        this.selector = Selector.open();

        // 将通道管理器和该通道绑定，并为该通道注册selectionKey.OP_ACCEPT事件
        // 注册该事件后，当事件到达的时候，selector.select()会返回，
        // 如果事件没有到达selector.select()会一直阻塞
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    // 采用轮训的方式监听selector上是否有需要处理的事件，如果有，进行处理
    public void listen() throws Exception {
        System.out.println("start server");
        // 轮询访问selector
        while (true) {
            // 当注册事件到达时，方法返回，否则该方法会一直阻塞
            selector.select();
            // 获得selector中选中的相的迭代器，选中的相为注册的事件
            Iterator ite = this.selector.selectedKeys().iterator();
            while (ite.hasNext()) {
                SelectionKey key = (SelectionKey) ite.next();
                // 删除已选的key 以防重负处理
                ite.remove();
                // 客户端请求连接事件
                if (key.isAcceptable()) {
                    ServerSocketChannel server = (ServerSocketChannel) key.channel();
                    // 获得和客户端连接的通道
                    SocketChannel channel = server.accept();
                    // 设置成非阻塞
                    channel.configureBlocking(false);
                    // 在这里可以发送消息给客户端
                    channel.write(ByteBuffer.wrap(new String("hello client").getBytes()));
                    // 在客户端 连接成功之后，为了可以接收到客户端的信息，需要给通道设置读的权限
                    channel.register(this.selector, SelectionKey.OP_READ);
                } else if (key.isReadable()) {
                    // 获得了可读的事件
                    read(key);

                } else if (key.isWritable()) {
                    write(key);
                }
            }
        }
    }

    // 处理 读取客户端发来的信息事件
    private void read(SelectionKey key) throws Exception {
        // 服务器可读消息，得到事件发生的socket通道
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        channel.read(buffer);
        byte[] data = buffer.array();
        String msg = new String(data).trim();
        System.out.println("server <- client: " + msg);

        SelectionKey writeKey = channel.register(this.selector, SelectionKey.OP_WRITE);
        writeKey.attach("Server:"+msg);
        Thread.sleep(1000);
    }

    private void write(SelectionKey key) throws Exception {
        SocketChannel channel = (SocketChannel) key.channel();
        String result = (String)key.attachment();
        System.out.println("Server--"+result);

        ByteBuffer outBuffer = ByteBuffer.wrap(result.getBytes());
        channel.write(outBuffer);
        Thread.sleep(1000);
    }

    public static void main(String[] args) throws Throwable {
        NIOServer server = new NIOServer();
        server.initServer(8989);
        server.listen();
    }
}
