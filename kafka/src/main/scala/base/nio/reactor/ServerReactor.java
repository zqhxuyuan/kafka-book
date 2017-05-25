package base.nio.reactor;

/**
 * Created by zhengqh on 16/5/4.
 */

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.spi.SelectorProvider;

public class ServerReactor implements Runnable {
    static Logger logger = Logger.getLogger(ServerReactor.class);
    private SelectorProvider selectorProvider = SelectorProvider.provider();
    private ServerSocketChannel serverSocketChannel;
    public ServerReactor(int port) throws IOException {
        serverSocketChannel = selectorProvider.openServerSocketChannel(); //ServerSocketChannel.open();
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.bind(new InetSocketAddress("localhost", port), 1024);
        serverSocketChannel.configureBlocking(false);
        logger.debug(String.format("Server : Server Start.----%d", port));
    }

    public void run() {
        try {
            new ServerDispatcher(serverSocketChannel, SelectorProvider.provider()).execute();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}