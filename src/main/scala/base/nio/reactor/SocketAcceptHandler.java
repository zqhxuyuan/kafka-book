package base.nio.reactor;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

/**
 * Created by zhengqh on 16/5/4.
 */

public class SocketAcceptHandler extends SocketHandler{
    private static final Logger logger = Logger.getLogger(SocketAcceptHandler.class);

    public SocketAcceptHandler(ServerDispatcher dispatcher, ServerSocketChannel sc, Selector selector)
            throws IOException {
        super(dispatcher, sc, selector);
        serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT, this);
    }

    @Override
    public void runnerExecute(int readyKeyOps) throws IOException {
        // TODO Auto-generated method stub
        if (readyKeyOps == SelectionKey.OP_ACCEPT)
        {
            socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            logger.debug("Server accept");

            socketChannel.register(dispatcher.getReadSelector(), SelectionKey.OP_READ);
        }
    }
}