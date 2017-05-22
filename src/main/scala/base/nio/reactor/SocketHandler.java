package base.nio.reactor;

/**
 * Created by zhengqh on 16/5/4.
 */

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class SocketHandler implements Runnable{
    protected Selector selector;
    protected SocketChannel socketChannel = null;
    protected ServerSocketChannel serverSocketChannel;
    protected ServerDispatcher dispatcher;
    protected final static Logger logger = Logger.getLogger(SocketHandler.class);
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();;
    public SocketHandler(ServerDispatcher dispatcher, ServerSocketChannel sc, Selector selector) throws IOException{
        this.selector = selector;
        this.serverSocketChannel = sc;
        this.dispatcher = dispatcher;
    }

    public abstract void runnerExecute(int readyKeyOps) throws IOException;

    public final void run()
    {
        while(true)
        {
            readWriteLock.readLock().lock();
            try {
                int keyOps = this.Select();

                runnerExecute(keyOps);

                Thread.sleep(1000);
            } catch (Exception e) {
                // TODO: handle exception
                logger.debug(e.getMessage());
            }
            finally {
                readWriteLock.readLock().unlock();
            }
        }
    }

    private int Select() throws IOException
    {
        int keyOps = this.selector.selectNow();

        boolean flag = keyOps > 0;

        if (flag)
        {
            Set<SelectionKey >readyKeySet = selector.selectedKeys();
            Iterator<SelectionKey> iterator = readyKeySet.iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();
                keyOps = key.readyOps();
                if (keyOps == SelectionKey.OP_READ || keyOps == SelectionKey.OP_WRITE)
                {
                    socketChannel = (SocketChannel)key.channel();
                    socketChannel.configureBlocking(false);
                }
            }
        }

        return keyOps;
    }
}