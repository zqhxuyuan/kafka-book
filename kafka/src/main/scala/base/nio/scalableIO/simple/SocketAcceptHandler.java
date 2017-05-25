package base.nio.scalableIO.simple;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import static base.nio.scalableIO.Logger.log;

public class SocketAcceptHandler extends SocketHandler{
	
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
			
			socketChannel.register(dispatcher.getReadSelector(), SelectionKey.OP_READ);
			log("Server accept");
		}
	}
}
