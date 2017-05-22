package base.nio.scalableIO.simple;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.spi.SelectorProvider;

public class ServerDispatcher {
	
	private ServerSocketChannel ssc;
	private Selector[] selectors = new Selector[3];

	public ServerDispatcher(ServerSocketChannel ssc, SelectorProvider selectorProvider) throws IOException {
		this.ssc = ssc;
		for (int i = 0; i < 3; i++) {
			selectors[i] = selectorProvider.openSelector();
		}
	}

	public Selector getAcceptSelector() {
		return selectors[0];
	}

	public Selector getReadSelector() {
		return selectors[1];
	}

	public Selector getWriteSelector() {
		return selectors[1];
	}

	public void execute() throws IOException {
		SocketHandler r = new SocketAcceptHandler(this, ssc, getAcceptSelector());
		new Thread(r).start();

		r = new SocketReadHandler(this, ssc, getReadSelector());
		new Thread(r).start();

		r = new SocketWriteHandler(this, ssc, getWriteSelector());
		new Thread(r).start();
	}

}
