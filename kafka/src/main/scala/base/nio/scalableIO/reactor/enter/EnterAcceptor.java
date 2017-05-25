package base.nio.scalableIO.reactor.enter;

import base.nio.scalableIO.reactor.Acceptor;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class EnterAcceptor extends Acceptor {

	EnterAcceptor(Selector selector, ServerSocketChannel serverChannel, boolean useMultipleReactors){
		super(selector, serverChannel, useMultipleReactors);
	}
	
	@Override
	public void handle(Selector selector, SocketChannel clientChannel) {
		new EnterHandler(selector, clientChannel).run();
	}

}
