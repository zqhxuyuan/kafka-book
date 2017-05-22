package base.nio.scalableIO.reactor.echo;

import base.nio.scalableIO.reactor.Handler;

import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class EchoHandler extends Handler {

	EchoHandler(Selector selector, SocketChannel clientChannel){
		super(selector, clientChannel);
	}
	
	@Override
	public int byteBufferSize() {
		return 1;
	}
	
	@Override
	public boolean readIsComplete() {
		return readData.length() > 0;
	}
	
	@Override
	public boolean writeIsComplete() {
		return !writeBuf.hasRemaining();
	}

}
