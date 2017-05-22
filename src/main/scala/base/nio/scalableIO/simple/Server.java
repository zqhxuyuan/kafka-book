package base.nio.scalableIO.simple;

import java.io.IOException;

public class Server {
	
	public static void main(String[] args) throws IOException {
		new Thread(new ServerReactor(9003)).start();
	}
	
}
