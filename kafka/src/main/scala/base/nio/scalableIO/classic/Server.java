package base.nio.scalableIO.classic;

import java.io.IOException;
import java.net.ServerSocket;

public class Server implements Runnable{

	private final int port;
	private ServerSocket serverSocket;
	
	public Server(int port){
		this.port = port;
		try {
			this.serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run(){
		try {
			while (!Thread.interrupted()) {
				new Thread(new Handler(serverSocket.accept())).start();
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public int getPort() {
		return port;
	}
	
	public static void main(String[] args) {
		new Thread(new Server(9001)).start();
	}
	
}
