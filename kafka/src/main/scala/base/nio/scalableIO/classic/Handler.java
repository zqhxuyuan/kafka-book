package base.nio.scalableIO.classic;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class Handler implements Runnable{

	private final Socket clientSocket;
    int BUF_SIZE = 1024;
	
	public Handler(Socket clientSocket){
		this.clientSocket = clientSocket;
	}
	
	@Override
	public void run() {
		int readSize;
		byte[] readBuf = new byte[BUF_SIZE];
		try {
			InputStream in = clientSocket.getInputStream();
			OutputStream out = clientSocket.getOutputStream();
			while ((readSize = in.read(readBuf)) != -1) {
				out.write(readBuf, 0, readSize);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
