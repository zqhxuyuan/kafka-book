package base.nio.reactor;

/**
 * Created by zhengqh on 16/5/4.
 */
import java.io.IOException;

public class Server {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int port = 8008;
        new Thread(new ServerReactor(port)).start();
    }
}
