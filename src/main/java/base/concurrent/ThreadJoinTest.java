package base.concurrent;

/**
 * Created by zhengqh on 16/5/20.
 */
public class ThreadJoinTest {
    public static void main(String[] args)  {

        Thread thread1 = new Thread() {
            @Override
            public void run() {
                while (true) {
                    System.out.println("first thread...");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        thread1.start();

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                while (true) {
                    System.out.println("sorry, you would't execute any more!");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        thread2.start();
    }
}
