package base.concurrent;

/**
 * Created by zhengqh on 16/5/20.
 */
public class ThreadJoinTest {
    public static void main(String[] args) throws Exception {

        Thread thread1 = new Thread() {
            @Override
            public void run() {
                while (true) {
                    System.out.println("thread1...");
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
                    System.out.println("thread2...");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        thread2.start();

        //确保要一起加入的线程,都准备好后,才执行join之后的逻辑
        //如果没有join,线程的执行时间是不确定的
        thread1.join();
        thread2.join();

        System.out.println("Main thread...");

    }
}
