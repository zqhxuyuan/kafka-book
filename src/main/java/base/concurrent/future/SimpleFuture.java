package base.concurrent.future;

import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by zhengqh on 16/7/3.
 */
public class SimpleFuture {

    public static void main(String[] args) throws Exception{
future2();
    }

    public static void future1() throws Exception{
        Callable callable = new Callable() {
            public Integer call() throws Exception {
                Thread.sleep(3000);
                return new Random().nextInt(100);
            }
        };
        FutureTask future = new FutureTask(callable);
        Thread t = new Thread(future);
        t.start();

        System.out.println(future.get());
        System.out.println("main thread");
    }

    //和future1的区别是,执行该方法后,并不会停止应用程序
    public static void future2() throws Exception {
        ExecutorService threadPool = Executors.newSingleThreadExecutor();
        Future<Integer> future = threadPool.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Thread.sleep(3000);
                return new Random().nextInt(100);
            }
        });

        System.out.println(future.get());
        System.out.println("main thread");
    }

    public static void future3() throws Exception {
        ExecutorService threadPool = Executors.newSingleThreadExecutor();
        Future<Integer> future = threadPool.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Thread.sleep(3000);
                return new Random().nextInt(100);
            }
        });

        //添加了等待,不过future.get本身就会阻塞,所以这里有点多此一举
        while(!future.isDone()) {
            future.wait();
        }
        System.out.println(future.get());
        System.out.println("main thread");
    }

}
