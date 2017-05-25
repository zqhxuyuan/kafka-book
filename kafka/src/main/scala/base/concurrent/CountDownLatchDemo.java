package base.concurrent;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * http://my.oschina.net/itblog/blog/516282
 */
class TaskPortion implements Runnable {
    private static int counter = 0;
    private final int id = counter++;
    private static Random random = new Random();
    private final CountDownLatch latch;
    public TaskPortion(CountDownLatch latch) {
        this.latch = latch;
    }
    @Override
    public void run() {
        try {
            doWork();
            latch.countDown();//普通任务执行完后，调用countDown()方法，减少count的值
            System.out.println(this + " completed. count=" + latch.getCount());
        } catch (InterruptedException e) {

        }
    }

    public void doWork() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(random.nextInt(2000));
    }

    @Override
    public String toString() {
        return String.format("%1$-2d ", id);
    }
}

class WaitingTask implements Runnable {
    private static int counter = 0;
    private final int id = counter++;
    private final CountDownLatch latch;
    public WaitingTask(CountDownLatch latch) {
        this.latch = latch;
    }
    @Override
    public void run() {
        try {
            //这些后续任务需要等到之前的任务都执行完成后才能执行，即count=0时
            latch.await();
            System.out.println("Latch barrier passed for " + this);
        } catch (InterruptedException e) {
            System.out.println(this + " interrupted.");
        }
    }

    @Override
    public String toString() {
        return String.format("WaitingTask %1$-2d ", id);
    }
}

public class CountDownLatchDemo {
    static final int SIZE = 10;
    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(SIZE);
        ExecutorService exec = Executors.newCachedThreadPool();
        //10个WaitingTask
        for (int i = 0; i < 5; i++) {
            exec.execute(new WaitingTask(latch));
        }
        //100个任务，这100个任务要先执行才会执行WaitingTask
        for (int i = 0; i < SIZE; i++) {
            exec.execute(new TaskPortion(latch));
        }
        System.out.println("Launched all tasks.");
        exec.shutdown();//当所有的任务都结束时，关闭exec
    }
}