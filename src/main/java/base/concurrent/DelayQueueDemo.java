package base.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * http://my.oschina.net/itblog/blog/516670
 */
class DelayedTask implements Runnable, Delayed {

    private static int counter = 0;
    protected static List<DelayedTask> sequence = new ArrayList<>();
    private final int id = counter++;
    private final int delayTime;
    private final long triggerTime;

    public DelayedTask(int delayInMillis) {
        delayTime = delayInMillis;
        triggerTime = System.nanoTime() + NANOSECONDS.convert(delayTime, MILLISECONDS);
        sequence.add(this);
    }

    @Override
    public int compareTo(Delayed o) {
        DelayedTask that = (DelayedTask)o;
        if (triggerTime < that.triggerTime) return -1;
        if (triggerTime > that.triggerTime) return 1;
        return 0;
    }

    /**
     * 剩余的延迟时间
     */
    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(triggerTime - System.nanoTime(), NANOSECONDS);
    }

    @Override
    public void run() {
        System.out.println(this + " ");
    }

    @Override
    public String toString() {
        return String.format("[%1$-4d]", delayTime) + " Task " + id;
    }

    public static class EndSentinel extends DelayedTask {
        private ExecutorService exec;
        CountDownLatch shutdownLatch = null;
        public EndSentinel(int delay, ExecutorService exec, CountDownLatch shutdownLatch) {
            super(delay);
            this.exec = exec;
            this.shutdownLatch = shutdownLatch;
        }
        @Override
        public void run() {
            System.out.println(this + " calling shutDownNow()");
            shutdownLatch.countDown();
            exec.shutdownNow();
        }
    }
}

class DelayedTaskConsumer implements Runnable {
    private DelayQueue<DelayedTask> tasks;
    public DelayedTaskConsumer(DelayQueue<DelayedTask> tasks) {
        this.tasks = tasks;
    }
    @Override
    public void run() {
        try {
            while(!Thread.interrupted()) {
                tasks.take().run();//run tasks with current thread.
            }
        } catch (InterruptedException e) {
            // TODO: handle exception
        }
        System.out.println("Finished DelaytedTaskConsumer.");
    }
}


public class DelayQueueDemo {
    public static void main(String[] args) throws Exception{
        long start = System.currentTimeMillis();
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        int maxDelayTime = 5000;//milliseconds
        Random random = new Random(47);
        ExecutorService exec = Executors.newCachedThreadPool();
        DelayQueue<DelayedTask> queue = new DelayQueue<>();
        //填充10个休眠时间随机的任务
        for (int i = 0; i < 10000; i++) {
            queue.put(new DelayedTask(random.nextInt(maxDelayTime)));
        }
        //设置结束的时候。
        queue.add(new DelayedTask.EndSentinel(maxDelayTime, exec, shutdownLatch));
        exec.execute(new DelayedTaskConsumer(queue));

        shutdownLatch.await();
        long end = System.currentTimeMillis();
        System.out.println("cost:" + (end-start));
    }
}