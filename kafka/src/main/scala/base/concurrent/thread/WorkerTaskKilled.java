package base.concurrent.thread;

import java.util.Date;

/**
 * Created by zhengqh on 17/5/24.
 */
public class WorkerTaskKilled {

    public static void main(String[] args) throws Exception {
        System.out.println(new Date());

        // 强制kill任务, 通过shutdownHook,可以执行cleanup
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("WorkerTask Stopped By Hooked");
                System.out.println(new Date());
            }
        });

        WorkerTask workerTask = new WorkerTask();
        workerTask.run();

        // Give WorkerTask enough time to work, during this time, we can kill -9 task (terminate task)
        Thread.sleep(300000);

        // 强制kill任务,并不会执行下面的语句. 所以不要在这里做cleanup
        System.out.println("WorkerTask Stopped");
        System.out.println(new Date());
    }
}
