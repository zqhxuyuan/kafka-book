package base.concurrent.thread;

import java.util.Date;

/**
 * Created by zhengqh on 17/5/24.
 *
 * kill -3 pid, exit code: 130
 * kill -9 pid, exit code: 137
 */
public class WorkerTaskFinally {

    public static void main(String[] args) throws Exception {
        System.out.println(new Date());

        try {
            WorkerTask workerTask = new WorkerTask();
            workerTask.run();

            // Give WorkerTask enough time to work, during this time, we can kill -9 task (terminate task)
            Thread.sleep(300000);

        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            // 强制kill任务,并不会执行下面的语句. 所以不要在这里做cleanup
            // 即使加入了finally, 也不起作用
            System.out.println("WorkerTask Stopped");
            System.out.println(new Date());
        }
    }
}
