package base.concurrent.thread;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zhengqh on 17/5/24.
 */
public class WorkerTaskTest {

    public static void main(String[] args) throws Exception {
        System.out.println(new Date());
        WorkerTask workerTask = new WorkerTask();

        workerTask.run();

        // After 5 seconds, stopp WorkerTask
        Thread.sleep(5000);

        workerTask.stopping.set(true);
        System.out.println("WorkerTask Stopped");
        System.out.println(new Date());

        WorkerTaskV2 workerTaskV2 = new WorkerTaskV2();
        workerTaskV2.run();
        Thread.sleep(5000);

        workerTaskV2.stopping = true;
        System.out.println("WorkerTask(V2) Stopped");
        System.out.println(new Date());
    }
}