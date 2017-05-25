package base.concurrent.thread;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zhengqh on 17/5/24.
 */
public class WorkerTaskV2 implements Runnable {

    volatile boolean stopping;

    WorkerTaskV2() {
        this.stopping = false;
    }

    @Override
    public void run() {
        while (stopping != false) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Done WorkerTask...");
    }
}
