package base.concurrent.thread;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zhengqh on 17/5/24.
 */
public class WorkerTask implements Runnable {

    AtomicBoolean stopping;

    WorkerTask() {
        this.stopping = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        while (stopping.get() != false) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Done WorkerTask...");
    }
}
