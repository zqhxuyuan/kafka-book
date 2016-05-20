package base.concurrent.timer;

import java.util.Date;
import java.util.TimerTask;

public class ErrorTask extends TimerTask {

    @Override
    public void run() {
        try {
            System.out.println("ErrorTask is executing...");
            Thread.sleep(1000);
            System.out.println("error occurs:" + new Date());
            throw new RuntimeException("something wrong");
        } catch (InterruptedException e) {
        }
    }

}