package base.concurrent.timer;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by zhengqh on 16/5/17.
 */
public class TestTimerTask {

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new OKTask(), 5000);

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                System.out.println("I run after 5 secounds too.");
            }
        };
        timer.schedule(task, 5000);
    }
}

