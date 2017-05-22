package base.concurrent.timer;

import java.util.Date;
import java.util.TimerTask;


public  class OKTask extends TimerTask {

    @Override
    public void run() {
        System.out.println("OKTask is executed:" + new Date());
    }
}