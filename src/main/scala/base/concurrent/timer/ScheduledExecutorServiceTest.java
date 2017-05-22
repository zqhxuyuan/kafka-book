package base.concurrent.timer;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author lijinnan
 * @date:2013-11-25 下午3:35:39
 */
public class ScheduledExecutorServiceTest {

    public static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);

    public static void main(String[] args){
        System.out.println("start:" + new Date());
        ErrorTask errorTask = new ErrorTask();
        OKTask okTask = new OKTask();
        int delay = 1000;
        int period = 2000;
        scheduledExecutorService.scheduleAtFixedRate(errorTask, delay, period, TimeUnit.MILLISECONDS);   //"ErrorTask" throws Exception and then stopes.
        scheduledExecutorService.scheduleAtFixedRate(okTask, delay * 2, period, TimeUnit.MILLISECONDS);     //"OKTask" is executed periodically, not affected by "ErrorTask"

        //scheduledExecutorService.shutdown();
    }

    /*
    start:Mon Nov 25 17:54:22 CST 2013
    ErrorTask is executing...
    error occurs:Mon Nov 25 17:54:24 CST 2013
    OKTask is executed:Mon Nov 25 17:54:24 CST 2013
    OKTask is executed:Mon Nov 25 17:54:26 CST 2013
    OKTask is executed:Mon Nov 25 17:54:28 CST 2013
     */

}