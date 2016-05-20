package base.concurrent.timer;

import java.util.Date;
import java.util.Timer;

/**
 * @author lijinnan
 * @date:2013-11-25 下午3:27:43
 */
public class TimerExceptionTest {

    public static void main(String[] args) {
        System.out.println("start:" + new Date());
        Timer timer = new Timer();
        int delay = 1000;
        int period = 2000;
        timer.schedule(new OKTask(), delay * 2, period);    //"OKTask" does not get chance to execute
        timer.schedule(new ErrorTask(), delay, period);  //exception in "ErrorTask" will terminate the Timer
    }

    /*输出：
    start:Mon Nov 25 17:49:53 CST 2013
    ErrorTask is executing...
    error:Mon Nov 25 17:49:55 CST 2013
    Exception in thread "Timer-0" java.lang.RuntimeException: something wrong
    at com.ljn.timer.ErrorTask.run(ErrorTask.java:14)
     */

}