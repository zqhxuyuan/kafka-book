package base.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * http://my.oschina.net/itblog/blog/515196
 */

//菜肴(消息)
class Meal {
    private final int orderNum;
    public Meal(int orderNum) {
        this.orderNum = orderNum;
    }
    @Override
    public String toString() {
        return "Meal " + orderNum;
    }
}

//服务员(消费者sub)
class Waiter implements Runnable {
    private Restaurant r;
    public Waiter(Restaurant r) {
        this.r = r;
    }
    @Override
    public void run() {
        try {
            while(!Thread.interrupted()) {
                synchronized (this) {
                    while(r.meal == null) {
                        wait();//等待厨师做菜
                    }
                }
                System.out.println("Waiter got " + r.meal);
                synchronized (r.chef) {
                    r.meal = null;//上菜
                    r.chef.notifyAll();//通知厨师继续做菜
                }
            }
        } catch (InterruptedException e) {
            System.out.println("Waiter task is over.");
        }
    }
}

//厨师(生产者pub)
class Chef implements Runnable {
    private Restaurant r;
    private int count = 0;//厨师做的菜品数量
    public Chef(Restaurant r) {
        this.r = r;
    }
    @Override
    public void run() {
        try {
            while(!Thread.interrupted()) {
                synchronized (this) {
                    while(r.meal != null) {
                        wait();//等待服务员上菜
                    }
                }
                if (++count > 10) {
                    System.out.println("Meal is enough, stop.");
                    r.exec.shutdownNow();
                }
                System.out.print("Order up! ");
                synchronized (r.waiter) {
                    r.meal = new Meal(count);//做菜
                    r.waiter.notifyAll();//通知服务员上菜
                }
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } catch (InterruptedException e) {
            System.out.println("Chef task is over.");
        }
    }
}

//餐厅
public class Restaurant {
    Meal meal;
    ExecutorService exec = Executors.newCachedThreadPool();
    //厨师和服务员都服务于同一个饭店
    Waiter waiter = new Waiter(this);
    Chef chef = new Chef(this);
    public Restaurant() {
        exec.execute(waiter);
        exec.execute(chef);
    }
    public static void main(String[] args) {
        new Restaurant();
    }
}
