package base.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by zhengqh on 16/6/28.
 */
public class ListIterator {

    public static void main(String[] args) {
        //ArrayList会报错
        List<Integer> list = new CopyOnWriteArrayList(){{
            for(int i=0;i<20;i++) add(i);
        }};

        Iterator iterator = list.iterator();

        //ConcurrentModificationException
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for(int i=20;i<30;i++) list.add(i);
            }
        });
        t.start();

        //即使有新的线程加入到list中,但是Iterator迭代器使用的还是旧的
        //所以只会打印出0-19,不会打印新加入list的20-29

        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }
}
