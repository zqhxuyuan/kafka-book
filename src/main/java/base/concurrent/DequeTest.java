package base.concurrent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by zhengqh on 16/7/12.
 */
public class DequeTest {

    public static void main(String[] args) {
        Deque<Integer> deque = new LinkedBlockingDeque<Integer>();
        deque.add(1);
        deque.add(2);
        deque.add(3);

        for(Integer i : deque) {
            System.out.println(i);
        }

        Integer last = deque.removeFirst();
        System.out.println(last);
    }
}
