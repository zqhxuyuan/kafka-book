package base.streams;

import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Created by zhengqh on 16/6/16.
 */
public class Solution {

    public static void main(String[] args) {
        long result = new Solution().fibonacci(10);
        System.out.println(result);
    }

    public int fibonacci(int n) {
        Stream<Integer> fibonacci = Stream.generate(new FibonacciSupplier());
        //fibonacci.limit(10).forEach(System.out::println);
        int result = fibonacci.skip(n-2).limit(1).findFirst().get();
        return result;
    }
}

class FibonacciSupplier implements Supplier<Integer> {

    int a = 0;
    int b = 1;

    @Override
    public Integer get() {
        int x = a + b;
        a = b;
        b = x;
        return a;
    }
}
