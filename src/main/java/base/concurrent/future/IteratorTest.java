package base.concurrent.future;

import java.util.*;

/**
 * Created by zhengqh on 16/6/9.
 */
public class IteratorTest {

    public static void main(String[] args) {
        List<String> list = new ArrayList(){{
            add("a");
            add("b");
            add("c");
        }};
        Iterator iterator = list.iterator();
        while(iterator.hasNext()) {
            System.out.println(iterator.next());;
        }

        Map<String,String> map = new HashMap(){{
            put("a","a");
            put("b","b");
            put("c","c");
        }};
        Iterator<Map.Entry<String,String>> mapIterator = map.entrySet().iterator();
        while(mapIterator.hasNext()) {
            Map.Entry<String,String> entry = mapIterator.next();
            String key = entry.getKey();
            String value = entry.getValue();
            System.out.println("key:" + key + ",value:" + value);
        }
    }
}
