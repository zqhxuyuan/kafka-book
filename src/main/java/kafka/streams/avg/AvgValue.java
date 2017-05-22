package kafka.streams.avg;

/**
 * Created by zhengqh on 16/10/28.
 */
public class AvgValue {
    Integer count;
    Integer sum;

    public AvgValue(Integer count, Integer sum) {
        this.count = count;
        this.sum = sum;
    }

}
