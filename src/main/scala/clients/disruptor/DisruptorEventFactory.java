package clients.disruptor;
import com.lmax.disruptor.EventFactory;

/**
 * @author jchugh
 */
public class DisruptorEventFactory<K, V> implements EventFactory<DisruptorEvent<K, V>> {

    @Override
    public DisruptorEvent<K, V> newInstance() {
        return new DisruptorEvent<>();
    }
}
