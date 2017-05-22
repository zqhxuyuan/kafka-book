package clients.disruptor;

import com.lmax.disruptor.EventHandler;

/**
 * @author jchugh
 */
public class DisruptorEventHandler implements EventHandler<DisruptorEvent<String, String>> {

    @Override
    public void onEvent(DisruptorEvent<String, String> disruptorEvent, long sequence, boolean endOfBatch) throws Exception {
        System.out.println("Disruptor: " + disruptorEvent.getConsumerRecord().partition());
    }
}
