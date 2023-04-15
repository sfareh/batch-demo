package pmn;

import java.util.UUID;

import org.springframework.batch.item.ItemProcessor;

public class TrackedOrderItemProcessor implements ItemProcessor<Order, TrackedOrder> {

	@Override
	public TrackedOrder process(Order item) throws Exception {
		TrackedOrder trackedOrder = new TrackedOrder(item);
		trackedOrder.setTrackingNumber(UUID.randomUUID().toString());
		return trackedOrder;
	}

	private String getTrackingNumber() throws OrderProcessingException {

		if (Math.random() < .03) {
			throw new OrderProcessingException();
		}

		return UUID.randomUUID().toString();
	}

}
