package pmn;

import java.util.UUID;

import org.springframework.batch.item.ItemProcessor;

public class TrackedOrderItemProcessor implements ItemProcessor<Order, TrackedOrder> {

	@Override
	public TrackedOrder process(Order item) throws Exception {
		TrackedOrder trackedOrder = new TrackedOrder(item);
		trackedOrder.setTrackingNumber(UUID.randomUUID().toString());
		
		System.out.println("Processing order with id: " + item.getOrderId());
		System.out.println("Processing with thread " + Thread.currentThread().getName());
		
		trackedOrder.setTrackingNumber(this.getTrackingNumber());
		return trackedOrder;
	}

	private String getTrackingNumber() throws OrderProcessingException {

		if (Math.random() < .20) {
			throw new OrderProcessingException();
		}

		return UUID.randomUUID().toString();
	}

}
