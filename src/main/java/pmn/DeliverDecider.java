package pmn;

import java.time.LocalDateTime;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

public class DeliverDecider implements JobExecutionDecider {

	@Override
	public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

		String result = LocalDateTime.now().getHour() > 12 ? "NOT_PRESENT" : "PRESENT";
		System.out.println("Decider result is : " + result);
		return new FlowExecutionStatus(result);
	}
}
