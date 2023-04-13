package pmn;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

public class CorrectItemDecider implements JobExecutionDecider {

	@Override
	public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

		int nombreAleatoire = (int) (Math.random() * 100) + 1;
		
		String result = nombreAleatoire <= 70 ? "CORRECT" : "INCORRECT";
		System.out.println("CorrectItemDecider result is : " + result);
		
		return new FlowExecutionStatus(result);
	}

}
