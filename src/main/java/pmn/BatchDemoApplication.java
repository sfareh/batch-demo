package pmn;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableBatchProcessing
public class BatchDemoApplication {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public JobExecutionDecider decider() {
		return new DeliverDecider();
	}
	
	@Bean
	public JobExecutionDecider deciderIsCorrectItem() {
		return new CorrectItemDecider();
	}
	
	@Bean
    public StepExecutionListener selectFlowersListener() {
		return new FlowersSelectionStepExecutionListener();
	}
	
	@Bean
    public Step selectFlowersStep() {
        return this.stepBuilderFactory.get("selectFlowersStep").tasklet(new Tasklet() {

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Gathering flowers for order.");
                return RepeatStatus.FINISHED; 
            }
            
        }).listener(selectFlowersListener()).build();
    }

    @Bean
    public Step removeThornsStep() {
        return this.stepBuilderFactory.get("removeThornsStep").tasklet(new Tasklet() {

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Remove thorns from roses.");
                return RepeatStatus.FINISHED; 
            }
            
        }).build();
    }
    
    @Bean
    public Step arrangeFlowersStep() {
        return this.stepBuilderFactory.get("arrangeFlowersStep").tasklet(new Tasklet() {

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Arranging flowers for order.");
                return RepeatStatus.FINISHED; 
            }
            
        }).build();
    }
    
	
	 @Bean
	    public Job prepareFlowers() {
	        return this.jobBuilderFactory.get("prepareFlowersJob")
	        		.start(selectFlowersStep())
	        			.on("TRIM_REQUIRED").to(removeThornsStep()).next(arrangeFlowersStep())
	        		.from(selectFlowersStep())
	        			.on("NO_TRIM_REQUIRED").to(arrangeFlowersStep())
	        		.end()
	        		.build();
	    }
	
	@Bean
	public Step refundCustomerStep() {
		return this.stepBuilderFactory.get("refundCustomerStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

				System.out.println("Refund the customer Step");
				return RepeatStatus.FINISHED;
			}

		}).build();
	}
	
	@Bean
	public Step thankCustomerStep() {
		return this.stepBuilderFactory.get("thankCustomerStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

				System.out.println("thank the customer Step");
				return RepeatStatus.FINISHED;
			}

		}).build();
	}

	@Bean
	public Step leaveAtDoorStep() {
		return this.stepBuilderFactory.get("leaveAtDoorStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

				System.out.println("Leaving package at the door");
				return RepeatStatus.FINISHED;
			}

		}).build();
	}

	@Bean
	public Step storePackageStep() {
		return this.stepBuilderFactory.get("storePackageStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

				System.out.println("Store the package while the costumer adress is located");
				return RepeatStatus.FINISHED;
			}

		}).build();
	}

	@Bean
	public Step givePackageToCustomerStep() {
		return this.stepBuilderFactory.get("givePackageToCustomerStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

				System.out.println("Successfully given the package to the costumer");
				return RepeatStatus.FINISHED;
			}

		}).build();
	}

	@Bean
	public Step driveToAdressStep() {

		boolean GOT_LOST = false;

		return this.stepBuilderFactory.get("driveToAdressStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

				if (GOT_LOST) {
					throw new RuntimeException("Got lost driving to the adress");
				}
				System.out.println("Successfully arrived at the adress");
				return RepeatStatus.FINISHED;
			}

		}).build();
	}

	@Bean
	public Step packageItemStep() {
		return this.stepBuilderFactory.get("packageItemStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

				String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
				String date = chunkContext.getStepContext().getJobParameters().get("run.date").toString();

				System.out.println("This item has been package");
				System.out.println(String.format("The %s has been packaged on %s.", item, date));

				return RepeatStatus.FINISHED;
			}

		}).build();
	}

	@Bean
	public Job deliverPackageJob() {
		return this.jobBuilderFactory.get("deliverPackageJob")
				.start(packageItemStep())
				.next(driveToAdressStep())
					.on("FAILED").to(storePackageStep())
				.from(driveToAdressStep())
					.on("*").to(decider())
						.on("PRESENT").to(givePackageToCustomerStep())
							.on("*").to(deciderIsCorrectItem())
								.on("CORRECT").to(thankCustomerStep())
							.from(deciderIsCorrectItem())
								.on("INCORRECT").to(refundCustomerStep())
					.from(decider())
						.on("NOT_PRESENT").to(leaveAtDoorStep())
					
				.end().build();
	}

	public static void main(String[] args) {
		SpringApplication.run(BatchDemoApplication.class, args);
	}

}
