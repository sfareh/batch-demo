package pmn;


import java.time.LocalDateTime;
import java.util.List;

import javax.sql.DataSource;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.quartz.QuartzJobBean;



@SpringBootApplication
@EnableBatchProcessing
@EnableScheduling
public class BatchDemoApplication extends QuartzJobBean{
	
	public static String[] tokens = new String[] {"order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"};

	public static String ORDER_SQL = "select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date "
			+ "from SHIPPED_ORDER order by order_id";
	
	public static String[] names = new String[] { "orderId", "firstName", "lastName", "email", "cost", "itemId",
			"itemName", "shipDate" };
	
	public static String INSERT_ORDER_SQL = "insert into "
			+ "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date)"
			+ " values(?,?,?,?,?,?,?,?)";
	
	public static String INSERT_ORDER_SQL_BIS = "insert into "
			+ "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date)"
			+ " values(:orderId,:firstName,:lastName,:email,:itemId,:itemName,:cost,:shipDate)";
	
	@Autowired
	public ResourceLoader resourceLoader;
	
	@Autowired
	public DataSource datasource;

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public JobLauncher jobLauncher;
	
	@Autowired
	public JobExplorer jobExplorer;
	
	
	/* Scheduling with Quartz */
	
	@Bean
	public Trigger trigger() {
		SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder
				.simpleSchedule()
				.withIntervalInSeconds(30)
				.repeatForever();
		
		return TriggerBuilder.newTrigger()
				.forJob(jobDetail())
				.withSchedule(scheduleBuilder)
				.build();
	}
	
	@Bean
	public JobDetail jobDetail() {
		return JobBuilder.newJob(BatchDemoApplication.class)
				.storeDurably()
				.build();
	}
	
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {

		JobParameters parameters = new JobParametersBuilder(jobExplorer)
				.getNextJobParameters(jobQuartz())
				.toJobParameters();
		
		try {
			
			this.jobLauncher.run(jobQuartz(), parameters);
			
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	@Bean
	public Step stepQuartz() {
		return this.stepBuilderFactory.get("stepQuartz").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("The run time is: " + LocalDateTime.now());
				return RepeatStatus.FINISHED;
			}
		}).build();

	}

	@Bean
	public Job jobQuartz() {
		return this.jobBuilderFactory.get("jobQuartz").incrementer(new RunIdIncrementer()).start(stepQuartz()).build();
	}
	
	/* Scheduling with Spring */
	
	/*
	
	@Scheduled(cron = "0/30 * * * * *")
	public void runJob() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException, Exception {
		JobParametersBuilder paramBuilder = new JobParametersBuilder();
		paramBuilder.addDate("runTime", new Date());
		this.jobLauncher.run(jobScheduling(), paramBuilder.toJobParameters());
	}

	@Bean
	public Step stepScheduling() throws Exception {
		return this.stepBuilderFactory.get("stepScheduling").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("The run time is: " + LocalDateTime.now());
				return RepeatStatus.FINISHED;
			}
		}).build();

	}

	@Bean
	public Job jobScheduling() throws Exception {
		return this.jobBuilderFactory.get("jobScheduling").start(stepScheduling()).build();
	}
	*/
	
	
/* CHUNK FOURTEEN : MULTI PROCESOR  +  RETRY LOGIC + MULTI THREAD JOB */
	
	@Bean
	public ItemReader<Order> itemReaderMultiThread() throws Exception {
		
		return new JdbcPagingItemReaderBuilder<Order>()
				.dataSource(this.datasource)
				.name("jdbcCursorItemReader")
				.queryProvider(queryProvider())
				.rowMapper(new OrderRowMapper())
				.pageSize(10)
				.saveState(false)
				.build();
		}
	
	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(2);
		executor.setMaxPoolSize(10);
		return executor;
	}
	
	
	@Bean
	public ItemWriter<TrackedOrder> itemWriterMultiProcessorMultiThread() {
		return new JdbcBatchItemWriterBuilder<TrackedOrder>()
				.dataSource(datasource)
				.sql(INSERT_ORDER_SQL_BIS)
				.beanMapped()
				.build();
	}
	
	@Bean
	public Step chunkMultiProcessorvMultiThreadStep() throws Exception {
		return this.stepBuilderFactory.get("chunkMultiProcessorvMultiThreadStep")
				.<Order,TrackedOrder>chunk(10)
				.reader(itemReaderMultiThread())
				.processor(compositeItemProcessorRetry())
				.faultTolerant()
				.retry(OrderProcessingException.class)
				.retryLimit(3)
				.listener(new CustomRetryListener())
				.writer(itemWriterMultiProcessorMultiThread())
				.taskExecutor(taskExecutor())
				.build();	
	}
	
	@Bean 
	public Job jobMultiProcessorMultiThread() throws Exception {
		return this.jobBuilderFactory.get("jobMultiProcessorMultiThread").start(chunkMultiProcessorvMultiThreadStep()).build();
	}
	
	
	/* CHUNK THIRTEEN : MULTI PROCESOR BIS +  RETRY LOGIC */
	
	@Bean
	public ItemProcessor<Order, TrackedOrder> compositeItemProcessorRetry() {
		return new CompositeItemProcessorBuilder<Order,TrackedOrder>()
				.delegates(orderValidatingItemProcessor(), trackedOrderItemProcessor(),freeShippingItemProcessor())
				.build();
	}
	
	@Bean
	public ItemWriter<TrackedOrder> itemWriterMultiProcessorRetry() {
		return new JsonFileItemWriterBuilder<TrackedOrder>()
				.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<TrackedOrder>())
				.resource( new FileSystemResource("/Users/sarahrouini/sst/batch-demo/src/main/resources/data/shipped_orders_output.json"))
				.name("jsonItemWriter")
				.build();
	}
	
	@Bean
	public Step chunkMultiProcessorvRetryStep() throws Exception {
		return this.stepBuilderFactory.get("chunkMultiProcessorvRetryStep")
				.<Order,TrackedOrder>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.processor(compositeItemProcessorRetry())
				.faultTolerant()
				.retry(OrderProcessingException.class)
				.retryLimit(3)
				.listener(new CustomRetryListener())
				.writer(itemWriterMultiProcessorRetry())
				.build();	
	}
	
	@Bean 
	public Job jobMultiProcessorRetry() throws Exception {
		return this.jobBuilderFactory.get("jobMultiProcessorRetry").start(chunkMultiProcessorvRetryStep()).build();
	}
	
	/* CHUNK TWELVE : MULTI PROCESOR BIS +  SKIP LOGIC */
	@Bean
	public ItemProcessor<TrackedOrder, TrackedOrder> freeShippingItemProcessor() {
		return new FreeShippingItemProcessor();
	}
	
	@Bean
	public ItemProcessor<Order, TrackedOrder> compositeItemProcessorBis() {
		return new CompositeItemProcessorBuilder<Order,TrackedOrder>()
				.delegates(orderValidatingItemProcessor(), trackedOrderItemProcessor(),freeShippingItemProcessor())
				.build();
	}
	
	@Bean
	public ItemWriter<TrackedOrder> itemWriterMultiProcessorBis() {
		return new JsonFileItemWriterBuilder<TrackedOrder>()
				.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<TrackedOrder>())
				.resource( new FileSystemResource("/Users/sarahrouini/sst/batch-demo/src/main/resources/data/shipped_orders_output.json"))
				.name("jsonItemWriter")
				.build();
	}
	
	@Bean
	public Step chunkMultiProcessorBisStep() throws Exception {
		return this.stepBuilderFactory.get("chunkMultiProcessorBisStep")
				.<Order,TrackedOrder>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.processor(compositeItemProcessorBis())
				.faultTolerant()
				.skip(OrderProcessingException.class)
				.skipLimit(5)
				.listener(new CustomSkipListener())
				.writer(itemWriterMultiProcessorBis())
				.build();	
	}

	@Bean 
	public Job jobMultiProcessorBis() throws Exception {
		return this.jobBuilderFactory.get("jobMultiProcessorBis").start(chunkMultiProcessorBisStep()).build();
	}
	
	/* CHUNK ELEVEN : MULTI PROCESOR TO VALIDATE BEAN */
	@Bean
	public ItemProcessor<Order, TrackedOrder> compositeItemProcessor() {
		return new CompositeItemProcessorBuilder<Order,TrackedOrder>()
				.delegates(orderValidatingItemProcessor(), trackedOrderItemProcessor())
				.build();
	}
	
	@Bean
	public ItemWriter<TrackedOrder> itemWriterMultiProcessor() {
		return new JsonFileItemWriterBuilder<TrackedOrder>()
				.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<TrackedOrder>())
				.resource( new FileSystemResource("/Users/sarahrouini/sst/batch-demo/src/main/resources/data/shipped_orders_output.json"))
				.name("jsonItemWriter")
				.build();
	}
	
	@Bean
	public Step chunkMultiProcessorStep() throws Exception {
		return this.stepBuilderFactory.get("chunkMultiProcessorStep")
				.<Order,TrackedOrder>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.processor(compositeItemProcessor())
				.writer(itemWriterMultiProcessor())
				.build();	
	}

	@Bean 
	public Job jobMultiProcessor() throws Exception {
		return this.jobBuilderFactory.get("jobMultiProcessor").start(chunkMultiProcessorStep()).build();
	}
	
	/* CHUNK TEN : ITEM PROCESOR TO VALIDATE BEAN */
	@Bean
	public ItemProcessor<Order, TrackedOrder> trackedOrderItemProcessor() {
		return new TrackedOrderItemProcessor();
	}
	
	@Bean
	public ItemWriter<TrackedOrder> itemWriterCustomProcessor() {
		return new JsonFileItemWriterBuilder<TrackedOrder>()
				.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<TrackedOrder>())
				.resource( new FileSystemResource("/Users/sarahrouini/sst/batch-demo/src/main/resources/data/shipped_orders_output.json"))
				.name("jsonItemWriter")
				.build();
	}
	
	@Bean
	public Step chunkCustomProcessorStep() throws Exception {
		return this.stepBuilderFactory.get("chunkCustomProcessorStep")
				.<Order,TrackedOrder>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.processor(trackedOrderItemProcessor())
				.writer(itemWriterCustomProcessor())
				.build();	
	}

	@Bean 
	public Job jobCustomProcessor() throws Exception {
		return this.jobBuilderFactory.get("jobCustomProcessor").start(chunkCustomProcessorStep()).build();
	}
	
	/* CHUNK NINE : ITEM PROCESOR TO VALIDATE BEAN */
	@Bean
	public ItemProcessor<Order, Order> orderValidatingItemProcessor() {
		BeanValidatingItemProcessor<Order> itemProcessor = new BeanValidatingItemProcessor<Order>();
		itemProcessor.setFilter(true);
		return itemProcessor;
	}
	
	@Bean
	public ItemWriter<Order> itemWriterValidationProcessor() {
		return new JsonFileItemWriterBuilder<Order>()
				.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<Order>())
				.resource( new FileSystemResource("/Users/sarahrouini/sst/batch-demo/src/main/resources/data/shipped_orders_output.json"))
				.name("jsonItemWriter")
				.build();
	}
	
	@Bean
	public Step chunkValidationProcessorStep() throws Exception {
		return this.stepBuilderFactory.get("chunkValidationProcessorStep")
				.<Order,Order>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.processor(orderValidatingItemProcessor())
				.writer(itemWriterValidationProcessor())
				.build();	
	}

	@Bean 
	public Job jobValidationProcessor() throws Exception {
		return this.jobBuilderFactory.get("jobValidationProcessor").start(chunkValidationProcessorStep()).build();
	}
	
	/* CHUNK HEIGHT : FROM A DATABASE DATASOURCE TO WRITING IN A JSON FILE */
	
	@Bean
	public ItemWriter<Order> itemWriterToJsonFile() {
		return new JsonFileItemWriterBuilder<Order>()
				.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<Order>())
				.resource( new FileSystemResource("/Users/sarahrouini/sst/batch-demo/src/main/resources/data/shipped_orders_output.json"))
				.name("jsonItemWriter")
				.build();
	}
	
	@Bean
	public Step chunkWritingOnAJsonFileStep() throws Exception {
		return this.stepBuilderFactory.get("chunkWritingOnAJsonFileStep")
				.<Order,Order>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.writer(itemWriterToJsonFile())
				.build();	
	}

	@Bean 
	public Job jobWritingOnAJsonFile() throws Exception {
		return this.jobBuilderFactory.get("jobWritingOnAJsonFile").start(chunkWritingOnAJsonFileStep()).build();
	}
	

/* CHUNK SEVEN : FROM A DATABASE DATASOURCE TO WRITING IN A DATABASE with named parameters */
	
	@Bean
	public ItemWriter<Order> itemWriterInADatabaseNamedParameters() {
		
		return new JdbcBatchItemWriterBuilder<Order>()
				.dataSource(datasource)
				.sql(INSERT_ORDER_SQL_BIS)
				.beanMapped()
				.build();
	}
	
	@Bean
	public Step chunkWritingOnADatabaseNamedParametersStep() throws Exception {
		return this.stepBuilderFactory.get("chunkWritingOnADatabaseStep")
				.<Order,Order>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.writer(itemWriterInADatabaseNamedParameters())
				.build();	
	}

	@Bean 
	public Job jobWritingOnADatabaseNamedParameters() throws Exception {
		return this.jobBuilderFactory.get("jobWritingOnADatabaseNamedParameters").start(chunkWritingOnADatabaseNamedParametersStep()).build();
	}
	
	/* CHUNK SIX : FROM A DATABASE DATASOURCE TO WRITING IN A DATABASE with Prepared Statement */
	
	@Bean
	public ItemWriter<Order> itemWriterInADatabase() {
		
		return new JdbcBatchItemWriterBuilder<Order>()
				.dataSource(datasource)
				.sql(INSERT_ORDER_SQL)
				.itemPreparedStatementSetter(new OrderItemPreparedStatementSetter())
				.build();
	}
	
	@Bean
	public Step chunkWritingOnADatabaseStep() throws Exception {
		return this.stepBuilderFactory.get("chunkWritingOnAFileStep")
				.<Order,Order>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.writer(itemWriterInADatabase())
				.build();	
	}

	@Bean 
	public Job jobWritingOnADatabase() throws Exception {
		return this.jobBuilderFactory.get("jobWritingOnADatabase").start(chunkWritingOnADatabaseStep()).build();
	}
	
	
	/* CHUNK FIVE : FROM A DATABASE DATASOURCE TO WRITING IN A FILE */
	
	@Bean
	public ItemWriter<Order> itemWriterInAFile() {
		
		FlatFileItemWriter<Order> itemWriter = new FlatFileItemWriter<Order>();
		FileSystemResource outputFile = new FileSystemResource("/Users/sarahrouini/sst/batch-demo/src/main/resources/data/shipped_orders_output.csv");

		itemWriter.setResource(outputFile);
		
		//  extract the field values from object then aggregate them into one line within the CSV file
		DelimitedLineAggregator<Order> aggregator = new DelimitedLineAggregator<Order>();
		aggregator.setDelimiter(",");
		
		//this object is going to be used to pull the values from the fields
		BeanWrapperFieldExtractor<Order> fieldExtractor = new BeanWrapperFieldExtractor<Order>();
		fieldExtractor.setNames(names);
		
		aggregator.setFieldExtractor(fieldExtractor);
		itemWriter.setLineAggregator(aggregator);
		
		return itemWriter;
	}
	
	@Bean
	public Step chunkWritingOnAFileStep() throws Exception {
		return this.stepBuilderFactory.get("chunkWritingOnAFileStep")
				.<Order,Order>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.writer(itemWriterInAFile())
				.build();	
	}

	@Bean 
	public Job jobWritingOnAFile() throws Exception {
		return this.jobBuilderFactory.get("jobWritingOnAFile").start(chunkWritingOnAFileStep()).build();
	}
	
	
	/* CHUNK FOUR : FROM A DATABASE DATASOURCE (MULTI THREAD)*/
	@Bean
	public PagingQueryProvider queryProvider() throws Exception {
		
		SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
		factory.setSelectClause("select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date");
		factory.setFromClause("from SHIPPED_ORDER");
		factory.setSortKey("order_id");
		factory.setDataSource(datasource);
		
		return factory.getObject();
	}
	
	

	@Bean
	public ItemReader<Order> orderItemReaderPagingJDBC() throws Exception {
		
		return new JdbcPagingItemReaderBuilder<Order>()
				.dataSource(this.datasource)
				.name("jdbcCursorItemReader")
				.queryProvider(queryProvider())
				.rowMapper(new OrderRowMapper())
				.pageSize(10)
				.build();
		}
	

	@Bean
	public Step chunkPagingJDBCStep() throws Exception {
		return this.stepBuilderFactory.get("chunkPagingJDBCStep")
				.<Order,Order>chunk(10)
				.reader(orderItemReaderPagingJDBC())
				.writer(new ItemWriter<Order>() {

					@Override
					public void write(List<? extends Order> items) throws Exception {

						 System.out.println(String.format("Received list of size : %s", items.size()));
						 items.forEach(System.out::println);
					}
					
				}).build();	
	}

	@Bean 
	public Job jobPagingJDBC() throws Exception {
		return this.jobBuilderFactory.get("jobPagingJDBC").start(chunkPagingJDBCStep()).build();
	}
	
	/* CHUNK THREAD : FROM A DATABASE DATASOURCE (ONE THREAD)*/
	
	@Bean
	public ItemReader<Order> orderItemReaderJDBC() {
		
		return new JdbcCursorItemReaderBuilder<Order>()
				.dataSource(this.datasource)
				.name("jdbcCursorItemReader")
				.sql(ORDER_SQL)
				.rowMapper(new OrderRowMapper())
				.build();
		}
	
	@Bean
	public Step chunkOrderJdbcStep() {
		return this.stepBuilderFactory.get("chunkOrderJdbcStep")
				.<Order,Order>chunk(3)
				.reader(orderItemReaderJDBC())
				.writer(new ItemWriter<Order>() {

					@Override
					public void write(List<? extends Order> items) throws Exception {

						 System.out.println(String.format("Received list of size : %s", items.size()));
						 items.forEach(System.out::println);
					}
					
				}).build();	
	}

	@Bean 
	public Job jobOrderItemReaderJDBC() {
		return this.jobBuilderFactory.get("jobOrderItemReaderJDBC").start(chunkOrderJdbcStep()).build();
	}
	
	/* CHUNK TWO : FROM A FLAT FILE (CSV) DATASOURCE*/
	
	@Bean
	public ItemReader<Order> orderItemReader() {
		
		FlatFileItemReader<Order> itemReader = new FlatFileItemReader<Order>();
		itemReader.setLinesToSkip(1);
		itemReader.setResource(resourceLoader.getResource("classpath:data/shipped_orders.csv"));
		
		// way of parsing data
		DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<Order>();
		//break the line based upon a character (comma as a delimiter by default)
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		// name of columns of csv file
		tokenizer.setNames(tokens); 
		
		lineMapper.setLineTokenizer(tokenizer);
		// to build an object of type order from the line
		lineMapper.setFieldSetMapper(new OrderFieldSetMapper());
		// set the lineMapper on the itemReader
		itemReader.setLineMapper(lineMapper);
		
		return itemReader;	
		
	}
	
	@Bean
	public Step chunkFromCsvFileStep() {
		return this.stepBuilderFactory.get("chunkFromCsvFileStep")
				.<Order,Order>chunk(3)
				.reader(orderItemReader())
				.writer(new ItemWriter<Order>() {

					@Override
					public void write(List<? extends Order> items) throws Exception {

						 System.out.println(String.format("Received list of size : %s", items.size()));
						 items.forEach(System.out::println);
					}
					
				}).build();	
	}
	
	@Bean 
	public Job jobFromCsvFileStep() {
		return this.jobBuilderFactory.get("jobFromCsvFileStep").start(chunkFromCsvFileStep()).build();
	}
	
	
	/* CHUNK ONE : FROM A LIST DATASOURCE*/
	
	@Bean
	public ItemReader<String> itemReader() {
		return new SimpleItemReader();
	}
	
	
	@Bean
	public Step chunkBasedStep() {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<String,String>chunk(3)
				.reader(itemReader())
				.writer(new ItemWriter<String>() {

					@Override
					public void write(List<? extends String> items) throws Exception {

						 System.out.println(String.format("Received list of size : %s", items.size()));
						 items.forEach(System.out::println);
					}
					
				}).build();	
	}

	@Bean 
	public Job job() {
		return this.jobBuilderFactory.get("job").start(chunkBasedStep()).build();
	}
	
	@Bean
	public Step nestingBillingJobStep() {
		return this.stepBuilderFactory.get("nestingBillingJobStep").job(billingJob()).build();
	}
	
	@Bean
	public Step sendInvoiceStep() {
		return this.stepBuilderFactory.get("sendInvoiceStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				 System.out.println("Invoice is sent to the custumer");
				 return RepeatStatus.FINISHED;
			}
			
		}).build();
	}
	
	@Bean
	public Flow billingFlow() {
		return new FlowBuilder<SimpleFlow>("billingFlow").start(sendInvoiceStep()).build();
	}
	
	@Bean
	public Job billingJob() {
		return this.jobBuilderFactory.get("billingJob").start(sendInvoiceStep()).build();
	}
	
	@Bean
	public Flow deliveryFlow() {
		return new FlowBuilder<SimpleFlow>("deliveryFlow").start(driveToAdressStep())
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
				.build();
	}

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
	        		.from(arrangeFlowersStep()).on("*").to(deliveryFlow())
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
				.split(new SimpleAsyncTaskExecutor())
				.add(deliveryFlow(), billingFlow())
				.end().build();
	}

	public static void main(String[] args) {
		SpringApplication.run(BatchDemoApplication.class, args);
	}

	

}
