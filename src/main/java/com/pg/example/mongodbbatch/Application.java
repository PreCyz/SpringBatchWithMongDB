package com.pg.example.mongodbbatch;

import com.pg.example.mongodbbatch.config.MainBatchConfigurer;
import com.pg.example.mongodbbatch.job.JobConfigurer;
import com.pg.example.mongodbbatch.job.JobParameter;
import com.pg.example.mongodbbatch.util.TimeLog;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.Date;

@EnableBatchProcessing
@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class Application implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

	@Autowired
    private MainBatchConfigurer batchConfigurer;
	@Autowired
	private JobConfigurer jobConfigurer;
	@Value("${csv.directory}")
    private String csvDirectory = "./csv";

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... strings) {
        TimeLog timeLog = new TimeLog(getClass().getSimpleName());
        try {
            long timestamp = System.currentTimeMillis();
            logger.debug("Start GCSS batch with timestamp '{}'.", timestamp);
            SimpleJobLauncher jobLauncher = batchConfigurer.getJobLauncher();
            JobParameters jobParameters = new JobParametersBuilder().addDate(JobParameter.RUN_ID.key(), new Date(timestamp)).toJobParameters();
			timeLog.logTime("Job execution");
            JobExecution jobExecution = jobLauncher.run(jobConfigurer.synchroniseDatabasesJob(), jobParameters);
			logger.debug("Job execution finished, timestamp = {}, job execution status = {}.", timestamp, jobExecution.getStatus());
		} catch (Exception e) {
            logger.error("Error when launching job.", e);
		} finally {
            try {
                timeLog.logTime("Delete " + csvDirectory);
                FileUtils.forceDelete(new File(csvDirectory));
            } catch (IOException e) {
                logger.error("Error when deleting  {} directory.", csvDirectory, e);
            }
        }
        timeLog.done();
        System.exit(0);
    }
}
