package com.pg.example.mongodbbatch.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

/** Created by Pawel Gawedzki on 16-Mar-18.*/
@Component
public class JobConfigurer {

    private static final String JOB_NAME = "SYNC-DBS";

    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private final Tasklet equipmentCargoStep;
    private final Tasklet haulageEquipmentStep;
    private final Tasklet haulageInfoStep;
    private final Tasklet operationalRouteStep;
    private final Tasklet trackingBookingStep;
    private final Tasklet cargoConditioningStep;

    @Autowired
    public JobConfigurer(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
                         Tasklet equipmentCargoStep, Tasklet haulageEquipmentStep, Tasklet haulageInfoStep,
                         Tasklet operationalRouteStep, Tasklet trackingBookingStep, Tasklet cargoConditioningStep) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;

        this.equipmentCargoStep = equipmentCargoStep;
        this.haulageEquipmentStep = haulageEquipmentStep;
        this.haulageInfoStep = haulageInfoStep;
        this.operationalRouteStep = operationalRouteStep;
        this.trackingBookingStep = trackingBookingStep;
        this.cargoConditioningStep = cargoConditioningStep;
    }

    public Job synchroniseDatabasesJob() {
        return jobBuilderFactory.get(JOB_NAME)
                .incrementer(parametersIncrementer())
                .preventRestart()
                // '*' means that we do not care if it fails or success, step are independent and should be executed that way
                .start(trackingBookingStep()).on("*").to(operationRouteStep())
                .from(operationRouteStep()).on("*").to(equipmentCargoStep())
                .from(equipmentCargoStep()).on("*").to(cargoConditioningStep())
                .from(cargoConditioningStep()).on("*").to(haulageInfoStep())
                // when the plan is COMPLETED, only then we can download equipments for plan
                .from(haulageInfoStep()).on("COMPLETED").to(haulageEquipmentStep())
                .from(haulageEquipmentStep()).on("*").end()
                .end()
                .build();
    }

    private JobParametersIncrementer parametersIncrementer() {
        return jobParameters -> {
            if (jobParameters == null || jobParameters.isEmpty()) {
                return new JobParametersBuilder().addDate(JobParameter.RUN_ID.key(), new Date(System.currentTimeMillis())).toJobParameters();
            }
            Date id = jobParameters.getDate(JobParameter.RUN_ID.key(), new Date(System.currentTimeMillis()));
            return new JobParametersBuilder().addDate(JobParameter.RUN_ID.key(), id).toJobParameters();
        };
    }

    private Step trackingBookingStep() {
        return this.stepBuilderFactory.get(trackingBookingStep.getClass().getSimpleName())
                .tasklet(trackingBookingStep)
                .build();
    }

    private Step operationRouteStep() {
        return this.stepBuilderFactory.get(operationalRouteStep.getClass().getSimpleName())
                .tasklet(operationalRouteStep)
                .build();
    }

    private Step cargoConditioningStep() {
        return this.stepBuilderFactory.get(operationalRouteStep.getClass().getSimpleName())
                .tasklet(cargoConditioningStep)
                .build();
    }

    private Step equipmentCargoStep() {
        return this.stepBuilderFactory.get(equipmentCargoStep.getClass().getSimpleName())
                .tasklet(equipmentCargoStep)
                .build();
    }

    private Step haulageInfoStep() {
        return this.stepBuilderFactory.get(haulageInfoStep.getClass().getSimpleName())
                .tasklet(haulageInfoStep)
                .build();
    }

    private Step haulageEquipmentStep() {
        return this.stepBuilderFactory.get(haulageEquipmentStep.getClass().getSimpleName())
                .tasklet(haulageEquipmentStep)
                .build();
    }
}
