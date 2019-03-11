package com.datacenter.utils;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

@Configuration
public class SchedulingConfiguration implements SchedulingConfigurer {

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {

        //默认是一个线程
        taskRegistrar.setScheduler(Executors.newScheduledThreadPool(100));
        // TODO Auto-generated method stub
        taskRegistrar.setScheduler(new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r,"scan-schedule");
                    }
                }));
    }
}