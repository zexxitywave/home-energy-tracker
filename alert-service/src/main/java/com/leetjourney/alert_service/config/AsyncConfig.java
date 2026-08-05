package com.leetjourney.alert_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class AsyncConfig {

    @Bean(name = "emailTaskExecutor")
    public Executor emailTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(50);
        executor.setQueueCapacity(1000); // When all core threads are busy, new tasks are placed into this queue.
        // 10 threads execute immediately.
        //
        //1000 tasks enter queue.
        //
        //Remaining 190 tasks:
        //
        //Pool starts creating extra threads
        //11...
        //12...
        //...
        //50
        //
        //If:
        //
        //50 threads are busy and
        //Queue has 1000 tasks
        //
        //then any additional task is rejected unless a custom rejection policy is configured.
        executor.setThreadNamePrefix("email-async-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        // Shutdown requested
        // Finish currently running email tasks
        // Application exits
        executor.setAwaitTerminationSeconds(30);
        //Spring will wait up to 30 seconds for pending tasks to finish.

        executor.initialize();
        return executor;
    }
}
