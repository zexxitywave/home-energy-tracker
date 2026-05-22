package com.leetjourney.insight_service.service;

import com.leetjourney.insight_service.client.UsageClient;
import com.leetjourney.insight_service.dto.DeviceDto;
import com.leetjourney.insight_service.dto.InsightDto;
import com.leetjourney.insight_service.dto.UsageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.ollama.OllamaChatModel;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class InsightService {

    private final UsageClient usageClient;
    private final OllamaChatModel ollamaChatModel;

    public InsightService(UsageClient usageClient,
                          OllamaChatModel ollamaChatModel) {
        this.usageClient = usageClient;
        this.ollamaChatModel = ollamaChatModel;
    }

    public InsightDto getSavingsTips(Long userId) {

        final UsageDto usageData = usageClient.getXDaysUsageForUser(userId, 3);

        List<DeviceDto> devices = usageData != null && usageData.devices() != null
                ? usageData.devices()
                : Collections.emptyList();

        double totalUsage = devices.stream()
                .mapToDouble(DeviceDto::energyConsumed)
                .sum();

        log.info("Calling Ollama for userId {} with total usage {}", userId, totalUsage);

        String prompt = new StringBuilder()
                .append("This is my total consumption over the past 3 days. ")
                .append("How can I reduce my energy consumption? ")
                .append("How does it compare to average households? ")
                .append("Total energy used: ")
                .append(totalUsage)
                .toString();

        ChatResponse response = ollamaChatModel.call(
                Prompt.builder()
                        .content(prompt)
                        .build()
        );

        return InsightDto.builder()
                .userId(userId)
                .tips(response.getResult().getOutput().getText())
                .energyUsage(totalUsage)
                .build();
    }

    public InsightDto getOverview(Long userId) {

        final UsageDto usageData = usageClient.getXDaysUsageForUser(userId, 3);

        List<DeviceDto> devices = usageData != null && usageData.devices() != null
                ? usageData.devices()
                : Collections.emptyList();

        double totalUsage = devices.stream()
                .mapToDouble(DeviceDto::energyConsumed)
                .sum();

        log.info("Calling Ollama for userId {} with total usage {}", userId, totalUsage);

        String prompt = new StringBuilder()
                .append("Analyse the following energy usage data and provide concise actionable insights. ")
                .append("This is aggregate data for the past 3 days. ")
                .append("Usage Data: ")
                .append(devices)
                .toString();

        ChatResponse response = ollamaChatModel.call(
                Prompt.builder()
                        .content(prompt)
                        .build()
        );

        return InsightDto.builder()
                .userId(userId)
                .tips(response.getResult().getOutput().getText())
                .energyUsage(totalUsage)
                .build();
    }
}