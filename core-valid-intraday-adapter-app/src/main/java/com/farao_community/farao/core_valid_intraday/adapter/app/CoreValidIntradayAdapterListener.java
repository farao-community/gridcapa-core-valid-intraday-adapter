/*
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_valid_intraday.adapter.app;

import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessRunDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_valid_intraday.api.resource.CoreValidIntradayFileResource;
import com.farao_community.farao.gridcapa_core_valid_intraday.api.resource.CoreValidIntradayRequest;
import com.farao_community.farao.gridcapa_core_valid_intraday.starter.CoreValidIntradayClient;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.farao_community.farao.gridcapa.task_manager.api.TaskStatus.ERROR;
import static com.farao_community.farao.gridcapa.task_manager.api.TaskStatus.READY;
import static com.farao_community.farao.gridcapa.task_manager.api.TaskStatus.SUCCESS;
/**
 * @author Marc Schwitzguebel {@literal <marc.schwitzguebel_external at rte-france.com>}
 */
@Component
public class CoreValidIntradayAdapterListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoreValidIntradayAdapterListener.class);
    private final CoreValidIntradayClient coreValidIntradayClient;
    private final MinioAdapter minioAdapter;

    public CoreValidIntradayAdapterListener(final CoreValidIntradayClient coreValidIntradayClient, final MinioAdapter minioAdapter) {
        this.coreValidIntradayClient = coreValidIntradayClient;
        this.minioAdapter = minioAdapter;
    }

    @Bean
    public Consumer<TaskDto> consumeTask() {
        return this::handleManualTask;
    }

    @Bean
    public Consumer<TaskDto> consumeAutoTask() {
        return this::handleAutoTask;
    }

    private void handleAutoTask(final TaskDto taskDto) {
        handleTask(taskDto, this::getAutomaticCoreValidIntradayRequest, "automatic");
    }

    private void handleManualTask(final TaskDto taskDto) {
        handleTask(taskDto, this::getManualCoreValidIntradayRequest, "manual");
    }

    private void handleTask(final TaskDto taskDto,
                            final Function<TaskDto, CoreValidIntradayRequest> coreValidReqMapper,
                            final String launchType) {
        try {
            if (isReadyOrFinished(taskDto)) {
                LOGGER.info("Handling {} run request on TS {} ", launchType, taskDto.getTimestamp());
                final CoreValidIntradayRequest request = coreValidReqMapper.apply(taskDto);
                coreValidIntradayClient.run(request);
            } else {
                LOGGER.warn("Failed to handle {} run request on timestamp {} because it is not ready yet",
                            launchType,
                            taskDto.getTimestamp());
            }
        } catch (final Exception e) {
            throw new CoreValidIntradayAdapterException(String.format("Error during handling of %s run request on TS %s",
                                                              launchType, taskDto.getTimestamp()), e);
        }

    }

    private static boolean isReadyOrFinished(final TaskDto taskDto) {
        final TaskStatus status = taskDto.getStatus();
        return status == READY || status == SUCCESS || status == ERROR;
    }

    CoreValidIntradayRequest getManualCoreValidIntradayRequest(final TaskDto taskDto) {
        return getCoreValidIntradayRequest(taskDto, false);
    }

    CoreValidIntradayRequest getAutomaticCoreValidIntradayRequest(final TaskDto taskDto) {
        return getCoreValidIntradayRequest(taskDto, true);
    }

    CoreValidIntradayRequest getCoreValidIntradayRequest(final TaskDto taskDto,
                                                 final boolean isLaunchedAutomatically) {
        final String id = taskDto.getId().toString();
        final OffsetDateTime offsetDateTime = taskDto.getTimestamp();
        final List<ProcessFileDto> processFiles = taskDto.getInputs();
        CoreValidIntradayFileResource cnecRam = null;
        CoreValidIntradayFileResource vertice = null;
        CoreValidIntradayFileResource cgm = null;
        CoreValidIntradayFileResource glsk = null;
        CoreValidIntradayFileResource mergedCnec = null;
        CoreValidIntradayFileResource marketPoint = null;
        CoreValidIntradayFileResource pra = null;
        for (final ProcessFileDto processFileDto : processFiles) {
            final String fileType = processFileDto.getFileType();
            final String fileUrl = minioAdapter.generatePreSignedUrlFromFullMinioPath(processFileDto.getFilePath(), 1);
            final String fileName = processFileDto.getFilename();
            switch (fileType) {
                case "CNEC-RAM" -> cnecRam = new CoreValidIntradayFileResource(fileName, fileUrl);
                case "VERTICE" -> vertice = new CoreValidIntradayFileResource(fileName, fileUrl);
                case "CGM" -> cgm = new CoreValidIntradayFileResource(fileName, fileUrl);
                case "GLSK" -> glsk = new CoreValidIntradayFileResource(fileName, fileUrl);
                case "MERGED-CNEC" -> mergedCnec = new CoreValidIntradayFileResource(fileName, fileUrl);
                case "MARKET-POINT" -> marketPoint = new CoreValidIntradayFileResource(fileName, fileUrl);
                case "PRA" -> pra = new CoreValidIntradayFileResource(fileName, fileUrl);
                default -> throw new IllegalStateException("Unexpected value: " + fileType);
            }
        }
        return new CoreValidIntradayRequest(
                id,
                getCurrentRunId(taskDto, isLaunchedAutomatically),
                offsetDateTime,
                cnecRam,
                vertice,
                cgm,
                glsk,
                mergedCnec,
                marketPoint,
                pra,
                isLaunchedAutomatically
        );
    }

    private String getCurrentRunId(final TaskDto taskDto,
                                   final boolean isLaunchedAutomatically) {
        final List<ProcessRunDto> runHistory = taskDto.getRunHistory();
        if (runHistory == null || runHistory.isEmpty()) {
            LOGGER.warn("Failed to handle {} run request on timestamp {} because it has no run history",
                        isLaunchedAutomatically ? "automatic" : "manual",
                        taskDto.getTimestamp());
            throw new CoreValidIntradayAdapterException("Failed to handle %s run request on timestamp because it has no run history"
                                                        .formatted(isLaunchedAutomatically ? "automatic" : "manual"));
        }
        runHistory.sort((o1, o2) -> o2.getExecutionDate().compareTo(o1.getExecutionDate()));
        return runHistory.getFirst().getId().toString();
    }
}
