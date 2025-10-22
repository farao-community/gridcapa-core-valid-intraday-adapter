/*
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_valid_intraday.adapter.app;

import com.farao_community.farao.gridcapa.task_manager.api.ProcessEventDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessRunDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_valid_intraday.api.resource.CoreValidIntradayRequest;
import com.farao_community.farao.gridcapa_core_valid_intraday.starter.CoreValidIntradayClient;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * @author Marc Schwitzguebel {@literal <marc.schwitzguebel_external at rte-france.com>}
 */
@SpringBootTest
class CoreValidIntradayAdapterListenerTest {

    @MockitoBean
    private CoreValidIntradayClient coreValidIntradayClient;

    @MockitoBean
    private MinioAdapter minioAdapter;

    @Captor
    ArgumentCaptor<CoreValidIntradayRequest> argumentCaptor;

    @Autowired
    private CoreValidIntradayAdapterListener coreValidIntradayAdapterListener;
    private String cnecRamFileType;
    private String verticeFileType;
    private String mergedCnecFileType;
    private String cgmFileType;
    private String glskFileType;
    private String marketPointFileType;
    private String praFileType;

    private String cnecRamFileName;
    private String verticeFileName;
    private String cgmFileName;
    private String glskFileName;
    private String mergedCnecFileName;
    private String marketPointFileName;
    private String praFileName;

    private String cgmFileUrl;
    private String glskFileUrl;
    private String cgmFilePath;
    private String glskFilePath;
    private String cnecRamFilePath;
    private String verticeFilePath;
    private String mergedCnecFilePath;
    private String marketPointFilePath;
    private String praFilePath;
    private String cnecRamFileUrl;
    private String verticeFileUrl;
    private String mergedCnecFileUrl;
    private String marketPointFileUrl;
    private String praFileUrl;

    public TaskDto createTaskDtoWithStatus(final TaskStatus status) {
        final UUID id = UUID.randomUUID();
        final OffsetDateTime timestamp = OffsetDateTime.parse("2025-10-02T14:30Z");
        final List<ProcessFileDto> processFiles = new ArrayList<>();

        processFiles.add(new ProcessFileDto(cnecRamFilePath, cnecRamFileType, ProcessFileStatus.VALIDATED, cnecRamFileName, "docId1", timestamp));
        processFiles.add(new ProcessFileDto(verticeFilePath, verticeFileType, ProcessFileStatus.VALIDATED, verticeFileName, "docId2", timestamp));
        processFiles.add(new ProcessFileDto(cgmFilePath, cgmFileType, ProcessFileStatus.VALIDATED, cgmFileName, "docId3", timestamp));
        processFiles.add(new ProcessFileDto(glskFilePath, glskFileType, ProcessFileStatus.VALIDATED, glskFileName, "docId4", timestamp));
        processFiles.add(new ProcessFileDto(mergedCnecFilePath, mergedCnecFileType, ProcessFileStatus.VALIDATED, mergedCnecFileName, "docId5", timestamp));
        processFiles.add(new ProcessFileDto(marketPointFilePath, marketPointFileType, ProcessFileStatus.VALIDATED, marketPointFileName, "docId6", timestamp));
        processFiles.add(new ProcessFileDto(praFilePath, praFileType, ProcessFileStatus.VALIDATED, praFileName, "docId7", timestamp));
        final List<ProcessEventDto> processEvents = new ArrayList<>();
        final List<ProcessRunDto> runHistory = new ArrayList<>();
        runHistory.add(new ProcessRunDto(UUID.randomUUID(), OffsetDateTime.now(), processFiles));
        return new TaskDto(id, timestamp, status, processFiles, null, Collections.emptyList(), processEvents, runHistory, Collections.emptyList());
    }

    @BeforeEach
    void setUp() {
        cnecRamFileType = "CNEC-RAM";
        verticeFileType = "VERTICE";
        cgmFileType = "CGM";
        glskFileType = "GLSK";
        mergedCnecFileType = "MERGED-CNEC";
        marketPointFileType = "MARKET-POINT";
        praFileType = "PRA";

        cnecRamFileName = "cnec-ram";
        verticeFileName = "vertice";
        cgmFileName = "cgm";
        glskFileName = "glsk";
        mergedCnecFileName = "merged-cnec";
        marketPointFileName = "market-point";
        praFileName = "PRA";

        cnecRamFilePath = "/CNEC-RAM";
        verticeFilePath = "/VERTICE";
        cgmFilePath = "/CGM";
        glskFilePath = "/GLSK";
        mergedCnecFilePath = "/MERGED-CNEC";
        marketPointFilePath = "/MARKET-POINT";
        praFilePath = "/PRA";

        cnecRamFileUrl = "file://CNEC-RAM/cnecram";
        verticeFileUrl = "file://VERTICE/vertice";
        cgmFileUrl = "file://CGM/cgm.uct";
        glskFileUrl = "file://GLSK/glsk";
        mergedCnecFileUrl = "file://MERGED-CNEC/mergedCnec";
        marketPointFileUrl = "file://MARKET-POINT/marketPoint";
        praFileUrl = "file://PRA/pra";

        Mockito.when(minioAdapter.generatePreSignedUrlFromFullMinioPath(cnecRamFilePath, 1)).thenReturn(cnecRamFileUrl);
        Mockito.when(minioAdapter.generatePreSignedUrlFromFullMinioPath(verticeFilePath, 1)).thenReturn(verticeFileUrl);
        Mockito.when(minioAdapter.generatePreSignedUrlFromFullMinioPath(cgmFilePath, 1)).thenReturn(cgmFileUrl);
        Mockito.when(minioAdapter.generatePreSignedUrlFromFullMinioPath(glskFilePath, 1)).thenReturn(glskFileUrl);
        Mockito.when(minioAdapter.generatePreSignedUrlFromFullMinioPath(mergedCnecFilePath, 1)).thenReturn(mergedCnecFileUrl);
        Mockito.when(minioAdapter.generatePreSignedUrlFromFullMinioPath(marketPointFilePath, 1)).thenReturn(marketPointFileUrl);
        Mockito.when(minioAdapter.generatePreSignedUrlFromFullMinioPath(praFilePath, 1)).thenReturn(praFileUrl);
    }

    @Test
    void testGetManualCoreValidRequest() {
        final TaskDto taskDto = createTaskDtoWithStatus(TaskStatus.READY);
        final CoreValidIntradayRequest coreValidRequest = coreValidIntradayAdapterListener.getManualCoreValidIntradayRequest(taskDto);
        Assertions.assertEquals(taskDto.getId().toString(), coreValidRequest.getId());
        Assertions.assertEquals(cnecRamFileName, coreValidRequest.getCnecRam().getFilename());
        Assertions.assertEquals(cnecRamFileUrl, coreValidRequest.getCnecRam().getUrl());
        Assertions.assertEquals(verticeFileName, coreValidRequest.getVertice().getFilename());
        Assertions.assertEquals(verticeFileUrl, coreValidRequest.getVertice().getUrl());
        Assertions.assertEquals(cgmFileName, coreValidRequest.getCgm().getFilename());
        Assertions.assertEquals(cgmFileUrl, coreValidRequest.getCgm().getUrl());
        Assertions.assertEquals(glskFileName, coreValidRequest.getGlsk().getFilename());
        Assertions.assertEquals(glskFileUrl, coreValidRequest.getGlsk().getUrl());
        Assertions.assertEquals(mergedCnecFileName, coreValidRequest.getMergedCnec().getFilename());
        Assertions.assertEquals(mergedCnecFileUrl, coreValidRequest.getMergedCnec().getUrl());
        Assertions.assertEquals(marketPointFileName, coreValidRequest.getMarketPoint().getFilename());
        Assertions.assertEquals(marketPointFileUrl, coreValidRequest.getMarketPoint().getUrl());
        Assertions.assertEquals(praFileName, coreValidRequest.getPra().getFilename());
        Assertions.assertEquals(praFileUrl, coreValidRequest.getPra().getUrl());
        Assertions.assertFalse(coreValidRequest.getLaunchedAutomatically());
    }

    @Test
    void testGetManualCoreValidRequestThrowsException() {
        final TaskDto taskDto = new TaskDto(UUID.randomUUID(), OffsetDateTime.now(), TaskStatus.READY, List.of(), null, List.of(), List.of(), List.of(), List.of());

        Assertions.assertThrows(
                CoreValidIntradayAdapterException.class,
                () -> coreValidIntradayAdapterListener.getManualCoreValidIntradayRequest(taskDto),
                "Failed to handle manual run request on timestamp because it has no run history");
    }

    @Test
    void testGetAutomaticCoreValidRequest() {
        final TaskDto taskDto = createTaskDtoWithStatus(TaskStatus.READY);
        final CoreValidIntradayRequest coreValidRequest = coreValidIntradayAdapterListener.getAutomaticCoreValidIntradayRequest(taskDto);
        Assertions.assertTrue(coreValidRequest.getLaunchedAutomatically());
    }

    @Test
    void testGetCoreValidRequestWithIncorrectFiles() {
        final UUID id = UUID.randomUUID();
        final OffsetDateTime timestamp = OffsetDateTime.parse("2021-12-07T14:30Z");
        final List<ProcessFileDto> processFiles = new ArrayList<>();
        processFiles.add(new ProcessFileDto(cnecRamFilePath, cnecRamFileType, ProcessFileStatus.VALIDATED, cnecRamFileName, "docId1", timestamp));
        processFiles.add(new ProcessFileDto(verticeFilePath, verticeFileType, ProcessFileStatus.VALIDATED, verticeFileName, "docId2", timestamp));
        processFiles.add(new ProcessFileDto(cgmFilePath, cgmFileType, ProcessFileStatus.VALIDATED, cgmFileName, "docId3", timestamp));
        processFiles.add(new ProcessFileDto(glskFilePath, glskFileType, ProcessFileStatus.VALIDATED, glskFileName, "docId4", timestamp));
        processFiles.add(new ProcessFileDto(mergedCnecFilePath, mergedCnecFileType, ProcessFileStatus.VALIDATED, mergedCnecFileName, "docId5", timestamp));
        processFiles.add(new ProcessFileDto(marketPointFilePath, "REF-PROG", ProcessFileStatus.VALIDATED, marketPointFileName, "docId6", timestamp));
        processFiles.add(new ProcessFileDto(praFilePath, praFileType, ProcessFileStatus.VALIDATED, praFileName, "docId7", timestamp));
        final List<ProcessEventDto> processEvents = new ArrayList<>();
        final TaskDto taskDto = new TaskDto(id, timestamp, TaskStatus.READY, processFiles, null, Collections.emptyList(), processEvents, Collections.emptyList(), Collections.emptyList());
        Assertions.assertThrows(IllegalStateException.class, () -> coreValidIntradayAdapterListener.getManualCoreValidIntradayRequest(taskDto));

    }

    @Test
    void consumeReadyAutoTask() {
        final TaskDto taskDto = createTaskDtoWithStatus(TaskStatus.READY);
        coreValidIntradayAdapterListener.consumeAutoTask().accept(taskDto);
        Mockito.verify(coreValidIntradayClient).run(argumentCaptor.capture());
        final CoreValidIntradayRequest coreValidRequest = argumentCaptor.getValue();
        assert coreValidRequest.getLaunchedAutomatically();
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"READY", "SUCCESS", "ERROR"})
    void consumeReadyTask(final TaskStatus taskStatus) {
        final TaskDto taskDto = createTaskDtoWithStatus(taskStatus);
        coreValidIntradayAdapterListener.consumeTask().accept(taskDto);
        Mockito.verify(coreValidIntradayClient).run(argumentCaptor.capture());
        final CoreValidIntradayRequest coreValidRequest = argumentCaptor.getValue();
        Assertions.assertFalse(coreValidRequest.getLaunchedAutomatically());
    }

    @Test
    void consumeCreatedTask() {
        final TaskDto taskDto = createTaskDtoWithStatus(TaskStatus.CREATED);
        coreValidIntradayAdapterListener.consumeTask().accept(taskDto);
        Mockito.verify(coreValidIntradayClient, Mockito.never()).run(argumentCaptor.capture());
    }

    @Test
    void consumeTaskThrowsException() {
        final TaskDto taskDto = createTaskDtoWithStatus(TaskStatus.READY);
        Mockito.doThrow(RuntimeException.class).when(coreValidIntradayClient).run(Mockito.any());
        final Consumer<TaskDto> taskDtoConsumer = coreValidIntradayAdapterListener.consumeTask();
        Assertions.assertThrows(
                CoreValidIntradayAdapterException.class,
                () -> taskDtoConsumer.accept(taskDto),
                "Error during handling manual run request on TS 2025-10-02T14:30Z");
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"READY", "SUCCESS", "ERROR"})
    void consumeSuccessAutoTask(final TaskStatus taskStatus) {
        final TaskDto taskDto = createTaskDtoWithStatus(taskStatus);
        coreValidIntradayAdapterListener.consumeAutoTask().accept(taskDto);
        Mockito.verify(coreValidIntradayClient).run(argumentCaptor.capture());
        final CoreValidIntradayRequest coreValidIntradayRequest = argumentCaptor.getValue();
        assert coreValidIntradayRequest.getLaunchedAutomatically();
    }

    @Test
    void consumeCreatedAutoTask() {
        final TaskDto taskDto = createTaskDtoWithStatus(TaskStatus.CREATED);
        coreValidIntradayAdapterListener.consumeAutoTask().accept(taskDto);
        Mockito.verify(coreValidIntradayClient, Mockito.never()).run(argumentCaptor.capture());
    }

    @Test
    void consumeAutoTaskThrowsException() {
        final TaskDto taskDto = createTaskDtoWithStatus(TaskStatus.READY);
        Mockito.doThrow(RuntimeException.class).when(coreValidIntradayClient).run(Mockito.any());
        final Consumer<TaskDto> taskDtoConsumer = coreValidIntradayAdapterListener.consumeAutoTask();
        Assertions.assertThrows(
                CoreValidIntradayAdapterException.class,
                () -> taskDtoConsumer.accept(taskDto),
                "Error during handling manual run request on TS 2025-10-02T14:30Z");
    }
}
