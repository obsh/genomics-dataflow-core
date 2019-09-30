package com.google.allenday.genomics.core.transform;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.cmd.WorkerSetupService;
import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.IoHandler;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AlignFn extends DoFn<KV<GeneExampleMetaData, Iterable<GeneData>>, KV<GeneExampleMetaData, GeneData>> {

    private Logger LOG = LoggerFactory.getLogger(AlignFn.class);
    private GCSService gcsService;

    private CmdExecutor cmdExecutor;
    private WorkerSetupService workerSetupService;
    private List<String> referenceNames;
    private IoHandler ioHandler;

    public AlignFn(CmdExecutor cmdExecutor,
                   WorkerSetupService workerSetupService,
                   List<String> referenceNames,
                   IoHandler ioHandler) {
        this.cmdExecutor = cmdExecutor;
        this.workerSetupService = workerSetupService;
        this.referenceNames = referenceNames;
        this.ioHandler = ioHandler;
    }

    @Setup
    public void setUp() throws Exception {
        gcsService = GCSService.initialize();
        workerSetupService.setupMinimap2();
    }

    private final static String ALIGN_COMMAND_PATTERN = "./minimap2-2.17_x64-linux/minimap2" +
            " -ax sr %s %s" +
            " -R '@RG\tID:minimap2\tPL:ILLUMINA\tPU:NONE\tSM:RSP11055' " +
            "> %s";
    private final static String SAM_FILE_PREFIX = ".sam";

    private String alignFastq(List<String> localFastqPaths,
                              String filePrefix,
                              String referenceName) {
        String alignedSamPath = filePrefix + "_" + referenceName + SAM_FILE_PREFIX;
        String minimapCommand = String.format(ALIGN_COMMAND_PATTERN, ioHandler.generateReferencePath(referenceName),
                String.join(" ", localFastqPaths), alignedSamPath);

        boolean success = cmdExecutor.executeCommand(minimapCommand);
        if (!success) {
            throw new RuntimeException("Align command failed");
        }
        return alignedSamPath;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of align with input: %s", c.element().toString()));

        KV<GeneExampleMetaData, Iterable<GeneData>> input = c.element();
        List<GeneData> geneDataList = StreamSupport.stream(input.getValue().spliterator(), false)
                .collect(Collectors.toList());
        GeneExampleMetaData geneExampleMetaData = input.getKey();


        if (geneExampleMetaData != null && geneDataList.size() > 0) {
            String workingDir = '/' + System.currentTimeMillis() + "_" + geneExampleMetaData.getRun() + "/";
            FileUtils.mkdir(workingDir);

            List<String> srcFilesPaths = new ArrayList<>();
            try {
                for (GeneData geneData : geneDataList) {
                    String destFilepath = workingDir + geneData.getFileName();
                    ioHandler.handleInputAsLocalFile(geneData, gcsService, destFilepath);
                    srcFilesPaths.add(destFilepath);
                }

                String samFilePrefix = workingDir + geneExampleMetaData.getRun();
                for (String referenceName : referenceNames) {
                    ioHandler.downloadReferenceIfNeeded(gcsService, referenceName);
                    String samFile = alignFastq(srcFilesPaths, samFilePrefix, referenceName);
                    c.output(KV.of(geneExampleMetaData, ioHandler.handleFileOutput(gcsService, samFile, referenceName)));

                    srcFilesPaths.forEach(FileUtils::deleteFile);
                    FileUtils.deleteFile(samFile);
                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
            } finally {
                FileUtils.deleteDir(workingDir);
            }
        }

    }
}
