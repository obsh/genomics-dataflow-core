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
import org.javatuples.Pair;
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
                              String alignedSamPath,
                              String referenceName) {
        String minimapCommand = String.format(ALIGN_COMMAND_PATTERN, ioHandler.generateReferencePath(referenceName),
                String.join(" ", localFastqPaths), alignedSamPath);

        Pair<Boolean, Integer> result = cmdExecutor.executeCommand(minimapCommand);
        if (!result.getValue0()) {
            throw new AlignException(minimapCommand, result.getValue1());
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

                for (String referenceName : referenceNames) {
                    ioHandler.downloadReferenceIfNeeded(gcsService, referenceName);

                    String alignedSamName = geneExampleMetaData.getRun() + "_" + referenceName + SAM_FILE_PREFIX;
                    String alignedSamPath = workingDir + alignedSamName;
                    GeneData copiedGeneData = ioHandler.tryToFindInPrevious(gcsService, alignedSamName, referenceName);
                    if (copiedGeneData != null) {
                        c.output(KV.of(geneExampleMetaData, copiedGeneData));
                    } else {
                        try {
                            String samFile = alignFastq(srcFilesPaths, alignedSamPath, referenceName);
                            c.output(KV.of(geneExampleMetaData, ioHandler.handleFileOutput(gcsService, samFile, referenceName)));
                            FileUtils.deleteFile(samFile);
                        } catch (AlignException e) {
                            LOG.error(e.getMessage());
                        }
                    }
                    LOG.info(String.format("Free disk space: %d", FileUtils.getFreeDiskSpace()));
                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
            } finally {
                srcFilesPaths.forEach(FileUtils::deleteFile);
                FileUtils.deleteDir(workingDir);
            }
        }

    }

    public class AlignException extends RuntimeException {

        public AlignException(String command, int code) {
            super(String.format("Align command %s failed with code %d", command, code));
        }
    }
}
