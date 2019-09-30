package com.google.allenday.genomics.core.transform;

import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneExampleMetaData;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.IoHandler;
import htsjdk.samtools.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class SortFn extends DoFn<KV<GeneExampleMetaData, GeneData>, KV<GeneExampleMetaData, GeneData>> {

    private final static String SORTED_BAM_FILE_PREFIX = ".sorted.bam";

    private Logger LOG = LoggerFactory.getLogger(SortFn.class);
    private GCSService gcsService;

    private IoHandler ioHandler;

    public SortFn(IoHandler ioHandler) {
        this.ioHandler = ioHandler;
    }

    @Setup
    public void setUp() throws Exception {
        gcsService = GCSService.initialize();
    }

    private String sortFastq(String inputFilePath) throws IOException {
        String alignedSamName = FileUtils.getFilenameFromPath(inputFilePath);
        String alignedSortedBamPath = inputFilePath.replace(alignedSamName,
                FileUtils.changeFileExtension(alignedSamName, SORTED_BAM_FILE_PREFIX));

        final SamReader reader = SamReaderFactory.makeDefault().open(new File(inputFilePath));
        reader.getFileHeader().setSortOrder(SAMFileHeader.SortOrder.coordinate);

        SAMFileWriter samFileWriter = new SAMFileWriterFactory()
                .makeBAMWriter(reader.getFileHeader(), false, new File(alignedSortedBamPath));

        for (SAMRecord record : reader) {
            samFileWriter.addAlignment(record);
        }
        samFileWriter.close();
        reader.close();
        return alignedSortedBamPath;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Start of sort with input: %s", c.element().toString()));

        KV<GeneExampleMetaData, GeneData> input = c.element();
        GeneData geneData = input.getValue();
        GeneExampleMetaData geneExampleMetaData = input.getKey();


        if (geneExampleMetaData != null && geneData != null) {
            String workingDir = '/' + System.currentTimeMillis() + "_" + geneExampleMetaData.getRun() + "/";
            FileUtils.mkdir(workingDir);

            try {
                String destFilepath = workingDir + geneData.getFileName();
                String inputFilePath = ioHandler.handleInputAsLocalFile(geneData, gcsService, destFilepath);

                String alignedSortedBamPath = sortFastq(inputFilePath);

                c.output(KV.of(geneExampleMetaData, ioHandler.handleFileOutput(gcsService, alignedSortedBamPath, geneData.getReferenceName())));
            } catch (IOException e) {
                LOG.error(e.getMessage());
            } finally {
                FileUtils.deleteDir(workingDir);
            }
        }

    }
}
