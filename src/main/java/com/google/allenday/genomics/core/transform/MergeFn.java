package com.google.allenday.genomics.core.transform;

import com.google.allenday.genomics.core.merge.BamFilesMerger;
import com.google.allenday.genomics.core.gene.GeneData;
import com.google.allenday.genomics.core.gene.GeneReadGroupMetaData;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.io.IoHandler;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MergeFn extends DoFn<KV<KV<GeneReadGroupMetaData, String>, Iterable<GeneData>>, KV<GeneReadGroupMetaData, GeneData>> {

    private final static String MERGE_SORTED_FILE_PREFIX = ".merged.sorted.bam";

    private Logger LOG = LoggerFactory.getLogger(MergeFn.class);
    private GCSService gcsService;

    private IoHandler ioHandler;
    private BamFilesMerger bamFilesMerger;

    public MergeFn(IoHandler ioHandler, BamFilesMerger bamFilesMerger) {
        this.ioHandler = ioHandler;
        this.bamFilesMerger = bamFilesMerger;
    }

    @Setup
    public void setUp() throws Exception {
        gcsService = GCSService.initialize();
    }

    private boolean isNeedToMerge(List<GeneData> geneDataList) {
        return geneDataList.size() > 1;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info(String.format("Merge of sort with input: %s", c.element().toString()));

        KV<KV<GeneReadGroupMetaData, String>, Iterable<GeneData>> input = c.element();
        KV<GeneReadGroupMetaData, String> geneReadGroupMetaDataAndReference = input.getKey();
        if (geneReadGroupMetaDataAndReference != null) {
            GeneReadGroupMetaData geneReadGroupMetaData = geneReadGroupMetaDataAndReference.getKey();
            String reference = geneReadGroupMetaDataAndReference.getValue();

            List<GeneData> geneDataList = StreamSupport.stream(input.getValue().spliterator(), false)
                    .collect(Collectors.toList());
            if (geneReadGroupMetaData != null && geneDataList.size() > 0) {
                String newFileName = geneReadGroupMetaData.getSraSample() + "_" + reference + MERGE_SORTED_FILE_PREFIX;
                String workDir = '/' + System.currentTimeMillis() + "_" + geneReadGroupMetaData.getSraSample() + "/";
                FileUtils.mkdir(workDir);

                try {
                    if (!isNeedToMerge(geneDataList)) {
                        GeneData geneData = ioHandler.handleInputAndCopyToGcs(geneDataList.get(0), gcsService, newFileName, reference, workDir);
                        c.output(KV.of(input.getKey().getKey(), geneData));
                    } else {
                        String outputPath = workDir + newFileName;

                        List<String> localBamPaths = new ArrayList<>();
                        for (GeneData geneData : geneDataList) {
                            localBamPaths.add(ioHandler.handleInputAsLocalFile(geneData,
                                    gcsService, workDir + geneData.getFileName()));
                        }
                        bamFilesMerger.merge(localBamPaths.stream().map(el -> Paths.get(el)).collect(Collectors.toList()), outputPath);
                        GeneData geneData = ioHandler.saveFileToGcsOutput(gcsService, outputPath, reference);
                        c.output(KV.of(input.getKey().getKey(), geneData));

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    FileUtils.deleteDir(workDir);
                }
            }

        }
    }
}
