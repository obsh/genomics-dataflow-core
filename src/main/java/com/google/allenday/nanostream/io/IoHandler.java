package com.google.allenday.genomics.core.io;

import com.google.allenday.genomics.core.gene.GeneData;
import com.google.cloud.storage.Blob;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;


public class IoHandler implements Serializable {

    private static Logger LOG = LoggerFactory.getLogger(IoHandler.class);

    private String srcBucket;
    private String resultsBucket;
    private String gcsReferenceDir;
    private String destGcsPrefix;
    private long memoryOutputLimitMb;

    public IoHandler(String srcBucket, String resultsBucket, String gcsReferenceDir, String destGcsPrefix, long memoryOutputLimitMb) {
        this.srcBucket = srcBucket;
        this.resultsBucket = resultsBucket;
        this.gcsReferenceDir = gcsReferenceDir;
        this.destGcsPrefix = destGcsPrefix;
        this.memoryOutputLimitMb = memoryOutputLimitMb;
    }

    public GeneData handleFileOutput(GCSService gcsService, String filepath, String referenceName) throws IOException {
        if (FileUtils.getFileSizeMegaBytes(filepath) > memoryOutputLimitMb) {
            return saveFileToGcsOutput(gcsService, filepath, referenceName);
        } else {
            String fileName = FileUtils.getFilenameFromPath(filepath);
            LOG.info(String.format("Pass %s file as RAW data", filepath));
            return new GeneData(GeneData.DataType.RAW, fileName).withReferenceName(referenceName).withRaw(FileUtils.readFileToByteArray(filepath));
        }
    }

    public GeneData saveFileToGcsOutput(GCSService gcsService, String filepath, String referenceName) throws IOException {
        String fileName = FileUtils.getFilenameFromPath(filepath);
        String gcsFilePath = destGcsPrefix + fileName;

        LOG.info(String.format("Export %s file to GCS %s", filepath, gcsFilePath));
        Blob blob = gcsService.saveToGcs(resultsBucket, gcsFilePath,
                Files.readAllBytes(Paths.get(gcsFilePath)));
        return new GeneData(GeneData.DataType.BLOB_URI, fileName).withReferenceName(referenceName).withBlobUri(gcsService.getUriFromBlob(blob));
    }

    public String handleInputAsLocalFile(GeneData geneData, GCSService gcsService, String destFilepath) throws IOException {
        if (geneData.getDataType() == GeneData.DataType.RAW) {
            FileUtils.saveDataToFile(geneData.getRaw(), destFilepath);
        } else if (geneData.getDataType() == GeneData.DataType.BLOB_URI) {
            Pair<String, String> blobElementsFromUri = gcsService.getBlobElementsFromUri(geneData.getBlobUri());
            gcsService.downloadBlobTo(gcsService.getBlob(blobElementsFromUri.getValue0(), blobElementsFromUri.getValue1()),
                    destFilepath);
        }
        return destFilepath;
    }

    public GeneData handleInputAndCopyToGcs(GeneData geneData, GCSService gcsService, String newFileName, String reference, String workDir) throws IOException {
        String gcsFilePath = destGcsPrefix + newFileName;
        Blob resultBlob;
        if (geneData.getDataType() == GeneData.DataType.RAW) {
            String filePath = workDir + newFileName;
            FileUtils.saveDataToFile(geneData.getRaw(), filePath);

            resultBlob = gcsService.saveToGcs(resultsBucket, gcsFilePath,
                    Files.readAllBytes(Paths.get(filePath)));
        } else if (geneData.getDataType() == GeneData.DataType.BLOB_URI) {
            resultBlob = gcsService.copy(srcBucket, geneData.getBlobUri(), resultsBucket, gcsFilePath);
        } else {
            throw new RuntimeException("Gene data type should be RAW or BLOB_URI");
        }
        return new GeneData(GeneData.DataType.BLOB_URI, newFileName).withReferenceName(reference).withBlobUri(gcsService.getUriFromBlob(resultBlob));
    }


    public void downloadReferenceIfNeeded(GCSService gcsService, String referenceName) {
        gcsService.getAllBlobsIn(srcBucket, gcsReferenceDir)
                .stream()
                .filter(blob -> blob.getName().contains(referenceName))
                .forEach(blob -> {
                    String filePath = generateReferenceDir(referenceName) + FileUtils.getFilenameFromPath(gcsService.getUriFromBlob(blob));
                    if (!Files.exists(Paths.get(filePath))) {
                        FileUtils.mkdir(filePath);
                        gcsService.downloadBlobTo(blob, filePath);
                    } else {
                        LOG.info(String.format("Reference %s already exists", blob.getName()));
                    }
                });
    }


    private String generateReferenceDir(String referenceName) {
        return gcsReferenceDir + referenceName + "/";
    }

    public String generateReferencePath(String referenceName) {
        return generateReferenceDir(referenceName) + referenceName + ".fa";
    }

}
