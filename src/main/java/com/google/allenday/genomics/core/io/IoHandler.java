package com.google.allenday.genomics.core.io;

import com.google.allenday.genomics.core.gene.GeneData;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
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
    private String previousDestGcsPrefix;
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

    public IoHandler withPreviousDestGcsPrefix(String previousDestGcsPrefix) {
        this.previousDestGcsPrefix = previousDestGcsPrefix;
        return this;
    }

    public GeneData handleFileOutput(GCSService gcsService, String filepath, String referenceName) throws IOException {
        if (FileUtils.getFileSizeMegaBytes(filepath) > memoryOutputLimitMb) {
            return saveFileToGcsOutput(gcsService, filepath, referenceName);
        } else {
            String fileName = FileUtils.getFilenameFromPath(filepath);
            LOG.info(String.format("Pass %s file as CONTENT data", filepath));
            return GeneData.fromByteArrayContent(FileUtils.readFileToByteArray(filepath), fileName).withReferenceName(referenceName);
        }
    }

    public GeneData saveFileToGcsOutput(GCSService gcsService, String filepath, String referenceName) throws IOException {
        String fileName = FileUtils.getFilenameFromPath(filepath);
        String gcsFilePath = destGcsPrefix + fileName;

        LOG.info(String.format("Export %s file to GCS %s", filepath, gcsFilePath));
        Blob blob = gcsService.writeToGcs(resultsBucket, gcsFilePath, filepath);
        return GeneData.fromBlobUri(gcsService.getUriFromBlob(blob), fileName).withReferenceName(referenceName);
    }

    public String handleInputAsLocalFile(GeneData geneData, GCSService gcsService, String destFilepath) throws IOException {
        if (geneData.getDataType() == GeneData.DataType.CONTENT) {
            FileUtils.saveDataToFile(geneData.getContent(), destFilepath);
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
        if (geneData.getDataType() == GeneData.DataType.CONTENT) {
            String filePath = workDir + newFileName;
            FileUtils.saveDataToFile(geneData.getContent(), filePath);

            resultBlob = gcsService.writeToGcs(resultsBucket, gcsFilePath, filePath);
        } else if (geneData.getDataType() == GeneData.DataType.BLOB_URI) {
            Pair<String, String> blobElementsFromUri = gcsService.getBlobElementsFromUri(geneData.getBlobUri());
            resultBlob = gcsService.copy(srcBucket, blobElementsFromUri.getValue1(), resultsBucket, gcsFilePath);
        } else {
            throw new RuntimeException("Gene data type should be CONTENT or BLOB_URI");
        }
        return GeneData.fromBlobUri(gcsService.getUriFromBlob(resultBlob), newFileName).withReferenceName(reference);
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

    public GeneData tryToFindInPrevious(GCSService gcsService, String alignedSamName, String reference) {
        BlobId previousBlobId = BlobId.of(resultsBucket, previousDestGcsPrefix + alignedSamName);
        LOG.info(String.format("Trying to find %s", previousBlobId.toString()));
        if (gcsService.isExists(previousBlobId)){
            LOG.info(String.format("File %s found in previous run bucket", alignedSamName));
            Blob resultBlob = gcsService.copy(previousBlobId.getBucket(), previousBlobId.getName(), resultsBucket, destGcsPrefix + alignedSamName);
            return GeneData.fromBlobUri(gcsService.getUriFromBlob(resultBlob), alignedSamName).withReferenceName(reference);
        } else {
            return null;
        }
    }
}
