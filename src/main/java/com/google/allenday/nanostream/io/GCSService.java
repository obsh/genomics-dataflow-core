package com.google.allenday.genomics.core.io;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Provides access to {@link Storage} instance with convenient interface
 */
public class GCSService {

    private Logger LOG = LoggerFactory.getLogger(GCSService.class);

    private Storage storage;

    public GCSService(Storage storage) {
        this.storage = storage;
    }

    public static GCSService initialize() {
        return new GCSService(StorageOptions.getDefaultInstance().getService());
    }

    public Blob getBlob(String bucketName, String blobName) throws StorageException {
        return storage.get(BlobId.of(bucketName, blobName));
    }

    public Blob saveToGcs(String bucketName, String blobName, byte[] content) {
        return storage.create(BlobInfo.newBuilder(bucketName, blobName).build(), content);
    }

    public String getUriFromBlob(Blob blob) {
        return String.format("gs://%s/%s", blob.getBucket(), blob.getName());
    }

    public Blob copy(String oldBucketName, String oldBlobName, String newBucketName, String newBlobName) {
        BlobId srcBlobId = BlobId.of(oldBucketName, oldBlobName);
        BlobId destBlobId = BlobId.of(newBucketName, newBlobName);
        CopyWriter copyWriter = storage.copy(Storage.CopyRequest.newBuilder()
                .setSource(srcBlobId)
                .setTarget(destBlobId)
                .build());
        return copyWriter.getResult();
    }

    public Pair<String, String> getBlobElementsFromUri(String uri) {
        try {
            String workPart = uri.split("//")[1];
            String[] parts = workPart.split("/");
            String bucket = parts[0];
            String name = workPart.replace(bucket + "/", "");
            return Pair.with(bucket, name);
        } catch (Exception e) {
            return Pair.with("", "");
        }
    }

    public ReadChannel getBlobReaderByGCloudNotificationData(String bucketName, String blobName) throws StorageException {
        return storage.reader(BlobId.of(bucketName, blobName));
    }


    public Page<Blob> getListOfBlobsInDir(String bucketName, String dirPrefix) throws StorageException {
        return storage.list(bucketName, Storage.BlobListOption.prefix(dirPrefix));
    }

    public Blob composeBlobs(Iterable<BlobId> blobIds, BlobId headers, BlobId destBlob) throws StorageException {
        Storage.ComposeRequest composeRequest = Storage.ComposeRequest
                .newBuilder()
                .addSource(headers.getName())
                .addSource(StreamSupport.stream(blobIds.spliterator(), false)
                        .map(BlobId::getName).collect(Collectors.toList()))
                .setTarget(BlobInfo.newBuilder(destBlob).build())
                .build();
        return storage.compose(composeRequest);
    }


    public boolean isExists(BlobId blobId) {
        return Optional.ofNullable(storage.get(blobId)).map(Blob::exists).orElse(false);
    }

    public List<Blob> getAllBlobsIn(String bucketName, String prefix) {
        Bucket bucket = storage.get(bucketName);
        return StreamSupport.stream(bucket.list(Storage.BlobListOption.prefix(prefix)).iterateAll().spliterator(), false)
                .collect(Collectors.toList());
    }


    public void downloadBlobTo(Blob blob, String filePath) {
        LOG.info(String.format("Start downloading blob gs://%s/%s with size %d into %s", blob.getBucket(), blob.getName(), blob.getSize(), filePath));
        blob.downloadTo(Paths.get(filePath));
        LOG.info(String.format("Blob gs://%s/%s successfully downloaded into %s", blob.getBucket(), blob.getName(), filePath));
    }

    public String readBlob(String bucketName, String blobName) throws IOException {
        Blob blob = getBlob(bucketName, blobName);
        ReadChannel reader = blob.reader();

        ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
        StringBuilder builder = new StringBuilder();
        while (reader.read(bytes) > 0) {
            bytes.flip();
            builder.append(StandardCharsets.UTF_8.decode(bytes).toString());
            bytes.clear();
        }
        reader.close();
        return builder.toString();
    }
}