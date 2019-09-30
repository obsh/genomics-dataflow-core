package com.google.allenday.genomics.core.gene;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class GeneData implements Serializable {

    private DataType dataType;
    private String fileName;
    @Nullable
    private String blobUri;
    @Nullable
    private byte[] raw;
    @Nullable
    private String referenceName;

    public GeneData(DataType dataType, String fileName) {
        this.dataType = dataType;
        this.fileName = fileName;
    }

    @Nullable
    public String getBlobUri() {
        return blobUri;
    }

    @Nullable
    public byte[] getRaw() {
        return raw;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getFileName() {
        return fileName;
    }

    public GeneData withBlobUri(String blobUri) {
        this.blobUri = blobUri;
        return this;
    }

    public GeneData withRaw(byte[] raw) {
        this.raw = raw;
        return this;
    }

    public GeneData withReferenceName(String referenceName) {
        this.referenceName = referenceName;
        return this;
    }

    @Nullable
    public String getReferenceName() {
        return referenceName;
    }

    @DefaultCoder(SerializableCoder.class)
    public enum DataType {
        RAW, BLOB_URI
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeneData geneData = (GeneData) o;
        return dataType == geneData.dataType &&
                Objects.equals(fileName, geneData.fileName) &&
                Objects.equals(blobUri, geneData.blobUri) &&
                Arrays.equals(raw, geneData.raw) &&
                Objects.equals(referenceName, geneData.referenceName);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(dataType, fileName, blobUri, referenceName);
        result = 31 * result + Arrays.hashCode(raw);
        return result;
    }

    @Override
    public String toString() {
        return "GeneData{" +
                "dataType=" + dataType +
                ", fileName='" + fileName + '\'' +
                ", blobUri='" + blobUri + '\'' +
                ", rawSize=" + (raw != null ? String.valueOf(raw.length) : "0") +
                ", referenceName='" + referenceName + '\'' +
                '}';
    }
}
