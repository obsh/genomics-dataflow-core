package com.google.allenday.genomics.core.gene;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Objects;

//TODO

/**
 *
 */
@DefaultCoder(AvroCoder.class)
public class GeneExampleMetaData extends GeneReadGroupMetaData implements Serializable {

    private String run;

    public GeneExampleMetaData() {
    }

    public GeneExampleMetaData(String project, String projectId, String bioSample, String sraSample, String run) {
        super(project, projectId, bioSample, sraSample);
        this.run = run;
    }

    public String getRun() {
        return run;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GeneExampleMetaData that = (GeneExampleMetaData) o;
        return Objects.equals(run, that.run);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), run);
    }

    @Override
    public String toString() {
        return "GeneExampleMetaData{" +
                "run='" + run + '\'' +
                ", project='" + project + '\'' +
                ", projectId='" + projectId + '\'' +
                ", bioSample='" + bioSample + '\'' +
                ", sraSample='" + sraSample + '\'' +
                '}';
    }
}

