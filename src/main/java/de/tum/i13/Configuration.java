package de.tum.i13;

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;

public class Configuration {
    private final String basedirectory;
    private final String resultDirectory;
    private int maxAmountOfData;
    private int executionThreads;

    public Configuration(String basedirectory, String resultDirectory) {
        this.basedirectory = basedirectory;
        this.resultDirectory = resultDirectory;
    }

    public String[] getXmlFiles() {
//        return new String[]{};
        return new String[]{new File(basedirectory, "xml/air_quality.xml").toString()};
    }

    public String[] getTwitterFiles() {
        //return new String[]{};
        return new String[]{new File(basedirectory, "twitter/NewYork-2015-2-23").toString()};
    }

    public String[] getLineBasedData() {
        //return new String[]{new File(basedirectory, "eex/epex.csv").toString()};


        return new String[]{new File(basedirectory, "twitter/NewYork-2015-2-23").toString(),
                new File(basedirectory, "debs2015/debs2015.json").toString(),
                new File(basedirectory, "debs2015/debs2015.xml").toString(),
                new File(basedirectory, "debs2015/debs2015.csv").toString(),
                new File(basedirectory, "eex/epex.csv").toString()};

    }

    public String getDebs2015CSV() {
//        return null;
        return new File(basedirectory, "debs2015/debs2015.csv").toString();
    }

    public double[] dictionaryMessageSizeMultiplier() {
        return new double[]{0.3, 0.5, 1, 2, 3, 5, 8, 13, 21};
    }

    public int[] windowSizes() {
        int[] windows = new int[]{50, 100, 200, 300, 500, 800, 1300, 2100, 3400, 5500};
        ArrayUtils.reverse(windows);
        return windows;
    }

    public String getResultDirectory() {
        return resultDirectory;
    }

    public void setMaxAmountOfData(int maxAmountOfData) {
        this.maxAmountOfData = maxAmountOfData;
    }

    public int getMaxAmountOfData() {
        return maxAmountOfData;
    }

    public int getExecutionThreads() {
        return executionThreads;
    }

    public void setExecutionThreads(int executionThreads) {
        this.executionThreads = executionThreads;
    }

}