package de.tum.i13;

import net.openhft.affinity.AffinityLock;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) throws IOException {
        Configuration config = getConfig();
        process(config);
        //ArrayList<byte[]> xmlNodes = XMLRowParser.getNodes(config.getXmlFiles()[0]);
        //new DiffAlternative(xmlNodes, config.getXmlFiles()[0], Const.DATA_TYPE_XML, config).run();
    }

    private static Configuration getConfig() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            if (hostname.equalsIgnoreCase("5e195d3a1ce5")) {
                Configuration config = new Configuration("/HDD/doblander/sdcdata", "/HDD/doblander/sdcresults6");
                config.setMaxAmountOfData(20000);
                config.setExecutionThreads(32);
                return config;
            } else if (hostname.equalsIgnoreCase("xr2d2")) {
                Configuration config = new Configuration("/home/chris/datasets/sdcdata", "/home/chris/datasets/sdcresults");
                config.setMaxAmountOfData(10000);
                config.setExecutionThreads(1);
                return config;
            } else if (hostname.equalsIgnoreCase("pc-jacobsen-sigma")) {
                Configuration config = new Configuration("/home/doblander/datasets/sdcdata", "/home/doblander/datasets/sdcresults7");
                config.setMaxAmountOfData(20000);
                config.setExecutionThreads(12); //Machine has 112 cores
                return config;
            }

        } catch (UnknownHostException e) {
            return null;
        }
        return null;
    }

    private static void process(Configuration config) {
        System.out.println("Executing with " + config.getExecutionThreads() + " threads");

        ArrayList<byte[]> debsProtobufs = null;

        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(config.getExecutionThreads());

        List<Pair<String, ArrayList<byte[]>>> xmlnodeFiles = new ArrayList<>();
        for (String file : config.getXmlFiles()) {
            ArrayList<byte[]> xmlNodes = XMLRowParser.getNodes(file);
            xmlnodeFiles.add(new Pair(file, xmlNodes));
        }

        List<Pair<String, ArrayList<byte[]>>> lineBasedFiles = new ArrayList<>();
        for (String file : config.getLineBasedData()) {
            ArrayList<byte[]> jsonData = new DataParser(config.getMaxAmountOfData()).processLineBased(file);
            lineBasedFiles.add(new Pair(file, jsonData));
        }

        List<Pair<String, ArrayList<byte[]>>> twitterDatas = new ArrayList<>();
        for (String file : config.getTwitterFiles()) {
            final ArrayList<byte[]> twitterJsonData = new DataParser(config.getMaxAmountOfData()).processLineBased(file);
            final ArrayList<byte[]> tweets = DataParser.extractTweets(twitterJsonData);

            twitterDatas.add(new Pair(file, tweets));
        }


        //Deflate
        for (Pair<String, ArrayList<byte[]>> xml : xmlnodeFiles) {
            executor.execute(new DeflateAlternative(xml.getValue(), xml.getKey(), Const.DATA_TYPE_XML, config));
            executor.execute(new DiffAlternative(xml.getValue(), xml.getKey(), Const.DATA_TYPE_XML, config));
        }
        for (Pair<String, ArrayList<byte[]>> xml : lineBasedFiles) {
            executor.execute(new DeflateAlternative(xml.getValue(), xml.getKey(), Const.DATA_TYPE_JSON, config));
            executor.execute(new DiffAlternative(xml.getValue(), xml.getKey(), Const.DATA_TYPE_JSON, config));
        }
        for (Pair<String, ArrayList<byte[]>> twitterData : twitterDatas) {
            executor.execute(new DeflateAlternative(twitterData.getValue(), twitterData.getKey(), Const.DATA_TYPE_TWEET, config));
            executor.execute(new DiffAlternative(twitterData.getValue(), twitterData.getKey(), Const.DATA_TYPE_TWEET, config));
        }

        if (config.getDebs2015CSV() != null) {
            ArrayList<byte[]> debsTripsCsv = new DataParser(config.getMaxAmountOfData()).processLineBased(config.getDebs2015CSV());
            debsProtobufs = new Debs2015Protobuf().convertDebsCsvToProtoBuf(debsTripsCsv);
            executor.execute(new DeflateAlternative(debsProtobufs, "debs2015.csv", Const.DATA_TYPE_PB, config));
            executor.execute(new DiffAlternative(debsProtobufs, "debs2015.csv", Const.DATA_TYPE_PB, config));
        }


        //SDC Stuff
/*        for (int i : config.windowSizes()) {
            for (int j : config.windowSizes()) {
                for(double dictMultiplier: config.dictionaryMessageSizeMultiplier()) {
                    for(Pair<String, ArrayList<byte[]>> xml:  xmlnodeFiles) {
                        executor.execute(getRunnable(i, j, dictMultiplier, xml.getValue(), xml.getKey(), Const.DATA_TYPE_XML, config));
                    }

                    for(Pair<String, ArrayList<byte[]>> linebased: lineBasedFiles) {
                        executor.execute(getRunnable(i, j, dictMultiplier, linebased.getValue(), linebased.getKey(), Const.DATA_TYPE_JSON, config));
                    }

                    for(Pair<String, ArrayList<byte[]>> twitterData: twitterDatas) {
                        executor.execute(getRunnable(i, j, dictMultiplier, twitterData.getValue(), twitterData.getKey(), Const.DATA_TYPE_TWEET, config));
                    }

                    if(debsProtobufs != null) {
                        executor.execute(getRunnable(i, j, dictMultiplier, debsProtobufs, "debs2015.csv", Const.DATA_TYPE_PB, config));
                    }
                }
            }
        }
*/
        executor.shutdown();
        while (!executor.isTerminated()) {
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Finished all threads");
        System.out.println("Executed took " + (endTime - startTime) / 1000 + " s with " + config.getExecutionThreads() + " threads");
    }

    private static Runnable getRunnable(final int updateFreq, final int dictBufferSize, double dictMultiplier, final ArrayList<byte[]> dataSet,
                                        final String fileName, final String dataType, Configuration config) {
        return new Runnable() {
            @Override
            public void run() {

                AffinityLock al = AffinityLock.acquireCore();
                try {

                    StopWatch sw = new StopWatch();
                    sw.start();

                    ArrayList<byte[]> localData = (ArrayList<byte[]>) dataSet.clone();

                    DictionaryModel dictionaryModel;
                    File f = new File(fileName);
                    String name = f.getName().replace(Const.DELIMITER_UNDERSCORE, "-").replace('.', '-');
                    String dataInfoFileName = dataType + Const.DELIMITER_UNDERSCORE + name + Const.DELIMITER_UNDERSCORE +
                            updateFreq + Const.DELIMITER_UNDERSCORE + dictBufferSize + Const.DELIMITER_UNDERSCORE + dictMultiplier;

                    System.out.println("Simulating: " + dataInfoFileName);


                    File resultFile = new File(config.getResultDirectory(), dataInfoFileName + ".csv");

                    try {
                        //if (!resultFile.exists() && !resultFile.isDirectory()) {
                        dictionaryModel = new DictionaryModel(dictBufferSize, updateFreq, dictMultiplier, resultFile, config);

                        int datasent = 0;
                        int maxAmountOfData = config.getMaxAmountOfData();
                        Iterator<byte[]> data = localData.iterator();

                        while (datasent < maxAmountOfData) {
                            byte[] next;
                            try {
                                next = data.next();
                            } catch (NoSuchElementException ex) {
                                data = localData.iterator();
                                next = data.next();
                            }

                            dictionaryModel.sendData(next);
                            datasent += 1;
                        }
                        dictionaryModel.close();
                        //}
                    } catch (Exception e) {
                        e.printStackTrace();

                        resultFile.delete();
                    }
                    sw.stop();
                    System.out.println("#info:finsished #dataType:" + dataType + " #updateFreq:" + updateFreq + " #buffersize:" + dictBufferSize + " #multiplier: " + dictMultiplier + " #filename:" + dataInfoFileName + " #duration:" + sw.toString());
                } finally {
                    al.release();
                }
            }
        };
    }

}
