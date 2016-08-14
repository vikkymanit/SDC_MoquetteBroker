package de.tum.i13.adaptive;

import de.tum.i13.Configuration;
import de.tum.i13.mqttbandwith.FemtoFactory;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.toubassi.femtozip.ArrayDocumentList;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;

public class AdaptivSampling {

    private final LineIterator it;
    private final CircularFifoQueue<Notification> messageHistory;
    private int dictMultiplicatorIdx;
    private final int firstbreak;
    private long nextEval;
    private int bufferSizeIdx;

    private NextStep nextStep;

    private final double minImprovement;

    private final double percentForEvaluation;
    private FemtoZipCompressionModel currentDictionary;

    private long currentTime;
    private int noAdaptions;
    private int adaptions;

    final int[] bufferSeries = new int[]{50, 100, 200, 300, 500, 800, 1300, 2100, 3400, 5500};
    final double[] dictSeries = new double[]{0.5, 1, 2, 3, 5, 8, 13, 21};
    private long payoffWhenNextUnsuccessful;
    private long uncompressed;
    private long compressed;

    public AdaptivSampling(LineIterator it) {
        this.it = it;
        messageHistory = new CircularFifoQueue<>(5500);
        dictMultiplicatorIdx = 1;
        firstbreak = 100;
        bufferSizeIdx = 1;

        nextEval = 0;

        percentForEvaluation = 0.3;

        minImprovement = 3; //at least two percent should the bandwith be reduced

        nextStep = NextStep.INCREASE_DICTIONARY;
        payoffWhenNextUnsuccessful = 10000;

        currentTime = 10;
        noAdaptions = 0;
        adaptions = 0;

        uncompressed = 0;
        compressed = 0;
    }

    public static void main(String[] args) throws IOException {

        Configuration config = new Configuration("/home/chris/datasets/sdcdata", "/home/chris/datasets/sdcresults");

        //LineIterator it = FileUtils.lineIterator(new File("/home/chris/datasets/sdcdata", "debs2015/debs2015.json"), "UTF-8");
        //new AdaptivSampling(it).run();

        LineIterator it = FileUtils.lineIterator(new File("/home/chris/datasets/sdcdata", "debs2015/debs2015.xml"), "UTF-8");
        new AdaptivSampling(it).run();
    }

    private long calculateRateBytesPerSecond() {
        if (messageHistory.size() < 2)
            return 0;
        else {
            long totalBytes = messageHistory.stream().mapToLong(not -> not.getPayload().length).sum();

            long duration = messageHistory.get(messageHistory.size() - 1).getArrivalTime() - messageHistory.get(0).getArrivalTime();
            return totalBytes / duration * 1000;
        }
    }

    private int calcAverageNotificationSize() {
        if (messageHistory.size() > 0) {
            OptionalDouble average = messageHistory.stream().mapToInt(not -> not.getPayload().length).average();
            return (int) average.getAsDouble();
        } else {
            return 0;
        }
    }

    private FemtoZipCompressionModel createDictionary(List<byte[]> documents, int maxSize) throws IOException {
        FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
        femtoZipCompressionModel.setMaxDictionaryLength(maxSize);
        femtoZipCompressionModel.build(new ArrayDocumentList(new ArrayList<>(documents)));

        return femtoZipCompressionModel;
    }

    private Double calculateCompressionRatioWithNewCompressionmodel(int dictSize, int maximumMessages) throws IOException {
        int forEvaluation = (int) ((double) maximumMessages * this.percentForEvaluation);
        List<Notification> reversed = messageHistory.stream().collect(Collectors.toList());
        Collections.reverse(reversed);
        List<Notification> finished = reversed.stream().limit(maximumMessages + forEvaluation).collect(Collectors.toList());


        List<byte[]> verification = finished.stream().limit(forEvaluation).map(not -> not.getPayload()).collect(Collectors.toList());
        List<byte[]> forDictionary = finished.stream().skip(forEvaluation).map(not -> not.getPayload()).collect(Collectors.toList());

        FemtoZipCompressionModel femtoZipCompressionModel = createDictionary(forDictionary, dictSize);

        long uncompressed_notifications = verification.stream().mapToLong(b -> b.length).sum();
        long compressed_notificatons = 0;
        for (byte[] uncompressed : verification) {
            byte[] compress = femtoZipCompressionModel.compress(uncompressed);
            compressed_notificatons += compress.length;
        }

        double savings = 100.0 - (100.0 / uncompressed_notifications * compressed_notificatons);

        return savings;
    }

    private FemtoZipCompressionModel sampleDictionary(int amountOfMessages, int dictSize) throws IOException {
        List<byte[]> forDictionary = messageHistory.stream().limit(amountOfMessages).map(not -> not.getPayload()).collect(Collectors.toList());

        FemtoZipCompressionModel dictionary = createDictionary(forDictionary, dictSize);
        return dictionary;
    }

    private void run() throws IOException {
        long begin = System.nanoTime();
        long duration = System.nanoTime();

        long count = 0;
        currentTime = 10;

        while (it.hasNext()) {
            count++;
            currentTime += 10;

            byte[] next = it.nextLine().getBytes();
            messageHistory.add(new Notification(currentTime, next));

            if (firstbreak == count) {
                int avg = calcAverageNotificationSize();
                long bytesPerSecond = calculateRateBytesPerSecond();

                begin = System.nanoTime();
                Double compressionratio = calculateCompressionRatioWithNewCompressionmodel((int) (avg * this.dictSeries[this.dictMultiplicatorIdx]), this.bufferSeries[this.bufferSizeIdx]);
                duration = System.nanoTime() - begin;
                System.out.println("#ty:firstestimation" + "#ts:" + currentTime + "#duration-ms:" + duration / 1000000);


                double savingPerSecond = bytesPerSecond * compressionratio / 100;

                begin = System.nanoTime();
                FemtoZipCompressionModel newDictionary = sampleDictionary(firstbreak, (int) (avg * dictMultiplicatorIdx));
                duration = System.nanoTime() - begin;
                System.out.println("#ty:firstnewdictionary" + "#ts:" + currentTime + "#duration-ms:" + duration / 1000000);

                byte[] dictSerialized = FemtoFactory.getDictionary(newDictionary);
                this.currentDictionary = newDictionary;
                this.uncompressed += dictSerialized.length;

                long payoffTimeMillis = (long) (((double) dictSerialized.length / savingPerSecond) * 1000.0) * 2;

                nextEval = currentTime + payoffTimeMillis;
                if (this.dictMultiplicatorIdx < this.dictSeries.length) {
                    dictMultiplicatorIdx = dictMultiplicatorIdx + 1;
                }
                if (this.bufferSizeIdx < this.bufferSeries.length) {
                    bufferSizeIdx = bufferSizeIdx + 1;
                }
            }

            if (nextEval < currentTime && this.messageHistory.size() > 100) {
                int avg = calcAverageNotificationSize();

                double currentBandWithSavings = calculateCurrentBandwithSavings();

                begin = System.nanoTime();
                Double newBandwithSavings = calculateCompressionRatioWithNewCompressionmodel((int) (avg * this.dictSeries[this.dictMultiplicatorIdx]), this.bufferSeries[this.bufferSizeIdx]);
                duration = System.nanoTime() - begin;
                System.out.println("#ty:newcompressionmodel" + "#ts:" + currentTime + "#duration-ms:" + duration / 1000000);


                System.out.println("#ty:bwsavings" + "#ts:" + currentTime + "#currentBandWithSavings:" + currentBandWithSavings + "#newSavings:" + newBandwithSavings);
                if (currentBandWithSavings < newBandwithSavings - this.minImprovement) {
                    //publish a new dictionary
                    adaptions += 1;
                    begin = System.nanoTime();
                    FemtoZipCompressionModel newDictionary = sampleDictionary(bufferSizeIdx, (int) (avg * dictMultiplicatorIdx));
                    duration = System.nanoTime() - begin;
                    System.out.println("#ty:sampledictionary" + "#ts:" + currentTime + "#duration-ms:" + duration / 1000000);

                    this.currentDictionary = newDictionary;

                    byte[] dictionarySerialized = FemtoFactory.getDictionary(newDictionary);
                    this.uncompressed += dictionarySerialized.length;

                    double gainFromNewDictionary = (newBandwithSavings - currentBandWithSavings);
                    long bytesPerSecond = calculateRateBytesPerSecond();

                    double savingPerSecond = bytesPerSecond * gainFromNewDictionary / 100;
                    long payoffTimeMillis = (long) (((double) dictionarySerialized.length / savingPerSecond) * 1000.0) * this.bufferSizeIdx * 10 * adaptions ^ 3;
                    //try it using a bigger dictionary
                    nextEval = currentTime + payoffTimeMillis;
                    payoffWhenNextUnsuccessful = payoffTimeMillis * 2;
                    noAdaptions = 0;

                    if (adaptions > 9) {
                        adaptions = 9;
                    }

                    System.out.println("#ty:newdictionary" + "#ts:" + currentTime + "#payoffTimeMillis:" + payoffTimeMillis + "#nexteval:" + nextEval + "#adaptions:" + adaptions + "#dictMultiplicatorIdx:" + this.dictSeries[this.dictMultiplicatorIdx] + "#bufferSizeIdx:" + this.bufferSeries[this.bufferSizeIdx] + "#currentBandWithSavings:" + currentBandWithSavings + "#newBandwithSavings:" + newBandwithSavings + "#payoffTimeMillis:" + payoffTimeMillis);
                } else {
                    adaptions = 0;
                    //long payoffTimeMillis = (long) (((double)this.currentDictionary.length * currentBandWithSavings) * 1000.0) * 2;
                    long payoffTimeMillis = payoffWhenNextUnsuccessful;
                    //try it using a bigger dictionary
                    nextEval = currentTime + payoffTimeMillis;

                    if (noAdaptions < 4) {

                        if (this.nextStep == NextStep.INCREASE_BUFFER) {
                            if (this.bufferSizeIdx < this.bufferSeries.length - 1) {
                                bufferSizeIdx = bufferSizeIdx + 1;
                            }

                            this.nextStep = NextStep.INCREASE_DICTIONARY;
                        } else if (this.nextStep == NextStep.INCREASE_DICTIONARY) {
                            if (this.dictMultiplicatorIdx < this.dictSeries.length - 1) {
                                dictMultiplicatorIdx = dictMultiplicatorIdx + 1;
                            }

                            this.nextStep = NextStep.INCREASE_BUFFER;
                        }
                        noAdaptions++;
                    }
                    System.out.println("#ty:nodictionary" + "#ts:" + currentTime + "#payoffTimeMillis:" + payoffTimeMillis + "#nexteval:" + nextEval + "#dictMultiplicatorIdx:" + this.dictSeries[this.dictMultiplicatorIdx] + "#bufferSizeIdx:" + this.bufferSeries[this.bufferSizeIdx] + "#noAdaptions:" + noAdaptions + "#currentTime:" + currentTime + "#payoffWhenNextUnsuccessful:" + payoffWhenNextUnsuccessful);

                    payoffWhenNextUnsuccessful *= 2;
                }
            }

            if (this.currentDictionary != null) {
                this.uncompressed += next.length;
                byte[] compressed = this.currentDictionary.compress(next);
                this.compressed += compressed.length + 1;
            }

            if ((count % 10000) == 0) {
                double currentSavings = (double) (100.0 - (100.0 / uncompressed * this.compressed));
                System.out.println("#ty:status" + "#ts:" + currentTime + "#currentSavings:" + currentSavings);
            }

            if (count > 1000000) {
                return;
            }
        }
    }

    private double calculateCurrentBandwithSavings() throws IOException {
        List<Notification> collect = this.messageHistory.stream().collect(Collectors.toList());
        Collections.reverse(collect);
        List<byte[]> msg = collect.stream().limit(500).map(not -> not.getPayload()).collect(Collectors.toList());

        return calculatePercentBandwithReduction(msg, this.currentDictionary);

    }

    private double calculatePercentBandwithReduction(List<byte[]> msg, FemtoZipCompressionModel femtoZipCompressionModel) {
        int totalSize = 0;
        int compressedSize = 0;
        for (byte[] not : msg) {
            byte[] compressed = femtoZipCompressionModel.compress(not);
            totalSize += not.length;
            compressedSize += compressed.length;
        }

        if (totalSize == 0)
            return 0.0;

        return 100 - (100.0 / totalSize * compressedSize);
    }
}