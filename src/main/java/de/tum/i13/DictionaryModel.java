package de.tum.i13;

import de.tum.i13.mqttbandwith.FemtoFactory;
import org.toubassi.femtozip.ArrayDocumentList;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DictionaryModel {
    private final DataInfo dataInfo;
    private FemtoZipCompressionModel model;

    private ArrayDocumentList trainingDocs = null;

    private CircularQueue msgBuffer;

    private CircularQueue movingAverageMessageSize;

    private int dictionaryUpdateFrequency;
    private final double maxDictionarySizeRelativeToMessage;

    private int dictionaryBufferSize;

    private int dictionaryBufferCount = 0;

    private int msgCount = 1;


    private byte[] dictionary;
    private int currentAverageDictionarySize;

    public DictionaryModel(int dictionaryBufferSize, int dictionaryUpdateFrequency, double maxDictionaryMultiplier, File dataInfoFileName, Configuration config) throws IOException {
        this.dictionaryBufferSize = dictionaryBufferSize;
        this.dictionaryUpdateFrequency = dictionaryUpdateFrequency;
        this.maxDictionarySizeRelativeToMessage = maxDictionaryMultiplier;
        msgBuffer = new CircularQueue(dictionaryBufferSize);
        movingAverageMessageSize = new CircularQueue(20);
        BufferedWriter dataInfoBufferedWriter = new BufferedWriter(new FileWriter(dataInfoFileName, false));
        dataInfo = new DataInfo(dataInfoBufferedWriter);
        dataInfo.writeHeader();

        this.currentAverageDictionarySize = 0;
    }

    private byte[] encode(byte[] data) {
        return model.compress(data);
    }

    private byte[] decode(byte[] data) {
        return model.decompress(data);
    }

    private int averageMessageSize() {

        int count = 0;
        int size = 0;

        for (byte[] str : msgBuffer.getAll()) {
            size += str.length;
            count += 1;
        }
        return size / count;
    }

    public void sendData(byte[] data) throws IOException {
        msgCount++;

        byte[] encodedData;

        msgBuffer.enqueue(data);
        movingAverageMessageSize.enqueue(data);

        long current = System.nanoTime();
        long dictTime = 0;

        if ((msgCount > dictionaryUpdateFrequency) && ((msgCount % dictionaryUpdateFrequency) == 1)) {
            int avgsize = averageMessageSize();
            currentAverageDictionarySize = (int) (avgsize * this.maxDictionarySizeRelativeToMessage);
            buildNewDictionary(currentAverageDictionarySize);
            dictTime = System.nanoTime() - current;
        }

        if (msgCount > dictionaryUpdateFrequency) {
            current = System.nanoTime();
            String encData = new String(data, StandardCharsets.UTF_8);

            encodedData = encode(data);

            long compressiontime = System.nanoTime() - current;

            current = System.nanoTime();
            byte[] decoded = decode(encodedData);
            String decData = new String(decoded, StandardCharsets.UTF_8);

            long uncompressiontime = System.nanoTime() - current;

            this.dataInfo.writeLine(msgCount, dictionaryBufferSize, dictionaryUpdateFrequency, data, encodedData, dictionary.length, currentAverageDictionarySize, dictTime, compressiontime, uncompressiontime);
        }
    }

    public void sendData(String str) throws IOException {
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        this.sendData(data);
    }

    private void buildNewDictionary(int dictionaryLength) throws IOException {
        trainingDocs = new ArrayDocumentList(msgBuffer.getAll());

        FemtoZipCompressionModel fmtCompressionModel = new FemtoZipCompressionModel();
        fmtCompressionModel.setMaxDictionaryLength(dictionaryLength);
        fmtCompressionModel.build(trainingDocs);

        model = fmtCompressionModel;
        dictionary = FemtoFactory.getDictionary(fmtCompressionModel);
        ;
    }

    public void close() throws IOException {
        this.dataInfo.close();
    }


}
