package de.tum.i13;

import java.io.BufferedWriter;
import java.io.IOException;

public class DataInfo {
    private final BufferedWriter bufferedWriter;

    public DataInfo(BufferedWriter bufferedWriter) {
        this.bufferedWriter = bufferedWriter;
    }

    public static final String DELIMITER_COMMA = ",";

    public void writeLine(int msgNo, int bufferSize, int updateFreq, byte[] data, byte[] encodedData, int dictionarylength, int currentAverageDictionarySize, long dictTime, long compresstime, long uncompressiontime) {
        int encodedDataSize = encodedData.length;
        int dataSize = data.length;
        String str = msgNo + DELIMITER_COMMA +
                bufferSize + DELIMITER_COMMA +
                updateFreq + DELIMITER_COMMA +
                dataSize + DELIMITER_COMMA +
                encodedDataSize + DELIMITER_COMMA +
                dictionarylength + DELIMITER_COMMA +
                currentAverageDictionarySize + DELIMITER_COMMA +
                dictTime + DELIMITER_COMMA +
                compresstime + DELIMITER_COMMA +
                uncompressiontime;
        try {
            this.bufferedWriter.write(str);
            this.bufferedWriter.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeHeader() {
        String header = "Msg no,Buffer Size,Update Freq,Original Data Size,Encoded data size,Dictionary Size,Max Dictionary Size,Dicttime(ns),Compresstime(ns),Uncompresstime(ns)";
        try {
            this.bufferedWriter.write(header);
            this.bufferedWriter.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void save() throws IOException {

    }

    public void close() {
        try {
            this.bufferedWriter.flush();
            this.bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}