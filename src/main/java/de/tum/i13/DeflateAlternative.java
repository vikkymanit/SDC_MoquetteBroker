package de.tum.i13;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class DeflateAlternative implements Runnable {


    private final List<byte[]> dataset;
    private final String fileName;
    private final String dataType;
    private final Configuration config;

    public DeflateAlternative(final List<byte[]> dataSet, final String fileName, final String dataType, Configuration config) {
        this.dataset = dataSet;
        this.fileName = fileName;
        this.dataType = dataType;
        this.config = config;
    }

    @Override
    public void run() {

        try {

            File f = new File(fileName);
            String name = f.getName().replace(Const.DELIMITER_UNDERSCORE, "-").replace('.', '-');
            String dataInfoFileName = dataType + Const.DELIMITER_UNDERSCORE + name + Const.DELIMITER_UNDERSCORE + "gzip";

            System.out.println("Simulating: " + dataInfoFileName);
            File resultFile = new File(config.getResultDirectory(), dataInfoFileName + ".csv");

            if (resultFile.exists() || resultFile.isDirectory()) {
                resultFile.delete();
            }

            BufferedWriter dataInfoBufferedWriter = new BufferedWriter(new FileWriter(resultFile, false));
            DataInfo dataInfo = new DataInfo(dataInfoBufferedWriter);
            dataInfo.writeHeader();

            int datasent = 0;
            Iterator<byte[]> data = dataset.iterator();


            while (datasent < config.getMaxAmountOfData()) {
                byte[] next;
                try {
                    next = data.next();
                } catch (NoSuchElementException ex) {
                    data = dataset.iterator();
                    next = data.next();
                }
                byte[] uncompressed = next;
                byte[] compressed = new byte[uncompressed.length];

                Deflater compresser = new Deflater();
                compresser.setInput(uncompressed);
                compresser.finish();

                long current = System.nanoTime(); //measure from setting to deflate
                int compressedDataLength = compresser.deflate(compressed);
                long compressiontime = System.nanoTime() - current;

                compresser.end();
                compressed = Arrays.copyOfRange(compressed, 0, compressedDataLength);


                Inflater decompresser = new Inflater();
                decompresser.setInput(compressed, 0, compressed.length);

                current = System.nanoTime();
                decompresser.inflate(uncompressed);
                long uncompressiontime = System.nanoTime() - current;

                decompresser.end();

                dataInfo.writeLine(datasent, 0, 0, uncompressed, compressed, 0, 0, 0, compressiontime, uncompressiontime);
                datasent++;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
