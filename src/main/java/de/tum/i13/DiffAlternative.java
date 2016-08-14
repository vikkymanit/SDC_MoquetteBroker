package de.tum.i13;

import org.badiff.ByteArrayDiffs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DiffAlternative implements Runnable {
    private final List<byte[]> dataset;
    private final String fileName;
    private final String dataType;
    private final Configuration config;

    public DiffAlternative(final List<byte[]> dataSet, final String fileName, final String dataType, Configuration config) {
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
            String dataInfoFileName = dataType + Const.DELIMITER_UNDERSCORE + name + Const.DELIMITER_UNDERSCORE + "diff";

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


            byte[] last = new byte[0];
            while (datasent < config.getMaxAmountOfData()) {
                byte[] next;
                try {
                    next = data.next();
                } catch (NoSuchElementException ex) {
                    data = dataset.iterator();
                    next = data.next();
                }
                if (last.length != 0) {

                    long current = System.nanoTime(); //measure from setting to deflate
                    byte[] compressed = ByteArrayDiffs.diff(next, last); // unbidirectional diff
                    long compressiontime = System.nanoTime() - current;


                    current = System.nanoTime();
                    byte[] restored = ByteArrayDiffs.undo(last, compressed);
                    long uncompressiontime = System.nanoTime() - current;

                    if (compressed.length > last.length) //this prevents the case that diffs get bigger, in this case ship the uncompressed
                        compressed = last;


                    dataInfo.writeLine(datasent, 0, 0, restored, compressed, 0, 0, 0, compressiontime, uncompressiontime);
                    datasent++;
                }
                last = next;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
