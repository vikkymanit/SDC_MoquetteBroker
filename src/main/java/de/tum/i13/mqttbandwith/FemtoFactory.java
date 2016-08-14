package de.tum.i13.mqttbandwith;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.*;

public class FemtoFactory {
    public static FemtoZipCompressionModel fromDictionary(byte[] dict) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(dict);
        DataInputStream dis = new DataInputStream(bis);

        FemtoZipCompressionModel compressionModel = new FemtoZipCompressionModel();
        compressionModel.load(dis);

        dis.close();

        return compressionModel;
    }

    public static byte[] getDictionary(FemtoZipCompressionModel compressionModel) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        compressionModel.save(dos);
        dos.flush();

        return bos.toByteArray();
    }

    public static File getDictCacheFile() {
        return new File("./Dictionary/dictProtobuf");
    }

    public static boolean isCachedDictionaryAvailable() {
        File dictFile = getDictCacheFile();
        return dictFile.exists() && !dictFile.isDirectory();
    }

    public static byte[] fromCache() throws IOException {
        return IOUtils.toByteArray(new FileInputStream(getDictCacheFile()));
    }

    public static byte[] fromCacheXml() throws IOException {
        return IOUtils.toByteArray(new FileInputStream(new File(Const.XML_DICTIONARY_PATH)));
    }

    public static byte[] fromCacheCsv() throws IOException {
        return IOUtils.toByteArray(new FileInputStream(new File(Const.CSV_DICTIONARY_PATH)));
    }

    public static byte[] fromCacheJson() throws IOException {
        return IOUtils.toByteArray(new FileInputStream(new File(Const.JSON_DICTIONARY_PATH)));
    }

    public static void toCache(FemtoZipCompressionModel compressionModel) throws IOException {
        FileUtils.writeByteArrayToFile(getDictCacheFile(), getDictionary(compressionModel));
    }

}
