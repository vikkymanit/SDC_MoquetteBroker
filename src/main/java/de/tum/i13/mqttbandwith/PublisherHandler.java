package de.tum.i13.mqttbandwith;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.eclipse.paho.client.mqttv3.*;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.File;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;

public class PublisherHandler implements MqttCallback, Runnable {

    private final MqttClient cl;
    private LineIterator it;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;
    private boolean hasSentBeginCompressionNotification;
    private boolean hasSentEndCompressionNotification;

    private int compressedCnt;
    private boolean hasSentBeginUnCompressionNotification;
    private boolean hasSentEndUnCompressionNotification;
    private int uncompressedCnt;

    private boolean hasReceivedDictionary = false;
    private boolean hasPublisherCompresssedStarted = false;
    private int dictCount = 0;

    public PublisherHandler(MqttClient cl, LineIterator it) {
        hasSentBeginCompressionNotification = false;
        hasSentEndCompressionNotification = false;
        hasSentBeginUnCompressionNotification = false;
        hasSentEndUnCompressionNotification = false;
        this.cl = cl;
        this.it = it;
        dictionaries = new Hashtable<>();
        compressedCnt = 0;
        uncompressedCnt = 0;
    }

    public void subscribe() throws MqttException {
        this.cl.subscribe(Const.DICT_TOPIC_NAME);
    }

    public void startSendingMessagesUncompressed() throws Exception {
        LineIterator it = FileUtils.lineIterator(new File("/Users/manit/Projects/sdcbenchmark/Dataset/debsxml"), "UTF-8");
        XmlPublisherHandlerUncompressed xmlPublisherHandlerUncompressed = new XmlPublisherHandlerUncompressed(cl, it);
        Thread thread = new Thread(xmlPublisherHandlerUncompressed);
        thread.start();
    }

    public void startSendingMessagesCompressed() throws Exception {
        LineIterator it = FileUtils.lineIterator(new File("/Users/manit/Projects/sdcbenchmark/Dataset/debsxml"), "UTF-8");
        XmlPublisherHandlerCompressed xmlPublisherHandlerCompressed = new XmlPublisherHandlerCompressed(cl, it);
        Thread thread = new Thread(xmlPublisherHandlerCompressed);
        thread.start();
    }

    public void startSendingMessages() throws Exception {
        it = FileUtils.lineIterator(new File("/Users/manit/Projects/sdcbenchmark/Dataset/debsxml"), "UTF-8");

        while (true && !Thread.interrupted()) {

            if (it.hasNext()) {

                String next = (String) it.next();

                //Publish Compressed
                FemtoZipCompressionModel femtoZipCompressionModel = dictionaries.get((byte) 1);

                byte[] payload = next.getBytes();
                byte[] compressedMessage = femtoZipCompressionModel.compress(payload);
                byte[] msg = new byte[compressedMessage.length + 1];
                msg[0] = -2;
                System.arraycopy(compressedMessage, 0, msg, 1, compressedMessage.length);
                this.cl.publish(Const.XML_COMPRESSED_TOPIC_NAME, msg, 0, false);

                System.out.println("#published compressed:" + msg.length);


                //Publish Uncompressed
//                msg = new byte[payload.length + 1];
//                msg[0] = -1;
//                System.arraycopy(payload, 0, msg, 1, payload.length);
//                this.cl.publish(Const.XML_UNCOMPRESSED_TOPIC_NAME, msg, 0, false);
//                System.out.println("#published uncompressed:" + msg.length);

            } else {
                it = FileUtils.lineIterator(new File("/Users/manit/Projects/sdcbenchmark/Dataset/debsxml"), "UTF-8");
            }
        }
    }

    public void getDictionaries() throws Exception {
        dictionaries.put((byte) 1, FemtoFactory.fromDictionary(FemtoFactory.fromCacheXml()));
        dictionaries.put((byte) 2, FemtoFactory.fromDictionary(FemtoFactory.fromCacheJson()));
        dictionaries.put((byte) 3, FemtoFactory.fromDictionary(FemtoFactory.fromCacheCsv()));
        System.out.println("Finished loading dictionaries");
    }


    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("Publisherhandler-connectionLost");
        throwable.printStackTrace();
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {

        byte[] msgPayload = mqttMessage.getPayload();
        byte id = msgPayload[0];
        if (id < -90) {
            return;
        }
        byte[] payload = Arrays.copyOfRange(msgPayload, 1, msgPayload.length);

        if (s.equalsIgnoreCase(Const.DICT_TOPIC_NAME)) {
            System.out.println("#got-dictionary:" + payload.length);
            FemtoZipCompressionModel femtoZipCompressionModel1 = FemtoFactory.fromDictionary(payload);
            dictionaries.put(id, femtoZipCompressionModel1);

            dictCount++;
            if (dictCount == 3)
                hasReceivedDictionary = true;
        }

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }

    @Override
    public void run() {
        try {
            startSendingMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
