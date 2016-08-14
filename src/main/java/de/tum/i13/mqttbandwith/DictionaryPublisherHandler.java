package de.tum.i13.mqttbandwith;

import io.moquette.proto.messages.AbstractMessage;
import io.moquette.proto.messages.PublishMessage;
import io.moquette.server.Server;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import sun.jvm.hotspot.debugger.win32.coff.COFFLineNumber;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class DictionaryPublisherHandler implements MqttCallback {

    private final MqttClient cl;


    public DictionaryPublisherHandler() throws MqttException {
        cl = new MqttClient("tcp://127.0.0.1:1883", "Publisher", new MemoryPersistence());
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setUserName("manit");
        mqttConnectOptions.setPassword("manit".toCharArray());
        cl.connect(mqttConnectOptions);
    }


    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("Publisherhandler-connectionLost");
        throwable.printStackTrace();
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }

    public void sendDictionary() throws Exception {


        byte[] dictionary;
        try {
//                LineIterator it = FileUtils.lineIterator(new File("/Users/manit/Projects/sdc_benchmark2/Dataset/debsxml"), "UTF-8");
//                System.out.println("Creating dictionary");
//
//                ArrayList<byte[]> temp = new ArrayList<>();
//                while(it.hasNext()) {
//                    String next = (String)it.next();
//                    byte[] msgBytes = next.getBytes();
//                    temp.add(msgBytes);
//                    count++;
//                }
//
//                FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
//
//                System.out.println("#sampling-a-dictionary");
//
//                //Build the dictionary
//                femtoZipCompressionModel.setMaxDictionaryLength(calcDictionarySize());
//                femtoZipCompressionModel.build(new ArrayDocumentList(temp));
//
//                System.out.println("#sampling-dictionary-completed");
//
//                FemtoFactory.toCache(femtoZipCompressionModel);
//

//
//                isDictionaryCreated = true;


            //publish xml dictionary
            dictionary = FemtoFactory.fromCacheXml();
            byte[] dictMesage = new byte[dictionary.length + 1];
            dictMesage[0] = (byte) 1;
            for (int i = 0; i < dictionary.length; i++) {
                dictMesage[i + 1] = dictionary[i];
            }

            this.cl.publish(Const.DICT_TOPIC_NAME, dictMesage, 0, false);
            System.out.println("finished sending xml dictionary");


            //publish Json dictionary
            dictionary = FemtoFactory.fromCacheJson();
            dictMesage = new byte[dictionary.length + 1];
            dictMesage[0] = (byte) 2;
            for (int i = 0; i < dictionary.length; i++) {
                dictMesage[i + 1] = dictionary[i];
            }

            this.cl.publish(Const.DICT_TOPIC_NAME, dictMesage, 0, false);
            System.out.println("finished sending json dictionary");


            //publish csv dictionary
            dictionary = FemtoFactory.fromCacheCsv();
            dictMesage = new byte[dictionary.length + 1];
            dictMesage[0] = (byte) 3;
            for (int i = 0; i < dictionary.length; i++) {
                dictMesage[i + 1] = dictionary[i];
            }

            this.cl.publish(Const.DICT_TOPIC_NAME, dictMesage, 0, false);
            System.out.println("finished sending csv dictionary");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
