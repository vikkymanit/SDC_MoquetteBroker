package de.tum.i13.mqttbandwith;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;

public class SusbscriberHandler implements MqttCallback {
    private MqttClient cl;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;
    int count = 0;

    public SusbscriberHandler(MqttClient cl) {
        this.cl = cl;
        dictionaries = new Hashtable<>();
    }

    public void startSubscribing() throws MqttException {
        this.cl.subscribe(Const.DICT_TOPIC_NAME);
//        this.cl.subscribe(Const.XML_COMPRESSED_TOPIC_NAME);
//        this.cl.unsubscribe(Const.XML_COMPRESSED_TOPIC_NAME);
    }

    @Override
    public void connectionLost(Throwable throwable) {

    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        byte[] msgPayload = mqttMessage.getPayload();
        byte id = msgPayload[0];
        byte[] payload = Arrays.copyOfRange(msgPayload, 1, msgPayload.length);

        count++;
        if (s.equalsIgnoreCase(Const.DICT_TOPIC_NAME)) {
            FemtoZipCompressionModel femtoZipCompressionModel1 = FemtoFactory.fromDictionary(payload);

            dictionaries.put(id, femtoZipCompressionModel1);
        }
        if (s.equalsIgnoreCase(Const.XML_COMPRESSED_TOPIC_NAME)) {
            if (id == -1) {
                System.out.println("#uncompressedMessage:" + count + ":" + payload.length);
            } else if (id == -2) {
                FemtoZipCompressionModel femtoZipCompressionModel = dictionaries.get(id);
                byte[] uncompressedMessage = femtoZipCompressionModel.decompress(payload);
                System.out.println("#compressedMessage:" + count + ":" + payload.length);
            }
        }

        if (count == 3) {
            System.out.println("Checking");
//            cl.disconnect();
            cl = new MqttClient("tcp://localhost:1883", "Subscriber", new MemoryPersistence());
            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            mqttConnectOptions.setUserName("manit");
            mqttConnectOptions.setPassword("manit".toCharArray());
            cl.connect(mqttConnectOptions);
            cl.subscribe(Const.XML_COMPRESSED_TOPIC_NAME);
            System.out.println("Working fine");
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
}
