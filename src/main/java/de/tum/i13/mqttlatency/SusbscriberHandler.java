package de.tum.i13.mqttlatency;

import org.eclipse.paho.client.mqttv3.*;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;

public class SusbscriberHandler implements MqttCallback{
    private final MqttClient cl;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;


    public SusbscriberHandler(MqttClient cl) {
        this.cl = cl;
        dictionaries = new Hashtable<>();
    }

    public void startSubscribing() throws MqttException {
        this.cl.subscribe(Const.DICT_TOPIC_NAME);
        this.cl.subscribe(Const.TOPIC_NAME);
    }

    @Override
    public void connectionLost(Throwable throwable) {

    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        byte[] msgPayload = mqttMessage.getPayload();
        byte id = msgPayload[0];
        byte[] payload = Arrays.copyOfRange(msgPayload, 1, msgPayload.length);

        if(s.equalsIgnoreCase(Const.DICT_TOPIC_NAME)) {
            FemtoZipCompressionModel femtoZipCompressionModel1 = FemtoFactory.fromDictionary(payload);

            dictionaries.put(id, femtoZipCompressionModel1);
        }
        if(s.equalsIgnoreCase(Const.TOPIC_NAME)) {
            if(id == -1) {
                System.out.println("#uncompressedMessage:" + payload.length);
            }
            else if (id > 0) {
                FemtoZipCompressionModel femtoZipCompressionModel = dictionaries.get(id);
                byte[] uncompressedMessage = femtoZipCompressionModel.decompress(payload);
                System.out.println("#compressedMessage:" + payload.length);
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
}
