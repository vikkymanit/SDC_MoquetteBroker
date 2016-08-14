package de.tum.i13.mqttbandwith;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.File;
import java.util.Dictionary;
import java.util.Hashtable;

public class JsonPublisherHandlerCompressed extends Thread implements MqttCallback {

    private final MqttClient cl;
    private LineIterator it;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;


    public JsonPublisherHandlerCompressed(MqttClient cl, LineIterator it) throws Exception {
        this.cl = cl;
        this.it = it;
        dictionaries = new Hashtable<>();
        getDictionaries();
    }


    public void startSendingMessages() {
        try {
            while (true && !Thread.interrupted()) {

                if (it.hasNext()) {

                    String next = (String) it.next();

                    FemtoZipCompressionModel femtoZipCompressionModel = dictionaries.get((byte) 2);

                    byte[] payload = next.getBytes();
                    byte[] compressedMessage = femtoZipCompressionModel.compress(payload);
                    byte[] msg = new byte[compressedMessage.length + 1];
                    msg[0] = -4;
                    System.arraycopy(compressedMessage, 0, msg, 1, compressedMessage.length);
                    this.cl.publish(Const.JSON_COMPRESSED_TOPIC_NAME, msg, 0, false);

//                System.out.println("#published compressed:" + msg.length);

//                Thread.sleep(25);
                } else {
                    it = FileUtils.lineIterator(new File(Const.JSON_INPUT_FILE_PATH), "UTF-8");
                }
            }
        } catch (Exception e) {

        }
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

    public void getDictionaries() throws Exception {
        dictionaries.put((byte) 1, FemtoFactory.fromDictionary(FemtoFactory.fromCacheXml()));
        dictionaries.put((byte) 2, FemtoFactory.fromDictionary(FemtoFactory.fromCacheJson()));
        dictionaries.put((byte) 3, FemtoFactory.fromDictionary(FemtoFactory.fromCacheCsv()));
        System.out.println("Finished loading dictionaries");
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
