package de.tum.i13.mqttbandwith;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.File;

public class CsvPublisherHandlerUncompressed extends Thread implements MqttCallback {

    private final MqttClient cl;
    private LineIterator it;

    public CsvPublisherHandlerUncompressed(MqttClient cl, LineIterator it) {

        this.cl = cl;
        this.it = it;
    }


    public void startSendingMessages() throws Exception {
        try {
            while (true && !Thread.interrupted()) {
                if (it.hasNext()) {

                    String next = (String) it.next();
                    byte[] payload = next.getBytes();
                    byte[] msg = new byte[payload.length + 1];
                    msg[0] = -5;
                    System.arraycopy(payload, 0, msg, 1, payload.length);
                    this.cl.publish(Const.CSV_UNCOMPRESSED_TOPIC_NAME, msg, 0, false);
//                System.out.println("#published uncompressed:" + msg.length);
//                Thread.sleep(25);

                } else {
                    it = FileUtils.lineIterator(new File(Const.CSV_INPUT_FILE_PATH), "UTF-8");
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

    @Override
    public void run() {
        try {
            startSendingMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
