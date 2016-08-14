package de.tum.i13.mqttlatency;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Subscriber {
    public static void main(String[] args) throws MqttException, InterruptedException {

        MqttClient cl = new MqttClient("tcp://localhost:1883", "Subscriber", new MemoryPersistence());

        SusbscriberHandler susbscriberHandler = new SusbscriberHandler(cl);
        cl.setCallback(susbscriberHandler);
        cl.connect();
        susbscriberHandler.startSubscribing();

        System.out.println("SamplingBroker started press [CTRL+C] to stop");
        //Bind  a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Stopping broker");
                try {
                    cl.disconnect();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
                System.out.println("SamplingBroker stopped");
            }
        });

        Thread.sleep(Long.MAX_VALUE);

        System.out.println("Quit");
    }
}