package de.tum.i13.mqttbandwith;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Subscriber {
    public static void main(String[] args) throws MqttException, InterruptedException {

        MqttClient cl = new MqttClient("tcp://131.159.52.29:1883", "Subscriber", new MemoryPersistence());
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setUserName("manit");
        mqttConnectOptions.setPassword("manit".toCharArray());

        SusbscriberHandler susbscriberHandler = new SusbscriberHandler(cl);
        cl.setCallback(susbscriberHandler);
        cl.connect(mqttConnectOptions);
//        cl.connect();
        susbscriberHandler.startSubscribing();

        if (cl.isConnected())
            System.out.println("SamplingBroker started press [CTRL+C] to stop");
        //Bind  a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Stopping broker");
                try {
                    cl.connect(mqttConnectOptions);
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