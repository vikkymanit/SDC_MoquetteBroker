package de.tum.i13.mqttbandwith;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.File;

public class Publisher {

    public static void main(String[] args) throws Exception {

//        LineIterator it = FileUtils.lineIterator(new File("D:\\Projects\\datasets\\debs2015\\debs2015.xml"), "UTF-8");
//        LineIterator it = FileUtils.lineIterator(new File("/home/chris/datasets/sdcdata/debs2015/debs2015.xml"), "UTF-8");
        LineIterator xmlIt = FileUtils.lineIterator(new File(Const.XML_INPUT_FILE_PATH), "UTF-8");
        LineIterator csvIt = FileUtils.lineIterator(new File(Const.CSV_INPUT_FILE_PATH), "UTF-8");
        LineIterator jsonIt = FileUtils.lineIterator(new File(Const.JSON_INPUT_FILE_PATH), "UTF-8");

        //MqttClientPersistence s_dataStore;
//        MqttClient cl = new MqttClient("tcp://131.159.52.29:1883", "Publisher", new MemoryPersistence());
        MqttClient cl = new MqttClient("tcp://127.0.0.1:1883", "Publisher", new MemoryPersistence());
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setUserName("manit");
        mqttConnectOptions.setPassword("manit".toCharArray());


//        PublisherHandler publisherHandler = new PublisherHandler(cl, it);
//        publisherHandler.getDictionaries();

//        cl.setCallback(publisherHandler);

        cl.connect(mqttConnectOptions);

//        publisherHandler.subscribe();


        switch (args[0]) {

            case Const.XML_COMPRESSED_TOPIC_NAME: {
                XmlPublisherHandlerCompressed xmlPublisherHandlerCompressed = new XmlPublisherHandlerCompressed(cl, xmlIt);
                Thread th = new Thread(xmlPublisherHandlerCompressed);
                th.run();
                break;
            }

            case Const.XML_UNCOMPRESSED_TOPIC_NAME: {
                XmlPublisherHandlerUncompressed xmlPublisherHandlerUncompressed = new XmlPublisherHandlerUncompressed(cl, xmlIt);
                Thread th = new Thread(xmlPublisherHandlerUncompressed);
                th.run();
                break;
            }

            case Const.CSV_COMPRESSED_TOPIC_NAME: {
                CsvPublisherHandlerCompressed csvPublisherHandlerCompressed = new CsvPublisherHandlerCompressed(cl, csvIt);
                Thread th = new Thread(csvPublisherHandlerCompressed);
                th.run();
                break;
            }

            case Const.CSV_UNCOMPRESSED_TOPIC_NAME: {
                CsvPublisherHandlerUncompressed csvPublisherHandlerUncompressed = new CsvPublisherHandlerUncompressed(cl, csvIt);
                Thread th = new Thread(csvPublisherHandlerUncompressed);
                th.run();
                break;
            }

            case Const.JSON_COMPRESSED_TOPIC_NAME: {
                JsonPublisherHandlerCompressed jsonPublisherHandlerCompressed = new JsonPublisherHandlerCompressed(cl, jsonIt);
                Thread th = new Thread(jsonPublisherHandlerCompressed);
                th.run();
                break;
            }

            case Const.JSON_UNCOMPRESSED_TOPIC_NAME: {
                JsonPublisherHandlerUncompressed jsonPublisherHandlerUncompressed = new JsonPublisherHandlerUncompressed(cl, jsonIt);
                Thread th = new Thread(jsonPublisherHandlerUncompressed);
                th.run();
                break;
            }

        }


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
