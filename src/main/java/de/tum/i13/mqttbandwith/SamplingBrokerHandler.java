package de.tum.i13.mqttbandwith;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.interception.messages.InterceptSubscribeMessage;
import io.moquette.interception.messages.InterceptUnsubscribeMessage;
import io.moquette.proto.messages.AbstractMessage;
import io.moquette.proto.messages.PublishMessage;
import io.moquette.server.Server;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.toubassi.femtozip.ArrayDocumentList;
import org.toubassi.femtozip.models.FemtoZipCompressionModel;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;

public class SamplingBrokerHandler extends AbstractInterceptHandler {

    private final Server mqttBroker;
    private final CircularFifoQueue<byte[]> cfb;
    private final Dictionary<Byte, FemtoZipCompressionModel> dictionaries;
    private long cnt;
    private byte dictionaryId;
    private boolean createDictionary;

    private boolean isDictionaryCreated = false;

    private Thread thread;

    public SamplingBrokerHandler(Server mqttBroker) {

        this.mqttBroker = mqttBroker;
        this.cfb = new CircularFifoQueue<>(500);
        dictionaries = new Hashtable<>();
        cnt = 0;
        dictionaryId = 1;
        createDictionary = false;
    }

    private int calcDictionarySize() {
        return 190; //todo make better
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        System.out.println("#onpublish:" + msg.getTopicName() + " #payload:" + msg.getPayload().array().length + "#cnt:" + cnt);

//        if (msg.getTopicName().equalsIgnoreCase(Const.TOPIC_NAME)) {
//
//            byte[] notification = msg.getPayload().array();
//            byte header = notification[0];
//            cnt++;
//
//            byte[] payload = Arrays.copyOfRange(notification, 1, notification.length);
//
//            if (header == -100) {
//                createDictionary = true;
//                System.out.println("#control-msg:" + header);
//            } else if (header < -98) {
//                System.out.println("#control-msg:" + header);
//                return;
//            }
//
//
//            if (header == -1) { //if -1 it's uncompressed
//                cfb.add(payload);
//            } else if (header > 0) { //if positive, it represents a new dictionary ID
////                FemtoZipCompressionModel femtoZipCompressionModel = this.dictionaries.get(header);
////                byte[] decompressed = femtoZipCompressionModel.decompress(payload);
////                cfb.add(decompressed);
//            }
//
//            if (createDictionary) { //every 100 messages we resample
//                byte[] dictionary = new byte[0];
//                try {
//                    FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
//                    if (!FemtoFactory.isCachedDictionaryAvailable()) {
//                        System.out.println("#sampling-a-dictionary");
//
//                        ArrayList<byte[]> temp = new ArrayList<>(cfb.size());
//
//                        for (int i = 0; i < cfb.size(); i++) {
//                            temp.add(cfb.get(i));
//                        }
//
//                        //Build the dictionary
//                        femtoZipCompressionModel.setMaxDictionaryLength(calcDictionarySize());
//                        femtoZipCompressionModel.build(new ArrayDocumentList(temp));
//
//                        System.out.println("#sampling-complete");
//
////                        Caching the sampled dictionary
//                        FemtoFactory.toCache(femtoZipCompressionModel);
//
//                        dictionary = FemtoFactory.getDictionary(femtoZipCompressionModel);
//
//                        System.out.println("Caching dictionary");
//                    } else {
//                        System.out.println("Loading dictionary from cache");
////                        femtoZipCompressionModel = FemtoFactory.fromCache();
//                        dictionary = FemtoFactory.fromCache();
//                    }
//
//
////                    old one
////                    System.out.println("#sampling-a-dictionary");
////                    ArrayList<byte[]> temp = new ArrayList<>(cfb.size());
////                    for(int i = 0; i < cfb.size(); i++) {
////                        temp.add(cfb.get(i));
////                    }
////
////                    //Build the dictionary
////                    FemtoZipCompressionModel femtoZipCompressionModel = new FemtoZipCompressionModel();
////                    femtoZipCompressionModel.setMaxDictionaryLength(calcDictionarySize());
////                    femtoZipCompressionModel.build(new ArrayDocumentList(temp));
//
////                    this.dictionaries.put(dictionaryId, femtoZipCompressionModel);
////                    byte[] dictionary = FemtoFactory.getDictionary(femtoZipCompressionModel);
//
//                    //publish the dictionary
//                    byte[] dictMesage = new byte[dictionary.length + 1];
//                    dictMesage[0] = dictionaryId;
//                    dictionaryId = (byte) ((dictionaryId + 1) % 127);
//                    for (int i = 0; i < dictionary.length; i++) {
//                        dictMesage[i + 1] = dictionary[i];
//                    }
//
//                    byte[] cmdb = new byte[1];
//                    cmdb[0] = -103;
//                    PublishMessage cmd = new PublishMessage();
//                    cmd.setTopicName(Const.DICT_TOPIC_NAME);
//                    cmd.setPayload(ByteBuffer.wrap(cmdb));
//                    cmd.setQos(AbstractMessage.QOSType.LEAST_ONE);
//                    this.mqttBroker.internalPublish(cmd);
//
//
//                    PublishMessage pm = new PublishMessage();
//                    pm.setTopicName(Const.DICT_TOPIC_NAME);
//                    pm.setPayload(ByteBuffer.wrap(dictMesage));
//                    pm.setQos(AbstractMessage.QOSType.LEAST_ONE);
//                    this.mqttBroker.internalPublish(pm);
//                    System.out.println("finished sampling dictionary");
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                createDictionary = false;
//            }
//
//        }
    }

    @Override
    public void onSubscribe(InterceptSubscribeMessage msg) {
        System.out.println("#onsubscribe: " + msg.getTopicFilter() + " #clientID:" + new String(msg.getClientID()));
//        if(msg.getTopicFilter().equals("debs2015_dc"))
//            createDictionary();

        //Testing
        if (thread != null)
            thread.interrupt();

//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        String topic = msg.getTopicFilter();

        switch (topic) {
            case Const.XML_COMPRESSED_TOPIC_NAME: {
                thread = new Thread() {
                    public void run() {
                        try {
                            Publisher.main(new String[]{Const.XML_COMPRESSED_TOPIC_NAME});
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };

                thread.start();
                break;
            }

            case Const.XML_UNCOMPRESSED_TOPIC_NAME: {
                thread = new Thread() {
                    public void run() {
                        try {
                            Publisher.main(new String[]{Const.XML_UNCOMPRESSED_TOPIC_NAME});
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                thread.start();
                break;
            }

            case Const.CSV_COMPRESSED_TOPIC_NAME: {
                thread = new Thread() {
                    public void run() {
                        try {
                            Publisher.main(new String[]{Const.CSV_COMPRESSED_TOPIC_NAME});
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };

                thread.start();
                break;
            }

            case Const.CSV_UNCOMPRESSED_TOPIC_NAME: {
                thread = new Thread() {
                    public void run() {
                        try {
                            Publisher.main(new String[]{Const.CSV_UNCOMPRESSED_TOPIC_NAME});
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                thread.start();
                break;
            }

            case Const.JSON_COMPRESSED_TOPIC_NAME: {
                thread = new Thread() {
                    public void run() {
                        try {
                            Publisher.main(new String[]{Const.JSON_COMPRESSED_TOPIC_NAME});
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };

                thread.start();
                break;
            }

            case Const.JSON_UNCOMPRESSED_TOPIC_NAME: {
                thread = new Thread() {
                    public void run() {
                        try {
                            Publisher.main(new String[]{Const.JSON_UNCOMPRESSED_TOPIC_NAME});
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                thread.start();
                break;
            }

            case Const.DICT_TOPIC_NAME: {
                thread = new Thread() {
                    public void run() {
                        try {
                            DictionaryPublisherHandler dictionaryPublisherHandler = new DictionaryPublisherHandler();
                            dictionaryPublisherHandler.sendDictionary();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                thread.start();
                break;
            }
        }
    }

    public void onUnsubscribe(InterceptUnsubscribeMessage msg) {

        System.out.println("#onUnsubscribe: " + msg.getTopicFilter() + " #clientID:" + new String(msg.getClientID()));
//        if(!msg.getTopicFilter().equalsIgnoreCase(Const.DICT_TOPIC_NAME))
        thread.interrupt();

    }

}
