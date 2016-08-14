package de.tum.i13;

import de.tum.i13.pb.Debs2015Protos;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

public class test {


    public static class CsvToProtobuf {
        public static void main(String args[]) {
            try {
//                LineIterator it = FileUtils.lineIterator(new File("/Users/manit/Projects/sdcbenchmark/Dataset/debscsv"), "UTF-8");
//                FileOutputStream out = new FileOutputStream("/Users/manit/Projects/sdcbenchmark/Dataset/debsprotobuf",true);
                FileInputStream in = new FileInputStream(new File("/Users/manit/Projects/sdcbenchmark/Dataset/debsprotobuf"));
                Debs2015Protos.Taxitrip.Builder builder = Debs2015Protos.Taxitrip.newBuilder();
                while (builder.mergeDelimitedFrom(in) == true) {

                    System.out.println(builder.getMedallion());
                    System.out.println(builder.getHackLicense());
                    System.out.println(builder.getPickupDatetime());
                    System.out.println(builder.getDropoffDatetime());
                    System.out.println(builder.getTripTimeInSecs());
                    System.out.println(builder.getTripDistance());
                    System.out.println(builder.getPickupLongitude());
                    System.out.println(builder.getPickupLatitude());
                    System.out.println(builder.getDropoffLongitude());
                    System.out.println(builder.getDropoffLatitude());
                    System.out.println(builder.getPaymentType());
                    System.out.println(builder.getFareAmount());
                    System.out.println(builder.getSurcharge());
                    System.out.println(builder.getMtaTax());
                    System.out.println(builder.getTipAmount());
                    System.out.println(builder.getTollsAmount());
                    System.out.println(builder.getTotalAmount());

//                        byte[] temp = builder.build().toByteArray();
//                    System.out.println(Arrays.toString(temp));

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
