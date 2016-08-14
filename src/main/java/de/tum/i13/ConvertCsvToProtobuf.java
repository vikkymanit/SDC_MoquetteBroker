package de.tum.i13;

import de.tum.i13.pb.Debs2015Protos;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

public class ConvertCsvToProtobuf {

    public static void main(String args[]) {
        try {
            LineIterator it = FileUtils.lineIterator(new File("/Users/manit/Projects/sdcbenchmark/Dataset/debscsv"), "UTF-8");
            FileOutputStream out = new FileOutputStream("/Users/manit/Projects/sdcbenchmark/Dataset/debsprotobuf", true);

            while (it.hasNext()) {

                String csvLine = (String) it.next();
                byte[] csvLineBytes = csvLine.getBytes();
                String line = new String(csvLineBytes, StandardCharsets.UTF_8);
                Debs2015Protos.Taxitrip.Builder builder = Debs2015Protos.Taxitrip.newBuilder();
                String[] splitted = line.split(",");

                builder.setMedallion(splitted[0]);
                builder.setHackLicense(splitted[1]);
                builder.setPickupDatetime(splitted[2]);
                builder.setDropoffDatetime(splitted[3]);
                builder.setTripTimeInSecs(Integer.parseInt(splitted[4]));
                builder.setTripDistance(Float.parseFloat(splitted[5]));
                builder.setPickupLongitude(Float.parseFloat(splitted[6]));
                builder.setPickupLatitude(Float.parseFloat(splitted[7]));
                builder.setDropoffLongitude(Float.parseFloat(splitted[8]));
                builder.setDropoffLatitude(Float.parseFloat(splitted[9]));
                builder.setPaymentType(splitted[10]);
                builder.setFareAmount(Float.parseFloat(splitted[11]));
                builder.setSurcharge(Float.parseFloat(splitted[12]));
                builder.setMtaTax(Float.parseFloat(splitted[13]));
                builder.setTipAmount(Float.parseFloat(splitted[14]));
                builder.setTollsAmount(Float.parseFloat(splitted[15]));
                builder.setTotalAmount(Float.parseFloat(splitted[16]));

                builder.build().writeDelimitedTo(out);
            }
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

