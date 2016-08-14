package de.tum.i13;

import de.tum.i13.pb.Debs2015Protos;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Debs2015Protobuf {


    public ArrayList<byte[]> convertDebsCsvToProtoBuf(ArrayList<byte[]> debscsv) {
        ArrayList<byte[]> protos = new ArrayList<>();

        for (byte[] bline : debscsv) {

            // Deal with the line
            try {
                String line = new String(bline, StandardCharsets.UTF_8);

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

                byte[] tripbytes = builder.build().toByteArray();
                protos.add(tripbytes);

            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("error parsing a line");
            }
        }

        return protos;
    }
}
