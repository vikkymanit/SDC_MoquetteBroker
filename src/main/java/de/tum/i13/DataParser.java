package de.tum.i13;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DataParser {

    private final int maxlines;

    public DataParser(int maxlines) {
        this.maxlines = maxlines;
    }

    public ArrayList<byte[]> processLineBased(String fileName) {
        ArrayList<byte[]> tweetData = new ArrayList<>();

        try {
            InputStream fis = new FileInputStream(fileName);
            InputStreamReader isr = new InputStreamReader(fis, Charset.defaultCharset());
            BufferedReader br = new BufferedReader(isr);
            int currline = 0;
            String line;
            while ((line = br.readLine()) != null) {
                currline++;
                tweetData.add(line.getBytes(StandardCharsets.UTF_8));
                if (currline > this.maxlines) {
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return tweetData;
    }

    public static ArrayList<byte[]> extractTweets(List<byte[]> twitterData) {
        ArrayList<byte[]> tweets = new ArrayList<>();

        if (twitterData == null)
            return tweets;

        try {
            for (byte[] tweet : twitterData) {
                String str = new String(tweet, StandardCharsets.UTF_8);
                JSONObject jsonObject = new JSONObject(str);
                tweets.add(jsonObject.getString("text").getBytes(StandardCharsets.UTF_8));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }

        return tweets;
    }
}
