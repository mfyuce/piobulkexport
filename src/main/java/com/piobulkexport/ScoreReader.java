package com.piobulkexport;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.ssl.SSLContextBuilder;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by myuce on 2.5.2016.
 */
public class ScoreReader extends Thread {
    private ConcurrentLinkedQueue<String> users = null;
    private ConcurrentLinkedQueue<String[]> scores = null;
    private int numRecommendations = 10;
    private String serviceUrl = "https://localhost:8000/queries.json";

    public ScoreReader(ConcurrentLinkedQueue<String> users, ConcurrentLinkedQueue<String[]> scores, int numRecommendations, String serviceUrl) {
        this.users = users;
        this.scores = scores;
        this.numRecommendations = numRecommendations;
        this.serviceUrl = serviceUrl;
    }

    public static List<String[]> getLines(String json, String[] users) throws IOException {

        List<String[]> lst = new LinkedList<>();
        JsonArray arr = new JsonParser().parse(json).getAsJsonObject().get("itemScores").getAsJsonArray();
        for (int i = 0; i < arr.size(); i++) {
            String[] l = new String[3];
            l[0] = users[0];
            l[1] = arr.get(i).getAsJsonObject().get("item").getAsString();
            l[2] = arr.get(i).getAsJsonObject().get("score").getAsString();
            lst.add(l);
        }
        return lst;
    }

    public static List<String[]> getLinesFromArray(String json, String[] users) throws IOException {

        List<String[]> lst = new LinkedList<>();
        JsonArray allArr = new JsonParser().parse(json).getAsJsonArray();
        for (int j = 0; j < allArr.size(); j++) {
            String user = users[j];
            JsonArray arr = allArr.get(j).getAsJsonObject().get("itemScores").getAsJsonArray();
            for (int i = 0; i < arr.size(); i++) {
                String[] l = new String[3];
                l[0] = user;
                l[1] = arr.get(i).getAsJsonObject().get("item").getAsString();
                l[2] = arr.get(i).getAsJsonObject().get("score").getAsString();
                lst.add(l);
            }
        }
        return lst;
    }

    public static CloseableHttpClient getClient() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        SSLContextBuilder sSLContextBuilder = new SSLContextBuilder();
        sSLContextBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
        SSLConnectionSocketFactory sSLConnectionSocketFactory = new SSLConnectionSocketFactory(sSLContextBuilder.build(),
                SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        return HttpClients.custom().setSSLSocketFactory(sSLConnectionSocketFactory).build();

    }
    /*public static String  download(String[] users) throws IOException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException {

        String urlParameters  = "";
        for (int i=0;i<users.length;i++   ){
            urlParameters+="{ \"user\": \"" +  users[i]+ "\", \"num\": 10 }";
        }

        return Request.Post(serviceUrl)
                .useExpectContinue()
                .version(HttpVersion.HTTP_1_1)
                .bodyString(urlParameters, ContentType.DEFAULT_TEXT)
                .execute().returnContent().asString();
    }*/

    public static String download1(CloseableHttpClient httpClient, String serviceUrl, String[] users, int numRecommendations) throws IllegalStateException, IOException, NoSuchAlgorithmException, KeyStoreException,
            KeyManagementException {

        HttpPost httpGet = null;
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(100000).build();
        String urlParameters = "";
        for (int i = 0; i < users.length; i++) {
            urlParameters += "{ \"user\": \"" + users[i] + "\", \"num\": " + numRecommendations + " }";
        }
        httpGet = new HttpPost(serviceUrl);

        StringEntity se = new StringEntity(urlParameters);
        se.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
        httpGet.setEntity(se);
        httpGet.setConfig(requestConfig);
        httpGet.setHeader(HTTP.CONTENT_TYPE, "application/json");
        CloseableHttpResponse response = null;

        try {
            response = httpClient.execute(httpGet);
            return IOUtils.toString(response.getEntity().getContent());
        } finally {
            if (response != null) {
                response.close();
            }

        }

    }

    public void run() {
        CloseableHttpClient httpClient = null;
        try {
            httpClient = getClient();
        } catch (KeyStoreException e) {
            e.printStackTrace();
            return;

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return;

        } catch (KeyManagementException e) {
            e.printStackTrace();
            return;
        }

        while (true) {
            try {
                String user = users.poll();
                if (user == null) {
                    if (PIOExport.readUSersFinished) {
                        break;
                    } else {
                        Thread.sleep(1);
                    }
                } else {
                    String[] us = new String[]{user};
                    String d = download1(httpClient, serviceUrl, us, numRecommendations);
                    List<String[]> lines = getLines(d, us);
                    for (int i = 0; i < lines.size(); i++) {
                        scores.offer(lines.get(i));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                return;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                return;
            } catch (KeyStoreException e) {
                e.printStackTrace();
                return;
            } catch (KeyManagementException e) {
                e.printStackTrace();
                return;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
        try {
            httpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
