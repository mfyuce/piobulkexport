package com.piobulkexport;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by myuce on 2.5.2016.
 */
public class PIOExport {
    public static final String TEXT_FILE_PROPERTIES = "main_local.properties";
    static volatile boolean readUSersFinished = false;
    static volatile boolean scoringFinished = false;
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static Properties properties = null;

    static {
        try {
            loadProps();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException, IOException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, InterruptedException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(getProperty("main.hive.url"), "", "");
        String tableName = getProperty("main.hive.itemtable");
        int itemFieldIndex = Integer.parseInt(getProperty("main.hive.itemfieldindex"));
        int numRunner = Integer.parseInt(getProperty("main.numRunner"));
        int numRecommendations = Integer.parseInt(getProperty("main.numrecommendations"));
        String serviceUrl = getProperty("main.pio.deployedserviceurl");
        Statement st = con.createStatement();
        ResultSet rec = st.executeQuery("SELECT * FROM " + tableName);

        BufferedWriter bw = new BufferedWriter(new FileWriter(getProperty("main.outputfile")));
        ConcurrentLinkedQueue<String> users = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String[]> scores = new ConcurrentLinkedQueue<>();
        readUSersFinished = false;
        scoringFinished = false;
        //read users queue
        Thread readUsers = new Thread() {
            public void run() {
                try {
                    while (rec.next()) {
                        users.offer(rec.getString(itemFieldIndex));
                        while (users.size() > 10000) {
                            Thread.sleep(1);
                        }
                    }
                    readUSersFinished = true;
                    System.out.println("finished reading");
                } catch (InterruptedException v) {
                    System.out.println(v);
                    return;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }
            }
        };

        Thread[] getScores = new Thread[numRunner];

        Thread saveScores = new Thread() {
            public void run() {
                while (true) {
                    if (scores.size() > 1000) {
                        try {
                            write(bw, scores);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return;
                        }
                    } else {
                        if (scoringFinished) {
                            break;
                        }
                    }
                }
                if (scores.size() > 0) {
                    try {
                        write(bw, scores);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                }
            }
        };
        readUsers.start();
        for (int i = 0; i < getScores.length; i++) {
            getScores[i] = new ScoreReader(users, scores, numRecommendations, serviceUrl);
            getScores[i].start();

        }
        saveScores.start();

        readUsers.join();
        for (int i = 0; i < getScores.length; i++) {
            getScores[i].join();
        }
        scoringFinished = true;
        saveScores.join();


        bw.close();
    }

    public static void write(BufferedWriter bw, ConcurrentLinkedQueue<String[]> lst) throws IOException {
        while (true) {
            String[] c = lst.poll();
            if (c != null) {
                for (int j = 0; j < c.length; j++) {
                    bw.write(c[j]);
                    if (j != c.length - 1) {
                        bw.write(';');
                    }
                }
                bw.write('\n');
            } else {
                break;
            }
        }
    }

    private static void loadProps() throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        if (properties == null) {
            properties = new Properties();
            properties.load(new FileInputStream(TEXT_FILE_PROPERTIES));
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
