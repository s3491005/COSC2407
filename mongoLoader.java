import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.MongoCredential;
import com.mongodb.MongoClientOptions;

public class mongoLoader {

    private static final String FILENAME = "BUSINESS_NAMES_201803.csv";
    private static final String DATABASE_NAME = "businessNames";
    private static final String BN_COLLECTION = "businessName";
    private static final String DELIMITER = "\t";

    private static MongoClient mongoClient = null;
    private static MongoDatabase db = null;
    private static MongoCollection<Document> collection = null;

    private static Pattern pattern = Pattern.compile("\\\"");

    private static void connect() {
        try {
            mongoClient = new MongoClient();
            db = mongoClient.getDatabase(DATABASE_NAME);
            collection = db.getCollection(BN_COLLECTION);
            System.out.println("Connected to mongodb");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void deleteAllBusinessNames() {
        try {
            System.out.println("Deleting all documents from " + BN_COLLECTION + "...");
            
            System.out.println("All documents deleted from " + BN_COLLECTION);
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }
    }

    private static String reformatDate(String date) {
        return date.substring(6) + "-" + date.substring(3,5) + "-" + date.substring(0,2);
    }

    public static void insertBusinessName(int id, String name, String status, String regDate, String cancelDate, String renewDate, String stateNum, String state, String abn) {

        Document doc = new Document("_id", id);
        
        name = pattern.matcher(name).replaceAll("\\\""); // NAME
        doc.append("name", name);

        doc.append("status", status.charAt(0)); // STATUS, never null

        if (!regDate.isEmpty()) {
            doc.append("regDate", reformatDate(regDate)); // REG_DT
        }

        if (!cancelDate.isEmpty()) {
            doc.append("cancelDate", reformatDate(cancelDate)); // CANCEL_DT
        }

        if (!renewDate.isEmpty()) {
            doc.append("renewDate", reformatDate(renewDate)); // CANCEL_DT
        }

        if (!stateNum.isEmpty()) {
            doc.append("stateNum", stateNum); // STATE_NUM
        }

        if (!state.isEmpty()) {
            if (state == "NT") {
                doc.append("state", "O"); // STATE_OF_REG
            } else {
                doc.append("state", state.charAt(0));
            }
        }

        if (!abn.isEmpty()) {
            doc.append("abn", abn); // ABN
        }

        collection.insertOne(doc);
    }

    public static void main(String[] args) {

        String line = "";
        int row = -1;
        final int MAX = 100;
        String[] cols;
        String name;
        String status;
        String regDate;
        String cancelDate;
        String renewDate;
        String stateNum;
        String state;
        String abn;
        
        connect();
        deleteAllBusinessNames();

        long startTime = System.currentTimeMillis();

        try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

            while (row < MAX && (line = br.readLine()) != null) {
                row++;
                if (row == 0) {
                    continue; // skip the header row
                }
                
                cols = line.split(DELIMITER);
                
                name       = cols[1];
                status     = cols[2];
                regDate    = cols[3];
                cancelDate = cols[4];
                renewDate  = cols[5];
                stateNum   = (cols.length > 6) ? cols[6] : "";
                state      = (cols.length > 7) ? cols[7] : "";
                abn        = (cols.length > 8) ? cols[8] : "";

                insertBusinessName(row, name, status, regDate, cancelDate, renewDate, stateNum, state, abn);

                if (row % 1000 == 0) {
                    System.out.print("\r" + row + " documents inserted ...");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("\nInserted " + row + " documents      ");

        long endTime = System.currentTimeMillis();
        long ms = endTime - startTime;
        System.out.println("Completed in: " + (ms/60000) + "m " + ((ms%60000)/1000) + "s " + (ms%1000) + "ms");
    }

}