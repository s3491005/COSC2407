import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.*;

/**
 * mongoLoader Class Object
 * Populate a CSV into the businessName collection of the local mongodb
 * 
 * Compile: javac -cp mongodb-driver-3.4.3.jar:mongodb-driver-core-3.4.3.jar:bson-3.4.3.jar mongoLoader.java
 * Execute: java -cp .:mongodb-driver-3.4.3.jar:.:mongodb-driver-core-3.4.3.jar:.:bson-3.4.3.jar mongoLoader
 * 
 * @author  Geoffrey Sage, s3491005
 * @version 4.0
 * @since   2018-04-01
 */
public class mongoLoader {

    // Class properties
    private static final String DATABASE_NAME = "businessNames";
    private static final String BN_COLLECTION = "businessName";
    private static final String DELIMITER = "\t";

    private static MongoClient mongoClient = null;
    private static MongoDatabase db = null;
    private static MongoCollection<Document> collection = null;

    private static Pattern pattern = Pattern.compile("\\\"");

    private static String filename = "BUSINESS_NAMES_201803.csv";
    
    /**
     * Method called when the list of arguments is invalid
     * Informs the User what the valid arguments are and terminates the program
     */
    private static void usage() {
        System.err.println("Usage: java -cp <classpath1[:classpath2]> mongoLoader <sourcefile>");
        System.err.println("       -cp <classpaths> = Required: Class paths for the .jar files needed for the driver, separated with :");
        System.err.println("       <sourcefile>     = Required: File to be loaded and stored in the derby database.");
        System.exit(1);
    }

    /**
     * Connect to the mongo database
     */
    private static void connect() {
        try {
            mongoClient = new MongoClient();
            db = mongoClient.getDatabase(DATABASE_NAME);
            collection = db.getCollection(BN_COLLECTION);
            System.out.println("Connected to mongodb");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Delete all existing documents in the collection prior to repopulating it from the file
     */
    private static void deleteAllBusinessNames() {
    	collection.drop();
    }

    /**
     * Reformat a date
     * @param date A date with the format DD/MM/YYYY
     * @return A date with the format YYYY-MM-DD
     */
    private static String reformatDate(String date) {
        return date.substring(6) + "-" + date.substring(3,5) + "-" + date.substring(0,2);
    }

    /**
     * Insert a new document into the database
     * Builds a Document to add to the collection
     * @param id Record ID (row number)
     * @param name BN_NAME
     * @param status BN_STATUS
     * @param regDate BN_REG_DT
     * @param cancelDate BN_CANCEL_DT
     * @param renewDate BN_RENEW_DT
     * @param stateNum BN_SATE_NUMBER
     * @param state BN_SATE_OF_REG
     * @param abn BN_ABN
     */
    public static void insertBusinessName(int id, String name, String status, String regDate, String cancelDate, String renewDate, String stateNum, String state, String abn) {

        // Create a new Docuement
        Document doc = new Document("_id", id);
        
        // Escape all double-quotes
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

        // Insert the new Document into the Collection
        collection.insertOne(doc);
    }

    /**
     * Main function for derbyLoader
     * @param args[] Path of source file
     */
    public static void main(String[] args) {

        // Validate arguments
        if (args.length != 1) {
            usage();
        }

        filename = args[0];

        // Check if the datafile exists
        File file = new File(filename);
        if (!file.exists() || !file.isFile()) {
            System.err.println(filename + " is not a valid file");
            System.exit(1);
        }

        String line = "";
        int row = -1;
        String[] cols;
        String name;
        String status;
        String regDate;
        String cancelDate;
        String renewDate;
        String stateNum;
        String state;
        String abn;
        
        connect(); // connect to MongoDB
        deleteAllBusinessNames(); // Delete all exsiting records

        long startTime = System.currentTimeMillis();

        // Read from the CSV file
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {

            // Iterate through each line
            while ((line = br.readLine()) != null) {
                row++;
                if (row == 0) {
                    continue; // skip the header row
                }
                
                cols = line.split(DELIMITER); // drops empty columns at the end
                
                name       = cols[1];
                status     = cols[2];
                regDate    = cols[3];
                cancelDate = cols[4];
                renewDate  = cols[5];
                stateNum   = (cols.length > 6) ? cols[6] : "";
                state      = (cols.length > 7) ? cols[7] : "";
                abn        = (cols.length > 8) ? cols[8] : "";

                // Create a Document and insert it into the Collection
                insertBusinessName(row, name, status, regDate, cancelDate, renewDate, stateNum, state, abn);

                if (row % 1000 == 0) {
                    System.out.print("\r" + row + " documents inserted ...");
                }
            }

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        // Output stats
        System.out.println("\nInserted " + row + " documents      ");
        long endTime = System.currentTimeMillis();
        long ms = endTime - startTime;
        System.out.println("Completed in: " + (ms/60000) + "m " + ((ms%60000)/1000) + "s " + (ms%1000) + "ms");
    }

}