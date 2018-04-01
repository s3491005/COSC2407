import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;

/**
 * derbyLoader Class Object
 * @author  Geoffrey Sage, s3491005
 * @version 1.0
 * @since   2018-04-01
 */
public class derbyLoader {

    // Class Properties
    private static final String DELIMITER = "\t";
    private static final String BUSINESS_NAME_TABLE = "BUSINESS_NAME";
    private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String DB_URL = "jdbc:derby:BusinessNameDb";
    
    private static String filename = "";
    private static Connection connection = null;
    private static Statement statement = null;
    private static Pattern pattern = Pattern.compile("\\'");

    /**
     * Method called when the list of arguments is invalid
     * Informs the User what the valid arguments are and terminates the program
     */
    private static void usage() {
        System.err.println("Usage: java derbyLoader <sourcefile>");
        System.err.println("       <sourcefile>  = Required: File to be loaded and stored in the derby database.");
        System.exit(1);
    }

    /**
     * Connect to the derby database
     */
    private static void connect() {
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(DB_URL); 
            System.out.println("Connected to " + DB_URL);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Close the connection to the derby database
     */
    private static void disconnect() {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }           
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
        }
    }

    /**
     * Delete all existing records in the database prior to repopulating it from the file
     */
    private static void deleteAllBusinessNames() {
        try {
            System.out.println("Deleting all rows from " + BUSINESS_NAME_TABLE + "...");
            statement = connection.createStatement();
            statement.execute("DELETE FROM " + BUSINESS_NAME_TABLE);
            statement.close();
            System.out.println("All rows deleted from " + BUSINESS_NAME_TABLE);
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }
    }

    /**
     * Insert a new record into the database
     * Builds a SQL statment to insert the record
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
        // Build a SQL statement for the record insert
        StringBuilder sql = new StringBuilder(); 
        try {
            sql.append("INSERT INTO ");
            sql.append(BUSINESS_NAME_TABLE);
            sql.append(" VALUES (");

            sql.append(id);
            sql.append(",'");
            sql.append(pattern.matcher(name).replaceAll("''")); // NAME
            sql.append("','");
            sql.append(status.charAt(0)); // STATUS, never null
            sql.append("',");
            if (regDate.isEmpty()) {
                sql.append("NULL");
            } else {
                // Convert the date format from DD/MM/YYYY to YYYY-MM-DD
                sql.append("'");
                sql.append(regDate.substring(6)); // REG_DT
                sql.append("-");
                sql.append(regDate.substring(3,5));
                sql.append("-");
                sql.append(regDate.substring(0,2));
                sql.append("'");
            }
            sql.append(",");
            if (cancelDate.isEmpty()) {
                sql.append("NULL");
            } else {
                // Convert the date format from DD/MM/YYYY to YYYY-MM-DD
                sql.append("'");
                sql.append(cancelDate.substring(6)); // CANCEL_DT
                sql.append("-");
                sql.append(cancelDate.substring(3,5));
                sql.append("-");
                sql.append(cancelDate.substring(0,2));
                sql.append("'");
            }
            sql.append(",");
            if (renewDate.isEmpty()) {
                sql.append("NULL");
            } else {
                // Convert the date format from DD/MM/YYYY to YYYY-MM-DD
                sql.append("'");
                sql.append(renewDate.substring(6)); // RENEW_DT
                sql.append("-");
                sql.append(renewDate.substring(3,5));
                sql.append("-");
                sql.append(renewDate.substring(0,2));
                sql.append("'");
            }
            sql.append(",");
            if (stateNum.isEmpty()) {
                sql.append("NULL");
            } else {
                sql.append("'");
                sql.append(stateNum); // STATE_NUM
                sql.append("'");
            }
            sql.append(",");
            if (state.isEmpty()) {
                sql.append("NULL");
            } else if (state.equals("NT")) {
                sql.append("'O'");
            } else {
                sql.append("'");
                sql.append(state.charAt(0)); // STATE_OF_REG
                sql.append("'");
            }
            sql.append(",");
            if (abn.isEmpty()) {
                sql.append("NULL");
            } else {
                sql.append("'");
                sql.append(abn); // ABN
                sql.append("'");
            }
            sql.append(")");
            // SQL Statement has been constructed

            statement = connection.createStatement();
            statement.execute(sql.toString());
            statement.close();
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
        }
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
        final int MAX = 100000;
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

        // Read from the CSV file
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {

            // Iterate through each row in the file
            while ((line = br.readLine()) != null) {
                row++;
                if (row == 0) {
                    continue; // skip the header row
                }
                
                cols = line.split(DELIMITER); // drops empty columns at the end
                
                name       = cols[1]; // Never null
                status     = cols[2]; // Never null
                regDate    = cols[3]; // Never null
                cancelDate = cols[4]; // Sometimes null
                renewDate  = cols[5]; // Never null
                stateNum   = (cols.length > 6) ? cols[6] : ""; // Sometimes null
                state      = (cols.length > 7) ? cols[7] : ""; // Sometimes null
                abn        = (cols.length > 8) ? cols[8] : ""; // Sometimes null

                // Insert the row into the database
                insertBusinessName(row, name, status, regDate, cancelDate, renewDate, stateNum, state, abn);

                if (row % 1000 == 0) {
                    System.out.print("\r" + row + " rows inserted ...");
                }
            }

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        disconnect(); // Close the database connection

        System.out.println("\nInserted " + row + " rows      ");

        long endTime = System.currentTimeMillis();
        long ms = endTime - startTime;
        System.out.println("Completed in: " + (ms/60000) + "m " + ((ms%60000)/1000) + "s " + (ms%1000) + "ms");
    }

}