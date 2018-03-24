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

public class CSVParser {

    private static final String FILENAME = "BUSINESS_NAMES_201803.csv";
    private static final String DELIMITER = "\t";
    private static final String BUSINESS_NAME_TABLE = "BUSINESS_NAME";
    private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String DB_URL = "jdbc:derby:BusinessNameDb";
    
    private static Connection connection = null;
    private static Statement statement = null;
    
    private static void connect() {
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(DB_URL); 
            System.out.println("Connected to " + DB_URL);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void disconnect() {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                DriverManager.getConnection(DB_URL + ";shutdown=true");
                connection.close();
            }           
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
        }
    }

    private static void deleteAllBusinessNames() {
        try {
            statement = connection.createStatement();
            statement.execute("DELETE FROM " + BUSINESS_NAME_TABLE);
            statement.close();
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }
    }

    public static String getValue(String value) {
        if (value == null || value.isEmpty()) {
            return "NULL";
        }
        return "'" + value + "'"; // wrap in single-quotes
    }

    private static String reformatDate(String date) {
        if (date == null || date.isEmpty() || date.length() != 10) {
            return "";
        }
        return date.substring(6) + "-" + date.substring(3,5) + "-" + date.substring(0,2);
    }

    public static String getFirstChar(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }
        if (str == "NT") {
            return "O";
        }
        return str.substring(0,1);
    }

    public static String escapeSingleQuotes(String str) {
        if (str == null || str.isEmpty() || !str.contains("'")) {
            return str;
        }
        return Pattern.compile("\\'").matcher(str).replaceAll("''");
    }

    public static void insertBusinessName(int id, String name, String status, String regDate, String cancelDate, String renewDate, String stateNum, String state, String abn) {
        try {
            StringBuilder sql = new StringBuilder(); 
            sql.append("insert into ");
            sql.append(BUSINESS_NAME_TABLE);
            sql.append(" values (");

            sql.append(id);
            sql.append(",");
            sql.append(getValue(escapeSingleQuotes(name)));
            sql.append(",");
            sql.append(getValue(getFirstChar(status)));
            sql.append(",");
            sql.append(getValue(reformatDate(regDate)));
            sql.append(",");
            sql.append(getValue(reformatDate(cancelDate)));
            sql.append(",");
            sql.append(getValue(reformatDate(renewDate)));
            sql.append(",");
            sql.append(getValue(stateNum));
            sql.append(",");
            sql.append(getValue(getFirstChar(state)));
            sql.append(",");
            sql.append(getValue(abn));

            sql.append(")");

            System.out.println(sql.toString());

            statement = connection.createStatement();
            statement.execute(sql.toString());
            statement.close();
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        String line = "";
        int row = -1;
        final int MAX = 10;
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

        try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

            while (row < MAX && (line = br.readLine()) != null) {
                row++;
                if (row == 0) {
                    continue; // skip the header row
                }
                
                cols = line.split(DELIMITER);

                name       = (cols.length > 1) ? cols[1] : null;
                status     = (cols.length > 2) ? cols[2] : null;
                regDate    = (cols.length > 3) ? cols[3] : null;
                cancelDate = (cols.length > 4) ? cols[4] : null;
                renewDate  = (cols.length > 5) ? cols[5] : null;
                stateNum   = (cols.length > 6) ? cols[6] : null;
                state      = (cols.length > 7) ? cols[7] : null;
                abn        = (cols.length > 8) ? cols[8] : null;

                insertBusinessName(row, name, status, regDate, cancelDate, renewDate, stateNum, state, abn);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        disconnect();
    }

}