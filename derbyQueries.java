import java.sql.Connection;
import java.sql.DriverManager;
//import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import java.sql.ResultSetMetaData;

/**
 * derbyLoader Class Object
 * @author  Geoffrey Sage, s3491005
 * @version 1.0
 * @since   2018-04-01
 */
public class derbyQueries {

    // Class Properties
    private static final String BUSINESS_NAME_TABLE = "BUSINESS_NAME";
    private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String DB_URL = "jdbc:derby:BusinessNameDb";
    private static Connection connection = null;
    private static Statement statement = null;
    private static int count = 0;

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


    private static void executeQuery(String query) {
        long startTime = 0;
        long endTime = 0;
        long diff = 0;
        count++;

        try {
            statement = connection.createStatement();
            startTime = System.currentTimeMillis();
            statement.execute(query);
            endTime = System.currentTimeMillis();
            diff = endTime - startTime;
            System.out.println("query " + count + ": " + diff + "ms");
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

        connect();
        
        String query1 = "SELECT NAME, REG_DT, ABN, ID FROM BUSINESS_NAME WHERE NAME LIKE '%JOY%' AND STATUS = 'R' ORDER BY REG_DT DESC";
        String query2 = "SELECT * FROM BUSINESS_NAME WHERE STATUS = 'D' AND REG_DT >= '2019-01-01' ORDER BY CANCEL_DT DESC";
        String query3 = "SELECT NAME, REG_DT, ABN, ID FROM BUSINESS_NAME WHERE NAME LIKE '%JOY%' AND STATUS = 'R' ORDER BY REG_DT DESC";
        String query4 = "SELECT NAME, REG_DT, ABN, ID FROM BUSINESS_NAME WHERE NAME LIKE '%JOY%' AND STATUS = 'R' ORDER BY REG_DT DESC";
        
        executeQuery(query1);
        executeQuery(query2);
        executeQuery(query3);
        executeQuery(query4);
        
        disconnect(); // Close the database connection
        
    }
}