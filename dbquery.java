import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.*;

import java.lang.StringIndexOutOfBoundsException;

/**
 * dbquery Class Object
 * Search a heap file for a given query term
 * 
 * @author  Geoffrey Sage, s3491005
 * @version 1.0
 * @since   2018-04-01
 */
public class dbquery {

    // Class Properties
    private static final String FIELD_DELIMITER = "#";
    private static int pagesize = 0;
    private static int results = 0;
    private static String filename = "";
    private static String text = null;
    private static Pattern regex = null;
    
    private static final int NUM_BYTES_FOR_INT = 4; // Number of bytes required for an integer
    private static final int NUM_BYTES_FOR_CHAR = 1; // Number of bytes required for a char
    private static final int NUM_BYTES_FOR_DATE = 8; // Number of bytes required for an String containing a Date in DDMMYYYY format
    // Set length of the 4 fixed-length fields for every record: _ID, STATUS, REG_DT, RENEW_DT
    private static final int LENGTH_OF_FIXED_FIELDS = NUM_BYTES_FOR_INT + NUM_BYTES_FOR_CHAR + (NUM_BYTES_FOR_DATE * 2);

    private static final int MIN_PAGESIZE = 256;      // 2^8  - Smallest page size able to fit largest record (239 bytes)
    private static final int MAX_PAGESIZE = 67108864; // 2^26 - Any bigger than this and we get "OutOfMemoryError: Java heap space"

    /**
     * Method called when the list of arguments is invalid
     * Informs the User what the valid arguments are and terminates the program
     */
    private static void usage() {
        System.err.println("Usage: java dbquery <text> <pagesize>");
        System.err.println("       <text>     = Required: Search query to be used when searching the heap file.");
        System.err.println("       <pagesize> = Required: Specify the page size of the heap file to be searched.");
        System.err.println("                    Must be between " + MIN_PAGESIZE + " and " + MAX_PAGESIZE + " inclusive.");
        System.exit(1);
    }

    /**
     * Search all records in a page for the text
     * @param page The page to be searched
     */
    private static void searchPage(byte[] page) {

        byte[] bRecordLength = new byte[NUM_BYTES_FOR_INT];
        int recordLength = 0;
        int index = 0;

        byte[] bRecordId;
        byte[] bStatus;
        byte[] bRegDate;
        byte[] bRenewDate;
        byte[] bVLFields; // Variable-length fields

        String vLFields;
        String[] vLFieldsArr;
        int lengthOfVariableFields = 0;

        int recordId;
        String name;
        String status;
        String regDate;
        String cancelDate;
        String renewDate;
        String stateNum;
        String state;
        String abn;

        // Iterate through each byte in the page
        while (index < pagesize) {
            if ((pagesize - index) < NUM_BYTES_FOR_INT) {
                return; // Not enough space for another record
            }

            // Extract the Record length of the next Record
            System.arraycopy(page, index, bRecordLength, 0, NUM_BYTES_FOR_INT); // Copy the next 4 bytes for the integer
            index += NUM_BYTES_FOR_INT; 
            recordLength = ByteBuffer.wrap(bRecordLength).getInt(); // Convert the byte[] to an int

            if (recordLength < 1) {
                return; // No more records on this page
            }
            
            // Extract the NAME field from the record so we can check it for the text
            // Ignore the other field unless we need them
            lengthOfVariableFields = recordLength - LENGTH_OF_FIXED_FIELDS;
            bVLFields = new byte[lengthOfVariableFields];
            System.arraycopy(page, index + LENGTH_OF_FIXED_FIELDS, bVLFields, 0, lengthOfVariableFields);

            vLFields    = new String(bVLFields); // Convert byte[] to Sstring
            vLFieldsArr = vLFields.split(FIELD_DELIMITER); // Separate the variable-length fields by the delimiter

            name = vLFieldsArr[0];
            
            if (regex.matcher(name).matches()) {

                // Found a match - retrieve the other fields
                cancelDate = (vLFieldsArr.length > 1) ? vLFieldsArr[1] : "";
                stateNum   = (vLFieldsArr.length > 2) ? vLFieldsArr[2] : "";
                state      = (vLFieldsArr.length > 3) ? vLFieldsArr[3] : "";
                abn        = (vLFieldsArr.length > 4) ? vLFieldsArr[4] : "";

                // Get the bytes for the fixed-length fields
                bRecordId = new byte[NUM_BYTES_FOR_INT];
                bStatus = new byte[NUM_BYTES_FOR_CHAR];
                bRegDate = new byte[NUM_BYTES_FOR_DATE];
                bRenewDate = new byte[NUM_BYTES_FOR_DATE];
                
                System.arraycopy(page, index, bRecordId, 0, NUM_BYTES_FOR_INT);
                index += NUM_BYTES_FOR_INT; 
                System.arraycopy(page, index, bStatus, 0, NUM_BYTES_FOR_CHAR);
                index += NUM_BYTES_FOR_CHAR; 
                System.arraycopy(page, index, bRegDate, 0, NUM_BYTES_FOR_DATE);
                index += NUM_BYTES_FOR_DATE; 
                System.arraycopy(page, index, bRenewDate, 0, NUM_BYTES_FOR_DATE);
                index += NUM_BYTES_FOR_DATE; 
                index += lengthOfVariableFields; 

                // Convert the byte arrays to ints or Strings
                recordId = ByteBuffer.wrap(bRecordId).getInt();
                status = new String(bStatus);
                regDate = new String(bRegDate);
                renewDate = new String(bRenewDate);

                // Restore the repeated data that was truncated in dbload
                status = status.equals("R") ? "Registered" : "Deregistered";
                regDate = regDate.substring(0,2) + "/" + regDate.substring(2,4) + "/" + regDate.substring(4);
                renewDate = renewDate.substring(0,2) + "/" + renewDate.substring(2,4) + "/" + renewDate.substring(4);
                if (cancelDate.length() == 8) {
                    cancelDate = cancelDate.substring(0,2) + "/" + cancelDate.substring(2,4) + "/" + cancelDate.substring(4);
                }
                
                // Print the record
                System.out.println(recordId + ", " + name + ", " + status + ", " + regDate + ", " + cancelDate + ", " + renewDate + ", " + stateNum + ", " + state + ", " + abn);
                results++;
            } else {
                // Not a match, skip over this record
                index += recordLength;
            }
        }
    }

    /**
     * Main function for dbquery
     * @param args[] text (query term) and pagesize
     */
    public static void main(String[] args) {

        // Validate the arguments
        if (args.length != 2) {
            usage();
        }

        text = args[0];

        // Make sure the pagesize looks like a number
        if (!Pattern.compile("^\\d+$").matcher(args[1]).matches()) {
            System.err.println("pagesize is not a valid integer");
            usage();
        }

        // Check the pagesize limits
        pagesize = Integer.parseInt(args[1]);
        if (pagesize < MIN_PAGESIZE || pagesize > MAX_PAGESIZE) {
            usage();
        }

        filename = "heap." + pagesize;

        // Check if a heap file for the given pagesize exists
        File file = new File(filename);
        if (!file.exists() || !file.isFile()) {
            System.err.println(filename + " is not a valid file");
            System.exit(1);
        }

        // Regular expression used to seach records for the text
        regex = Pattern.compile(".*" + text + ".*", Pattern.CASE_INSENSITIVE);

        byte[] page = null;
        Boolean notEmpty = true;
        int pageCount = 0;

        long startTime = System.currentTimeMillis();
        long endTime = 0;

        // Read from the heap file
        try (FileInputStream stream = new FileInputStream(filename)) {

            int fileSize = stream.available(); // size of the heap file
            int numPages = fileSize / pagesize; // number of pages in the heap file
            
            // Read in a page at a time and seach it for the text
            while (pageCount < numPages) {
                page = new byte[pagesize];
                stream.read(page);
                searchPage(page);
                pageCount++;
            }

            stream.close();

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        endTime = System.currentTimeMillis();
        
        // Output the results
        System.out.println("results: " + results);
        //System.out.println("pages searched: " + pageCount);
        System.out.println("Completed in " + (endTime - startTime) + " ms");
    }
}