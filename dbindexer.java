import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.*;

import java.lang.StringIndexOutOfBoundsException;

/**
 * dbindexer Class Object
 * Search a heap file for a given query term
 * 
 * @author  Geoffrey Sage, s3491005
 * @version 1.0
 * @since   2018-05-01
 */
public class dbindexer {

    // Class Properties
    private static final String FIELD_DELIMITER = "#";
    private static int pagesize = 0;
    private static String filename = "";

    private static Pattern hyphens = Pattern.compile("\\-");
    private static Pattern specialChars = Pattern.compile("[^a-z0-9\\s]");
    private static Pattern extraSpaces = Pattern.compile("\\s\\s+");
    
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
        System.err.println("Usage: java indexer <pagesize>");
        System.err.println("       <pagesize> = Required: Specify the page size of the heap file to be searched.");
        System.err.println("                    Must be between " + MIN_PAGESIZE + " and " + MAX_PAGESIZE + " inclusive.");
        System.exit(1);
    }

    /**
     * Search all records in a page for the text
     * @param page The page to be searched
     */
    private static void indexPage(byte[] page, int pageNumber) {

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
        String[] terms;
        String term;
        int i;

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

            vLFields    = new String(bVLFields); // Convert byte[] to String
            vLFieldsArr = vLFields.split(FIELD_DELIMITER); // Separate the variable-length fields by the delimiter

            name = vLFieldsArr[0];
            
            // sanitise the name
            name = name.trim().toLowerCase(); // trim and convert to lower case
            name = hyphens.matcher(name).replaceAll(" "); // Separate hyphenated words
            name = specialChars.matcher(name).replaceAll(""); // Remove all remaining special characters
            name = extraSpaces.matcher(name).replaceAll(" "); // Remove sequential spaces

            terms = name.split(" ");

            for (i = 0; i < terms.length; i++) {
                term = terms[i];
                
            }
        }
    }

    /**
     * Main function for dbquery
     * @param args[] text (query term) and pagesize
     */
    public static void main(String[] args) {

        // Validate the arguments
        if (args.length != 1) {
            usage();
        }

        // Make sure the pagesize looks like a number
        if (!Pattern.compile("^\\d+$").matcher(args[1]).matches()) {
            System.err.println("pagesize is not a valid integer");
            usage();
        }

        // Check the pagesize limits
        pagesize = Integer.parseInt(args[0]);
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
            while (pageCount <= numPages) {
                page = new byte[pagesize];
                stream.read(page);
                pageCount++;
                indexPage(page, pageCount);
            }

            stream.close();

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        endTime = System.currentTimeMillis();
        
        //System.out.println("pages searched: " + pageCount);
        System.out.println("Completed in " + (endTime - startTime) + " ms");
    }
}