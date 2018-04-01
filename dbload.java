import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.*;

/**
 * dbload Class Object
 * Read a CSV file and create a heap file
 * Page size for the heap file can be specified in an argument when the program is executed
 * @author  Geoffrey Sage, s3491005
 * @version 1.0
 * @since   2018-04-01
 */
public class dbload {

    private static int pagesize = 4096; // Default values
    private static String filename = null;
    
    private static byte[] page = null;
    private static int currentPageSize = 0;
    private static int pageCount = 0;
    
    private static final int NUM_BYTES_FOR_INT = 4; // Number of bytes required for an integer
    private static final byte[] MINUS_ONE = ByteBuffer.allocate(NUM_BYTES_FOR_INT).putInt(-1).array();
    private static final byte HASH = (byte)'#';
    private static final int MIN_PAGESIZE = 256;      // 2^8  - Smallest page size able to fit largest record (239 bytes)
    private static final int MAX_PAGESIZE = 67108864; // 2^26 - Any bigger than this and we get "OutOfMemoryError: Java heap space"

    /**
     * Method called when the list of arguments is invalid
     * Informs the User what the valid arguments are and terminates the program
     */
    private static void usage() {
        System.err.println("Usage: java dbload [-p <pagesize>] <datafile>");
        System.err.println("       -p <pagesize> = Optional: Set the page size. Defaults to 4096.");
        System.err.println("                       Must be between " + MIN_PAGESIZE + " and " + MAX_PAGESIZE + " inclusive.");
        System.err.println("       <sourcefile>  = Required: File to be loaded and stored as a heap file.");
        System.exit(1);
    }

    private static void addRecord(byte[] record) {
        if (page == null) {
            page = new byte[pagesize];
            currentPageSize = 0;
        }
        if ((currentPageSize + record.length) > pagesize) {
            writePage();
        }
        
        System.arraycopy(record, 0, page, currentPageSize, record.length);
        currentPageSize += record.length;
    }

    private static void writePage() {
        //System.out.println("writePage()");
        //System.out.println("currentPageSize: " + currentPageSize);
        if (currentPageSize == 0) {
            return; 
        }

        // If there is enough free space, set the next record length to 0
        if ((currentPageSize + NUM_BYTES_FOR_INT) <= pagesize) {
            System.arraycopy(MINUS_ONE, 0, page, currentPageSize, NUM_BYTES_FOR_INT);
            currentPageSize += NUM_BYTES_FOR_INT;
        }

        // Fill the rest of the page with hashes
        for (int i = currentPageSize; i < page.length; i++) {
            page[i] = HASH;
        }

        // If this it a new program execution, delete any existing file of the same name
        if (pageCount == 0) {
            File oldFile = new File(filename);
            oldFile.delete();
        }
        
        // Append the page to the file
        try (FileOutputStream stream = new FileOutputStream(filename, true)) {
            stream.write(page);
            stream.close();
		} catch (IOException e) {
            System.err.println(e.getMessage());
        }

        pageCount++;
        page = new byte[pagesize];
        currentPageSize = 0;
    }

    /**
     * Main function for index
     * @param args[] filename and option inputs
     */
    public static void main(String[] args) {

        String datafile = null;
        int pagesizeIndex = -1;
        final String CSV_DELIMITER = "\t";
        Pattern numRegex = Pattern.compile("^\\d+$");
        String arg = null;

        // Read in the arguments
        for (int i = 0; i < args.length; i++) {
            arg = args[i].toLowerCase();
            if (i == pagesizeIndex) {
                // last argument was "-p" - should be the pagesize
                if (arg.charAt(0) == '-' || !numRegex.matcher(arg).matches() || i == args.length - 1) {
                    usage(); // not a number, or last argument (datafile) was encountered
                }
                pagesize = Integer.parseInt(args[i]);
            } else if (arg.equals("-p")) {
                // Print terms as they are encountered
                pagesizeIndex = i + 1;
            } else if (i == args.length - 1) {
                // Should be the datafile
                datafile = args[i];
            }
        }

        if (datafile == null) {
            usage();
        }

        if (pagesize < MIN_PAGESIZE || pagesize > MAX_PAGESIZE) {
            usage();
        }

        filename = "heap." + pagesize;

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
        
        final String FIELD_DELIMITER = "#";
        String variableLengthFields;
        byte[] bRecordId;
        byte[] bStatus;
        byte[] bRegDate;
        byte[] bRenewDate;
        byte[] bVariableLengthFields;
        byte[] bRecordLength;
        byte[] record;
        int currentPageSize = 0;
        int recordLength = 0;
        int recordCount = 0;
        int i = 0;

        Pattern slash = Pattern.compile("\\/");

        long startTime = System.currentTimeMillis();
        long endTime = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(datafile))) {

            while (row < MAX && (line = br.readLine()) != null) {
                row++;
                if (row == 0) {
                    continue; // skip the header row
                }
                
                cols = line.split(CSV_DELIMITER);
                
                name       = cols[1];
                status     = cols[2];
                regDate    = cols[3];
                cancelDate = cols[4];
                renewDate  = cols[5];
                stateNum   = (cols.length > 6) ? cols[6] : "";
                state      = (cols.length > 7) ? cols[7] : "";
                abn        = (cols.length > 8) ? cols[8] : "";

                // Fixed-length fields
                status     = "" + status.charAt(0);
                regDate    = slash.matcher(regDate).replaceAll("");
                cancelDate = slash.matcher(cancelDate).replaceAll("");
                renewDate  = slash.matcher(renewDate).replaceAll("");

                // Variable length or optional fields
                variableLengthFields = name + FIELD_DELIMITER + cancelDate + FIELD_DELIMITER + stateNum + FIELD_DELIMITER + state + FIELD_DELIMITER + abn;
                
                // Create Byte arrays
                bRecordId  = ByteBuffer.allocate(4).putInt(row).array();
                bStatus    = status.getBytes();
                bRegDate   = regDate.getBytes();
                bRenewDate = renewDate.getBytes();
                bVariableLengthFields = variableLengthFields.getBytes();

                recordLength = bRecordId.length + bStatus.length + bRegDate.length + bRenewDate.length + bVariableLengthFields.length;
                bRecordLength = ByteBuffer.allocate(4).putInt(recordLength).array();

                record = new byte[bRecordLength.length + recordLength];

                // Copy all the bytes arrays to one byte array for the record
                System.arraycopy(bRecordLength, 0, record, 0, bRecordLength.length);
                i = bRecordLength.length;
                System.arraycopy(bRecordId, 0, record, i, bRecordId.length);
                i += bRecordId.length;
                System.arraycopy(bStatus, 0, record, i, bStatus.length);
                i += bStatus.length;
                System.arraycopy(bRegDate, 0, record, i, bRegDate.length);
                i += bRegDate.length;
                System.arraycopy(bRenewDate, 0, record, i, bRenewDate.length);
                i += bRenewDate.length;
                System.arraycopy(bVariableLengthFields, 0, record, i, bVariableLengthFields.length);

                addRecord(record);

                if (row % 10000 == 0) {
                    System.out.print("\r" + row + " records loaded ...");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        writePage();

        System.out.println("\r" + row + " records loaded      ");
        System.out.println(pageCount + " pages created");
       
        endTime = System.currentTimeMillis();
        System.out.println("Completed in " + (endTime - startTime) + " ms");
    }

}