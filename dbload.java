import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.*;

public class dbload {

    /**
     * Method called when the list of arguments is invalid
     * Informs the User what the valid arguments are and terminates the program
     */
    private static void usage() {
        System.err.println("Usage: java dbload [-p <pagesize>] <datafile>");
        System.err.println("       -p <pagesize> = Optional: Set the page size. Defaults to 4096.");
        System.err.println("       <sourcefile>  = Required: File to be loaded and stored as a heap file.");
        System.exit(1);
    }

    /**
     * Main function for index
     * @param args[] filename and option inputs
     */
    public static void main(String[] args) {

        int pagesize = 4096;
        String datafile = null;
        int pagesizeIndex = -1;
        final String DELIMITER = "\t";
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

        System.out.println("pagesize: " + pagesize);
        System.out.println("datafile: " + datafile);

        String line = "";
        int row = -1;
        final int MAX = 3000000;
        String[] cols;
        String name;
        String status;
        String regDate;
        String cancelDate;
        String renewDate;
        String stateNum;
        String state;
        String abn;
        
        int nameCount = 0;
        int statusCount = 0;
        int regDateCount = 0;
        int cancelDateCount = 0;
        int renewDateCount = 0;
        int stateNumCount = 0;
        int stateCount = 0;
        int abnCount = 0;

        long startTime = System.currentTimeMillis();

        try (BufferedReader br = new BufferedReader(new FileReader(datafile))) {

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

                

                //insertBusinessName(row, name, status, regDate, cancelDate, renewDate, stateNum, state, abn);
                if (!name.isEmpty()) {
                    nameCount++;
                }
                if (!status.isEmpty()) {
                    statusCount++;
                }
                if (!regDate.isEmpty()) {
                    regDateCount++;
                }
                if (!cancelDate.isEmpty()) {
                    cancelDateCount++;
                }
                if (!renewDate.isEmpty()) {
                    renewDateCount++;
                }
                if (!stateNum.isEmpty()) {
                    stateNumCount++;
                }
                if (!state.isEmpty()) {
                    stateCount++;
                }
                if (!abn.isEmpty()) {
                    abnCount++;
                }

                if (row % 10000 == 0) {
                    System.out.print("\r" + row + " rows read ...");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.print("\r" + row + " rows read      ");

        System.out.println("name      : " + nameCount);
        System.out.println("status    : " + statusCount);
        System.out.println("regDate   : " + regDateCount);
        System.out.println("cancelDate: " + cancelDateCount);
        System.out.println("renewDate : " + renewDateCount);
        System.out.println("stateNum  : " + stateNumCount);
        System.out.println("state     : " + stateCount);
        System.out.println("abn       : " + abnCount);
        System.out.println("---------------------------------------------");
        System.out.println("name      : " + (nameCount / row * 100) + "%");
        System.out.println("status    : " + (statusCount / row * 100) + "%");
        System.out.println("regDate   : " + (regDateCount / row * 100) + "%");
        System.out.println("cancelDate: " + (cancelDateCount / row * 100) + "%");
        System.out.println("renewDate : " + (renewDateCount / row * 100) + "%");
        System.out.println("stateNum  : " + (stateNumCount / row * 100) + "%");
        System.out.println("state     : " + (stateCount / row * 100) + "%");
        System.out.println("abn       : " + (abnCount / row * 100) + "%");
        
        long endTime = System.currentTimeMillis();
        long ms = endTime - startTime;
        System.out.println("Completed in: " + (ms/60000) + "m " + ((ms%60000)/1000) + "s " + (ms%1000) + "ms");
    }

}