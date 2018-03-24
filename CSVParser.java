import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CSVParser {

    public static void main(String[] args) {

        final String FILENAME = "BUSINESS_NAMES_201803.csv";
        final String DELIMITER = "\t";
        String line = "";
        int row = 0;
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

        HashSet<String> statusList = new HashSet<>();
        HashSet<String> stateList   = new HashSet<>();
        int minNameLength = 10000;
        int maxNameLength = 0;
        int nameLength;

        try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

            while ((line = br.readLine()) != null) {
                row++;
                if (row == 1) {
                    continue; // skip the header row
                }
                
                cols = line.split(DELIMITER);
                name       = cols[1];
                status     = (cols.length > 2) ? cols[2] : "";
                //regDate    = (cols.length > 3) ? reformatDate(cols[3]) : "";
                //cancelDate = (cols.length > 4) ? reformatDate(cols[4]) : "";
                //renewDate  = (cols.length > 5) ? reformatDate(cols[5]) : "";
                //stateNum   = (cols.length > 6) ? cols[6] : "";
                state      = (cols.length > 7) ? cols[7] : "";
                //abn        = (cols.length > 8) ? cols[8] : "";


                if (!statusList.contains(status)) {
                    statusList.add(status);
                }
                if (!state.isEmpty() && !stateList.contains(state)) {
                    stateList.add(state);
                }

                nameLength = name.length();
                if (nameLength < minNameLength) {
                    minNameLength = nameLength + 0;
                }
                if (nameLength > maxNameLength) {
                    maxNameLength = nameLength + 0;
                }

                //System.out.println(name);
                //System.out.println(status + "\t" + regDate + "\t" + cancelDate + "\t" + renewDate + "\t" + stateNum + "\t" + regState + "\t" + abn);
            }

            System.out.println(row);            // 2,523,933
            System.out.println(minNameLength);  // 1
            System.out.println(maxNameLength);  // 199
            System.out.println(statusList);     // [Deregistered, Registered]
            System.out.println(stateList);      // [VIC, ACT, NSW, NT, TAS, QLD, WA, SA]

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static String reformatDate(String date) {
        if (date == null || date.isEmpty() || date.length() != 10) {
            return "";
        }
        return date.substring(6) + "-" + date.substring(3,5) + "-" + date.substring(0,2);
    }

}