import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CSVParser {

    public static void main(String[] args) {

        final String FILENAME = "BUSINESS_NAMES_201803.csv";
        final String DELIMITER = "\t";
        String line = "";
        int row = 0;
        final int MAX = 1000;
        String[] cols;
        String name;
        String status;
        String regDate;
        String cancelDate;
        String renewDate;
        String stateNum;
        String regState;
        String abn;

        try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

            while (row <= MAX && (line = br.readLine()) != null) {
                row++;
                if (row == 1) {
                    continue; // skip the header row
                }
                cols = line.split(DELIMITER);
                name       = cols[1];
                status     = cols[2];
                regDate    = reformatDate(cols[3]);
                cancelDate = reformatDate(cols[4]);
                renewDate  = reformatDate(cols[5]);
                stateNum   = (cols.length > 6) ? cols[6] : "";
                regState   = (cols.length > 7) ? cols[7] : "";
                abn        = (cols.length > 8) ? cols[8] : "";

                System.out.println(name);
                System.out.println(status + "\t" + regDate + "\t" + cancelDate + "\t" + renewDate + "\t" + stateNum + "\t" + regState + "\t" + abn);
            }

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