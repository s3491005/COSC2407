import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CSVParser {

    public static void main(String[] args) {

        final String FILENAME = "BUSINESS_NAMES_201803.csv";
        final String DELIMITER = "\t";
        String line = "";
        int row = 1;
        final int MAX = 20;

        try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

            while (row <= MAX && (line = br.readLine()) != null) {

                if (row < 10) {
                    System.out.println(" " + row + ": " + line);
                } else {
                    System.out.println(row + ": " + line);
                }
                
                row++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}