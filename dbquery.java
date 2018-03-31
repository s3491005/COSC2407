import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class dbquery {

    private static void getPage(byte[] page, int pagesize) {
        byte[] bRecordLength = new byte[4];
        int recordLength = 0;
        int index = 0;

        byte[] bRecordId;
        byte[] bStatus;
        byte[] bRegDate;
        byte[] bRenewDate;
        byte[] bVariableLengthFields;

        final String FIELD_DELIMITER = "#";
        String variableLengthFields;
        String[] variableLengthFieldsArr;

        int recordId;
        String name;
        String status;
        String regDate;
        String cancelDate;
        String renewDate;
        String stateNum;
        String state;
        String abn;

        while (index < pagesize) {
            if ((pagesize - index) < 4) {
                return;
            }
            System.arraycopy(page, index, bRecordLength, 0, 4);
            index += 4; 
            
            recordLength = ByteBuffer.wrap(bRecordLength).getInt();
            System.out.println("recordLength: " + recordLength);
            if (recordLength == -1) {
                return;
            }
            bRecordId = new byte[4];
            bStatus = new byte[1];
            bRegDate = new byte[8];
            bRenewDate = new byte[8];
            bVariableLengthFields = new byte[recordLength-21];

            System.arraycopy(page, index, bRecordId, 0, 4);
            index += 4; 
            System.arraycopy(page, index, bStatus, 0, 1);
            index += 1; 
            System.arraycopy(page, index, bRegDate, 0, 8);
            index += 8; 
            System.arraycopy(page, index, bRenewDate, 0, 8);
            index += 8; 
            System.arraycopy(page, index, bVariableLengthFields, 0, bVariableLengthFields.length);
            index += bVariableLengthFields.length; 
            
            recordId = ByteBuffer.wrap(bRecordId).getInt();
            status = new String(bStatus);
            regDate = new String(bRegDate);
            renewDate = new String(bRenewDate);
            variableLengthFields = new String(bVariableLengthFields);

            variableLengthFieldsArr = variableLengthFields.split(FIELD_DELIMITER);

            name       = variableLengthFieldsArr[0];
            cancelDate = variableLengthFieldsArr[1];
            stateNum   = variableLengthFieldsArr[2];
            state      = variableLengthFieldsArr[3];
            abn        = variableLengthFieldsArr[4];
            
            System.out.println("======================================================");
            System.out.println("recordId: " + recordId);
            System.out.println("name: " + name);
            System.out.println("status: " + status);
            System.out.println("regDate: " + regDate);
            System.out.println("cancelDate: " + cancelDate);
            System.out.println("renewDate: " + renewDate);
            System.out.println("stateNum: " + stateNum);
            System.out.println("state: " + state);
            System.out.println("abn: " + abn);
        }

    }

    public static void main(String[] args) {

        int pagesize = 2048;
        byte[] page = null;
        Boolean hasNextPage = true;
        int count = 0;

        try (FileInputStream stream = new FileInputStream("heap.2048")) {

            while (hasNextPage && count < 100) {
                page = new byte[pagesize];
                stream.read(page);
                getPage(page, pagesize);
                count++;

                // 
            }

            stream.close();

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        System.out.println("count: " + count);

    }
}