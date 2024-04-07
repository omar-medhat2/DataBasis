package database_project1;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class createcsv {
	
	
	public static void addtoCSV(String tableName, Hashtable<String, String> columnTypes, String clusteringKey) {
        String fileName = "table_metadata.csv";
        String directory = System.getProperty("user.dir");
        String path = directory + File.separator + fileName;

        try (FileWriter write = new FileWriter(fileName, true)) {
            // Check if the file exists and if not, write the header
            if (!new File(fileName).exists() || new File(fileName).length() == 0) {
                write.append("TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType\n");
            }

            // Write meta data for each table
            for (Map.Entry<String, String> entry : columnTypes.entrySet()) {
                String columnName = entry.getKey();
                String columnType = entry.getValue();

                write.append(tableName)
                        .append(",")
                        .append(columnName)
                        .append(",")
                        .append(columnType)
                        .append(",")
                        .append(Boolean.toString(columnName.equals(clusteringKey)))
                        .append(",")
                        .append("Null") // IndexName
                        .append(",")
                        .append("Null") // IndexType
                        .append("\n");
            }

            System.out.println("CSV file updated");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
