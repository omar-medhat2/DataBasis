package database_project1;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class createcsv {
	
	
	public static void addtoCSV(String tableName, Hashtable<String, String> columnTypes, String clusteringKey) {
        String fileName = "table_metadata.csv";
//        String directory = System.getProperty("user.dir");
//        String path = directory + File.separator + fileName;

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
	
	public static boolean TableNameExists(String tableName) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("table_metadata.csv"));
        for (String line : lines) {
            String[] fields = line.split(",");
            if (fields.length >= 3 && fields[0].equals(tableName)) {
                return true;
            }
        }
        return false;
    }
	
	
	public static boolean updateIndex(String strTableName, String strColName, String indexName) throws IOException {
        // Read all lines from the CSV file
        List<String> lines = Files.readAllLines(Paths.get("table_metadata.csv"));

        // Iterate through each line in the CSV file
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] fields = line.split(",");

            // Check if the line corresponds to the given table and column
            if (fields.length >= 3 && fields[0].equals(strTableName) && fields[1].equals(strColName)) {
                // Update the IndexName field with the provided index name
                fields[4] = indexName;
                
                // If an index name is provided, set the IndexType to "B+ Tree"
                if (indexName != null && !indexName.isEmpty()) {
                    fields[5] = "B+ Tree";
                } else {
                    // If no index name is provided, set the IndexType to "Null"
                    fields[5] = "Null";
                }

                // Reconstruct the line with updated fields
                lines.set(i, String.join(",", fields));

                // Write the updated lines back to the CSV file
                Files.write(Paths.get("table_metadata.csv"), lines);
                
                // Return true indicating successful update
                return true;
            }
        }

        // Return false indicating the update was not successful
        return false;
    }
	
	
	public static String getIndexName(String strTableName, String strColName) throws IOException {
        // Read all lines from the CSV file
        List<String> lines = Files.readAllLines(Paths.get("table_metadata.csv"));

        // Iterate through each line in the CSV file
        for (String line : lines) {
            String[] fields = line.split(",");

            // Check if the line corresponds to the given table and column
            if (fields.length >= 3 && fields[0].equals(strTableName) && fields[1].equals(strColName)) {
                // Check if an index name is present
                if (fields.length >= 6 && !fields[5].equals("Null")) {
                    // Return the index name
                    return fields[4];
                }
            }
        }
        // No index found for the given table and column
        return null;
    }
	
	
	public static String getType(String strTableName, String strColName) throws IOException {
	        List<String> lines = Files.readAllLines(Paths.get("table_metadata.csv"));
	        for (String line : lines) {
	            String[] fields = line.split(",");
	            if (fields.length >= 3 && fields[0].equals(strTableName) && fields[1].equals(strColName)) {
	                return fields[2];
	            }
	        }
	        return null;
	}
	public static String getCluster(String strTableName) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("table_metadata.csv"));
        for (String line : lines) {
            String[] fields = line.split(",");
            if (fields.length >= 3 && fields[0].equals(strTableName) && (fields[3].equals("TRUE")) || (fields[3].equals("true"))){
                return fields[1];
            }
        }
        return null;
}
}
