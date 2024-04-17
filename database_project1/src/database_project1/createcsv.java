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
	public static String getIndexType(String strTableName, String strColName) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("table_metadata.csv"));
        for (String line : lines) {
            String[] fields = line.split(",");
            if (fields.length >= 3 && fields[0].equals(strTableName) && fields[1].equals(strColName) &&(!(fields[5].equals("Null"))||(!(fields[5].equals("NULL"))) || (!(fields[5].equals("null"))))) {
                return fields[5];
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
