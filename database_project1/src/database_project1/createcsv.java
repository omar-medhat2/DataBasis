package database_project1;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class createcsv {
	
	
	 public static void generateCSV(List<TableMetaData> metaDataList) {
	        String csvFileName = "table_metadata.csv";
	        
	        
	        String currentDirectory = System.getProperty("user.dir");
	        String filePath = currentDirectory + File.separator + csvFileName;
	        
	        
	        try (FileWriter writer = new FileWriter(csvFileName)) {
	            // Write header
	            writer.append("TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType\n");

	            // Write meta data for each table
	            for (TableMetaData metaData : metaDataList) {
	                writer.append(metaData.getTableName())
	                        .append(",")
	                        .append(metaData.getColumnName())
	                        .append(",")
	                        .append(metaData.getColumnType())
	                        .append(",")
	                        .append(Boolean.toString(metaData.isClusteringKey()))
	                        .append(",")
	                        .append(metaData.getIndexName() != null ? metaData.getIndexName() : "")
	                        .append(",")
	                        .append(metaData.getIndexType() != null ? metaData.getIndexType() : "")
	                        .append("\n");
	            }

	            System.out.println("CSV file has been created successfully.");
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
}
