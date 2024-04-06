package database_project1;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class createcsv {
	
	
	 public static void generateCSV(List<TableMetaData> metaDataList) {
	        String FileName = "table_metadata.csv";
	        
	        
	        String Directory = System.getProperty("user.dir");
	        String Path = Directory + File.separator + FileName;
	        
	        
	        try (FileWriter write = new FileWriter(FileName)) {
	            // Write header
	            write.append("TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType\n");

	            // Write meta data for each table
	            for (TableMetaData metaData : metaDataList) {
	                write.append(metaData.getTableName())
	                        .append(",")
	                        .append(metaData.getColumnName())
	                        .append(",")
	                        .append(metaData.getColumnType())
	                        .append(",")
	                        .append(Boolean.toString(metaData.isClusteringKey()))
	                        .append(",")
	                        .append(metaData.getIndexName() != null ? metaData.getIndexName() : "Null")
	                        .append(",")
	                        .append(metaData.getIndexType() != null ? metaData.getIndexType() : "Null")
	                        .append("\n");
	            }

	            System.out.println("CSV file created");
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
}
