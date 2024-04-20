package database_project1;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.io.File;

public class DBApp {
	
public DBApp( ){
		
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		
		
	}

		public void updateTable(String strTableName, 
				String strClusteringKeyValue,
				Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
	
	Table targetTable = Table.loadFromFile(strTableName + ".ser");
	if (targetTable == null) {
	throw new DBAppException("Table not found: " + strTableName);
	}
	
	List<Page> targetPage = null;
	try {
	targetPage = targetTable.retrievePages();
	} catch (ClassNotFoundException e) {
	e.printStackTrace();
	} catch (IOException e) {
	e.printStackTrace();
	}
	if (targetPage == null) {
	throw new RuntimeException("Row with clustering key " + strClusteringKeyValue + " not found in table " + strTableName);
	}
	for (String attributeName : htblColNameValue.keySet()) {
	String columnType = "";
	try {
	columnType = createcsv.getType(strTableName, attributeName);
	} catch (IOException e) {
	e.printStackTrace();
	}
	if (columnType == null ) 
	throw new DBAppException("Column not found in table: " + attributeName);
	Object attributeValue = htblColNameValue.get(attributeName);
	String attributeValueType = attributeValue.getClass().getName();
	if(columnType.equals("java.lang.double")) {
	columnType = "java.lang.Double";
	}
	if (!attributeValueType.equals(columnType)) 
	throw new DBAppException("Type mismatch for column: " + attributeName);
	}
	String clusteringKeyColumn = targetTable.getStrClusteringKeyColumn();
	Tuple targetTuple = null;
	int j = 0;
	int pageLocation = 0;
	String clusterKeyType = "";
	try {
	clusterKeyType = targetTable.getTypeOfClusterkingKey(strClusteringKeyValue);
	} catch (ClassNotFoundException | IOException e1) {
	e1.printStackTrace();
	}
	
	if(clusterKeyType.equals("string")) {
	for (Page page : targetPage) {
	int low = 0;
	int high = page.getTuples().size() - 1;
	while (low <= high) {
	    int mid = (low + high) / 2;
	    Object primaryKeyValue = page.getTuples().get(mid).getValue(clusteringKeyColumn);
	    int compareResult = strClusteringKeyValue.compareTo(primaryKeyValue.toString());
	    if (compareResult == 0) {
	    	pageLocation = j;
	        targetTuple = page.getTuples().get(mid);
	        break;
	    } else if (compareResult < 0) {
	        high = mid - 1;
	    } else {
	        low = mid + 1;
	    }
	}
	j++;
	}
	}
	else {
	for (Page page : targetPage) {
	int low = 0;
	int high = page.getTuples().size() - 1;
	while (low <= high) {
	    int mid = (low + high) / 2;
	    Object primaryKeyValue = page.getTuples().get(mid).getValue(clusteringKeyColumn);
	    String primaryKeyString = primaryKeyValue.toString();
	    if (strClusteringKeyValue.equals(primaryKeyString)) {
	        targetTuple = page.getTuples().get(mid);
	        pageLocation = j;
	        break;
	    } else if (Integer.parseInt(primaryKeyString) > Integer.parseInt(strClusteringKeyValue)){
	        high = mid - 1;
	    } else {
	        low = mid + 1;
	    }
	}
	j++;
	}
	}
	if (targetTuple == null) {
	throw new RuntimeException("Row with clustering key " + strClusteringKeyValue + " not found in table " + strTableName);
	}
	for (String columnName : htblColNameValue.keySet()) {
	if (!columnName.equals(targetTable.getStrClusteringKeyColumn())) {
		Object newValue;
	    String indexName = "";
	    newValue = htblColNameValue.get(columnName);
			try {
				indexName = targetTable.getIndexName();
			} catch (IOException e) {
				e.printStackTrace();
			}
	        if (indexName != null) {
	        	Object oldValue = targetTuple.getValue(indexName.substring(0, indexName.length()-5));
	        	Object oldValueName = indexName.substring(0, indexName.length()-5);
	        	if(oldValueName.equals(columnName)) {
	                BPlusTree bPlusTree = BPlusTree.loadFromFile(indexName + ".ser");
	                bPlusTree.remove((Comparable) oldValue,strTableName + pageLocation + ".ser");
	                bPlusTree.insert((Comparable)newValue, strTableName + pageLocation + ".ser");
	                bPlusTree.saveToFile(indexName + ".ser");
	            }
	        }
	    targetTuple.updateTuple(columnName, newValue);
	}
	}
	(targetPage.get(pageLocation)).saveToFile(strTableName + pageLocation +".ser");
	targetTable.saveToFile(strTableName + ".ser");
	}


// following method could be used to delete one or more rows.
// htblColNameValue holds the key and value. This will be used in search 
// to identify which rows/tuples to delete. 	



// htblColNameValue entries are ANDED together

private Object convertToType(String value, String type) {
// Convert string value to the specified type
// You might need to add more type conversions based on your supported types
switch (type.toLowerCase()) {
case "integer":
return Integer.parseInt(value);
case "double":
return Double.parseDouble(value);
case "string":
return value;
// Add more cases as needed
default:
throw new IllegalArgumentException("Unsupported type: " + type);
}
}
	
	public static void main( String[] args ){
		
		//System.out.println(getPageNumber("Page-1.ser"));
		
	try{
//		Hashtable htblColNameType = new Hashtable( );
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.double");
//		//Tuple tupleObj = new Tuple(htblColNameType);
//	    //System.out.println( tupleObj);
//	    
//		
//		Hashtable htblColNameType2 = new Hashtable( );
//		htblColNameType2.put("id2", "java.lang.double");
//		htblColNameType2.put("name2", "java.lang.double");
//		htblColNameType2.put("gpa2", "java.lang.double");
//		
//		createTable("Table1","id",htblColNameType);
//		createTable("Table2","id2",htblColNameType2);

		
			
        // Generate CSV file
        //createcsv.generateCSV(metaDataList);
       
      //  page.addTuple(new Tuple("Ahmed", 20, "Zamalek"));
        //page.addTuple(new Tuple("John", 25, "New York"));
		 
		 


	
		String strTableName = "Student";
		DBApp	dbApp = new DBApp( );
////		
		
			Hashtable htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			//dbApp.createTable( strTableName, "id", htblColNameType );
			
//			//dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
////
			
			//Hashtable htblColNameValue = new Hashtable( );
			//htblColNameValue.put("id", new Integer( 1 ));

			
			
			Hashtable htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343432));

			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );

			//dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343433));
			htblColNameValue.put("name", new String("Ahmed Soroor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );
			
			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343434));
			htblColNameValue.put("name", new String("Ahmed Ghandour" ) );
			htblColNameValue.put("gpa", new Double( 0.75 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );
			/*
			Hashtable htblColNameForDelete = new Hashtable( );
			htblColNameForDelete.put("name", new String("Ahmed Soroor" ) );
			htblColNameForDelete.put("gpa", new Double( 0.95 ) );
			dbApp.deleteFromTable("Student", htblColNameForDelete);
			htblColNameForDelete = new Hashtable( );
			htblColNameForDelete.put("name", new String("Ahmed Ghandour" ) );
			htblColNameForDelete.put("gpa", new Double( 0.75 ) );
			dbApp.deleteFromTable("Student", htblColNameForDelete);
			htblColNameForDelete = new Hashtable( );
			htblColNameForDelete.put("name", new String("Ahmed Noor" ) );
			htblColNameForDelete.put("gpa", new Double( 0.95 ) );
			dbApp.deleteFromTable("Student", htblColNameForDelete);
			*/
			/*
			////testing select
			
			SQLTerm[] arrSQLTerms = new SQLTerm[2];
			arrSQLTerms[0] = new SQLTerm(); // Initialize the first element
			arrSQLTerms[0]._strTableName = "Student";
			arrSQLTerms[0]._strColumnName= "name";
			arrSQLTerms[0]._strOperator = "!=";
			arrSQLTerms[0]._objValue = "Ahmed Noor";

			arrSQLTerms[1] = new SQLTerm(); // Initialize the second element
			arrSQLTerms[1]._strTableName = "Student";
			arrSQLTerms[1]._strColumnName= "gpa";
			arrSQLTerms[1]._strOperator = "=";
			arrSQLTerms[1]._objValue = new Double(0.95);
>>>>>>> AmrBranchOmarTesting

			String[] strarrOperators = new String[1];
			strarrOperators[0] = "AND";
			// select * from Student where name = “John Noor” or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
			System.out.println("Result Set:");
			while (resultSet.hasNext()) {
			    Tuple tuple = (Tuple) resultSet.next();
			    System.out.println(tuple); // Assuming Tuple class overrides the toString() method
			}
			
			
			
			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 1 ));
			htblColNameValue.put("name", new String("Ahmed Noorrr" ) );
			htblColNameValue.put("gpa", new Double( 0.8 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
			dbApp.updateTable(strTableName, "2", htblColNameValue);
			dbApp.updateTable(strTableName, "1", htblColNameValue);
//			htblColNameValue.clear( );
				*/
			
			SQLTerm[] arrSQLTerms = new SQLTerm[3];
			arrSQLTerms[0] = new SQLTerm(); // Initialize the first element
			arrSQLTerms[0]._strTableName = "Student";
			arrSQLTerms[0]._strColumnName= "name";
			arrSQLTerms[0]._strOperator = "!=";
			arrSQLTerms[0]._objValue = "Ahmed Noor";

			arrSQLTerms[1] = new SQLTerm(); // Initialize the second element
			arrSQLTerms[1]._strTableName = "Student";
			arrSQLTerms[1]._strColumnName= "gpa";
			arrSQLTerms[1]._strOperator = "!=";
			arrSQLTerms[1]._objValue = new Double(0.95);
			
			arrSQLTerms[2] = new SQLTerm(); // Initialize the second element
			arrSQLTerms[2]._strTableName = "Student";
			arrSQLTerms[2]._strColumnName= "gpa";
			arrSQLTerms[2]._strOperator = "=";
			arrSQLTerms[2]._objValue = new Double(0.75);

			String[] strarrOperators = new String[2];
			strarrOperators[0] = "XOR";
			strarrOperators[1] = "OR";
			// select * from Student where name = “John Noor” or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
			System.out.println("Result Set:");
			while (resultSet.hasNext()) {
			    Tuple tuple = (Tuple) resultSet.next();
			    System.out.println(tuple); // Assuming Tuple class overrides the toString() method
			}
			
			
			
			///////////////////////////////////////////////////////////
			FileInputStream fileIn = new FileInputStream("Student.ser");

            // Step 2: Deserialize the table object
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Table table = (Table) objectIn.readObject();
            objectIn.close();
			
			for (String page : table.getPages()) {
				Page currPage = Page.loadFromFile(page);
			    Vector<Tuple> tuples = currPage.getTuples();
			    System.out.println("Number of tuples in page: " + tuples.size());
			    
			    for (Tuple tuple : tuples) {
			        // Print out clustering key column value for each tuple
			        Object clusteringKeyValue = tuple.getValue(table.getStrClusteringKeyColumn());
			        System.out.println("Clustering key value: " + clusteringKeyValue);
			        
			        // Access tuple attributes as needed
			        Object attributeValue = tuple.getValue("id");
			        System.out.println("Attribute Value: " + attributeValue);
			    }
			}
////			

		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}

	private static InputStream FileInputStream(String string) {
		// TODO Auto-generated method stub
		return null;
	}
}
