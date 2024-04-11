package database_project1;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class DBApp {
	public static  ArrayList<Table> theTables = new ArrayList<>();
public DBApp( ){
		
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		
		
	}

	public static int CurrentPageNumber(String PageName) {
        int pageNumber = 0;
            String[] parts = PageName.split("\\.");
            String[] nameParts = parts[0].split("page");
            pageNumber = Integer.parseInt(nameParts[1]);
       
        return pageNumber;
    }
	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public static void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException{
								
				Table newTable = new Table();
				newTable.strTableName = strTableName;
				newTable.strClusteringKeyColumn = strClusteringKeyColumn;
				createcsv.addtoCSV(newTable.strTableName,htblColNameType,strClusteringKeyColumn);
				theTables.add(newTable);
				
		//throw new DBAppException("not implemented yet");
	}


	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		
		throw new DBAppException("not implemented yet");
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException{
		Table targetTable = null;
        for (Table table : theTables) {
            if (table.strTableName.equals(strTableName)) {
                targetTable = table;
                break;
            }
        }
        if (targetTable == null) {
            throw new DBAppException("Table not found: " + strTableName);
        }

        // Get the clustering key column
        String clusteringKeyColumn = targetTable.strClusteringKeyColumn;

        // Get the value of the clustering key from the inserted data
        Object clusteringKeyValue = htblColNameValue.get(clusteringKeyColumn);

        // Find the correct page to insert the tuple
        Page targetPage = null;
        for (Page page : targetTable.pages) {
            Tuple firstTuple = page.getFirstTuple();
            Tuple lastTuple = page.getLastTuple();
            Object firstKey = firstTuple != null ? firstTuple.getValue(clusteringKeyColumn) : null;
            Object lastKey = lastTuple != null ? lastTuple.getValue(clusteringKeyColumn) : null;
            if ((firstKey == null || ((Comparable) clusteringKeyValue).compareTo(firstKey) >= 0) &&
                (lastKey == null || ((Comparable) clusteringKeyValue).compareTo(lastKey) <= 0)) {
                targetPage = page;
                break;
            }
        }
        if (targetPage == null) {
            // Create a new page if no suitable page is found
            targetPage = new Page();
            targetTable.pages.add(targetPage);
        }
/////////////////////////////////////////////////////////////////////////////////////
        // Insert the tuple into the page
        Vector<Tuple> temp = new Vector<>();
        boolean inserted = false;
        // Iterate through existing tuples to find the correct position to insert the new tuple
        for (Tuple tuple : targetPage.getTuples()) {
            // Compare primary key values
            Object primaryKeyValue = tuple.getValue(clusteringKeyColumn);
            if (((Comparable) clusteringKeyValue).compareTo(primaryKeyValue) < 0 && !inserted) {
                // If the new tuple's primary key is less than the current tuple's primary key, insert it into the temp vector
                temp.add(new Tuple(htblColNameValue));
                inserted = true; // Flag to indicate that the new tuple has been inserted
            }
            // Insert the current tuple into the temp vector
            temp.add(tuple);
        }

        // If the new tuple hasn't been inserted yet (e.g., if it's greater than all existing tuples), add it to the end
        if (!inserted) {
            temp.add(new Tuple(htblColNameValue));
        }

        // If the temp vector is still empty, it means the new tuple should be inserted at the end
        if (temp.isEmpty()) {
            temp.add(new Tuple(htblColNameValue));
        }

        // Replace the existing tuples with the temp vector
        targetPage.setTuples(temp);

        // Check if the page is full, then handle shifting if needed
        if (targetPage.isFull()) {
            // If it's the last page, create a new page
            if (targetTable.pages.indexOf(targetPage) == targetTable.pages.size() - 1) {
                Page newPage = new Page();
                targetTable.pages.add(newPage);
            } else {
                // Otherwise, shift a row down to the following page
                Page nextPage = targetTable.pages.get(targetTable.pages.indexOf(targetPage) + 1);
                nextPage.shiftRow(targetPage);
            }
        }

        // Save the table back to disk
        targetTable.saveToFile(strTableName + ".ser");
    }
		//throw new DBAppException("not implemented yet");
	


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, 
									String[]  strarrOperators) throws DBAppException{
										
		return null;
	}


	public static void main( String[] args ){
	
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
		 
		 


        // Load page from file
//        int totalTuples = 40000;
//        int N = 200;
//        int totalPages = totalTuples / N;
//
//        for (int i = 0; i < totalPages; i++) {
//            Page page = new Page();
//            for (int j = 0; j < N; j++) {
//                page.addTuple(new Tuple("Name" + (i * N + j), 20, "Address"));
//            }
//            page.saveToFile("page" + (i + 1) + ".bin");
//        }
  
         //System.out.println(CurrentPageNumber("page5"));
        // Load and display the first page to verify
//        Page loadedPage = Page.loadFromFile("page1.bin");
//        if (loadedPage != null) {
//            System.out.println("Loaded Page Contents: " + loadedPage.toString());
//        } else {
//            System.out.println("Failed to load page.");
//        }
//        
//		
		String strTableName = "Student";
		DBApp	dbApp = new DBApp( );
////			
			Hashtable htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			dbApp.createTable( strTableName, "id", htblColNameType );
			//dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//
			Hashtable htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343432 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

////		
			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 453455 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 5674567 ));
//			htblColNameValue.put("name", new String("Dalia Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.25 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			
//			  // Step 1: Read the serialized table file
			FileInputStream fileIn = new FileInputStream("Student.ser");

            // Step 2: Deserialize the table object
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Table table = (Table) objectIn.readObject();
            objectIn.close();
////
////            // Step 3: Access the tuples
////            // Iterate through each page in the table
////            for (Page page : table.getPages()) {
////                // Access the tuples within the page
////                Vector<Tuple> tuples = page.getTuples();
////                for (Tuple tuple : tuples) {
////                    // Access tuple attributes as needed
////                    // For example:
////                    Object attributeValue = tuple.getValue("id");
////                    System.out.println("Attribute Value: " + attributeValue);
////                }
////            }
			for (Page page : table.getPages()) {
			    Vector<Tuple> tuples = page.getTuples();
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
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 23498 ));
//			htblColNameValue.put("name", new String("John Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.5 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 78452 ));
//			htblColNameValue.put("name", new String("Zaky Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.88 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[2];
//			arrSQLTerms[0]._strTableName =  "Student";
//			arrSQLTerms[0]._strColumnName=  "name";
//			arrSQLTerms[0]._strOperator  =  "=";
//			arrSQLTerms[0]._objValue     =  "John Noor";
//
//			arrSQLTerms[1]._strTableName =  "Student";
//			arrSQLTerms[1]._strColumnName=  "gpa";
//			arrSQLTerms[1]._strOperator  =  "=";
//			arrSQLTerms[1]._objValue     =  new Double( 1.5 );
//
//			String[]strarrOperators = new String[1];
//			strarrOperators[0] = "OR";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
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
