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

	public int getPageNumber(String pageName) {
	    StringBuilder pageNumberStr = new StringBuilder();
	    boolean isNegative = false;

	    for (int i = 0; i < pageName.length(); i++) {
	        char ch = pageName.charAt(i);
	        if (Character.isDigit(ch) || (ch == '-' && pageNumberStr.length() == 0)) {
	            // Include digits and allow the negative sign only if it's the first character
	            if (ch == '-') {
	                isNegative = true;
	            } else {
	                pageNumberStr.append(ch);
	            }
	        } else if (pageNumberStr.length() > 0) {
	            // If a digit or negative sign has been found previously, stop when a non-digit character is encountered
	            break;
	        }
	    }

	    // Convert the extracted string of digits to an integer
	    int pageNumber = pageNumberStr.length() > 0 ? Integer.parseInt(pageNumberStr.toString()) : 0;

	    // If a negative sign was found, make the page number negative
	    if (isNegative) {
	        pageNumber *= -1;
	    }

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
	                                Hashtable<String,String> htblColNameType) throws DBAppException {
	    // Check if the table file already exists
	    File tableFile = new File(strTableName + ".ser");
//	    if (tableFile.exists()) {
//	        throw new DBAppException("Table already exists: " + strTableName);
//	    }

	    // Create a new table
	    Table newTable = new Table();
	    newTable.strTableName = strTableName;
	    newTable.strClusteringKeyColumn = strClusteringKeyColumn;
	    createcsv.addtoCSV(newTable.strTableName, htblColNameType, strClusteringKeyColumn);
	    newTable.saveToFile(strTableName + ".ser");
	}



	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		
		throw new DBAppException("not implemented yet");
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
	    // Get the clustering key column from metadata
	    String clusteringKeyColumnTest = createcsv.getCluster(strTableName);
		
		

	    // Check if clustering key column exists
//	    if (clusteringKeyColumn == null) {
//	        throw new DBAppException("Clustering key column not found for table: " + strTableName);
//	    }

	    // Load the target table from file
	    Table targetTable = Table.loadFromFile(strTableName + ".ser");

	    // Check if the target table exists
	    if (targetTable == null) {
	        throw new DBAppException("Table not found: " + strTableName);
	    }
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	   
	    String clusteringKeyColumn = targetTable.strClusteringKeyColumn;
	    
	    Object clusteringKeyValue = htblColNameValue.get(clusteringKeyColumn);

	    
	    Page targetPage = null;

	    
	    for (String page : targetTable.getPages()) {
	        Page currPage = Page.loadFromFile(page);
	        Tuple lastTuple = currPage.getLastTuple();
	        Object primaryKeyValue = lastTuple.getValue(clusteringKeyColumn);
	        if (((Comparable) clusteringKeyValue).compareTo(primaryKeyValue) < 0 || !currPage.isFull()) {
	            targetPage = currPage;
	            break;
	        }
	    }

	    
	    int incPageNumber = 0;
	    if (targetPage == null) {
	        targetPage = new Page();
	        String lastPage = targetTable.getLastPage();
	        int lastPageNumber = getPageNumber(lastPage);
	       incPageNumber = lastPageNumber + 1;
	       
	        targetPage.saveToFile(strTableName + (incPageNumber) + ".ser");
	        targetTable.getPages().add(strTableName + (incPageNumber) + ".ser");
	    }
	   
//	    
//	    String clusteringKeyColumn = targetTable.getStrClusteringKeyColumn();
//        Object clusteringKeyValue = htblColNameValue.get(clusteringKeyColumn);
//        
//        Page targetPage = null;
//        
//        List<String> pages = targetTable.getPages();
//        Collections.sort(pages); // Assuming pages are sorted
//        
//        int low = 0;
//        int high = pages.size() - 1;
//        
//        while (low <= high) {
//            int mid = low + (high - low) / 2;
//            Page currPage = Page.loadFromFile(pages.get(mid));
//            Tuple firstTuple = currPage.getFirstTuple();
//            Tuple lastTuple = currPage.getLastTuple();
//            Object firstPrimaryKeyValue = firstTuple.getValue(clusteringKeyColumn);
//            Object lastPrimaryKeyValue = lastTuple.getValue(clusteringKeyColumn);
//            
//            if (((Comparable) clusteringKeyValue).compareTo(lastPrimaryKeyValue) > 0) {
//                // If clustering key value is greater than the last tuple's value,
//                // search in the upper half
//                low = mid + 1;
//            } else if (((Comparable) clusteringKeyValue).compareTo(firstPrimaryKeyValue) < 0) {
//                // If clustering key value is less than the first tuple's value,
//                // search in the lower half
//                high = mid - 1;
//            } else {
//                // If clustering key value is in range of the current page's tuples
//                targetPage = currPage;
//                break;
//            }
//        }
//        int incPageNumber = 0;
//        if (targetPage == null) {
//            // If target page is still null, create a new page
//            targetPage = new Page();
//            String lastPage = targetTable.getLastPage();
//            int lastPageNumber = getPageNumber(lastPage);
//            incPageNumber = lastPageNumber + 1;
//            targetPage.saveToFile(strTableName + (incPageNumber) + ".ser");
//            targetTable.getPages().add(strTableName + (incPageNumber) + ".ser");
//        }
/////////////////////////////////////////////////////////////////////////////////////////////////////////
	    // Check if the target page is full
	    if (!targetPage.isFull()) {
	        Vector<Tuple> temp = new Vector<>();
	        boolean inserted = false;

	        // Iterate over tuples in the target page
	        for (Tuple tuple : targetPage.getTuples()) {
	            Object primaryKeyValue = tuple.getValue(clusteringKeyColumn);
	            if (((Comparable) clusteringKeyValue).compareTo(primaryKeyValue) == 0 )
	            {
	            	throw new DBAppException("Tuple with same Clustering key already inserted");
	            }
	            
	            // Compare primary key values to find the correct position to insert the new tuple
	            if (((Comparable) clusteringKeyValue).compareTo(primaryKeyValue) < 0 && !inserted) {

	            	temp.add(new Tuple(htblColNameValue,clusteringKeyColumn));
	                inserted = true;
	            }
	            temp.add(tuple);
	        }

	        // If the new tuple hasn't been inserted yet, add it to the end
	        if (!inserted) {
	            temp.add(new Tuple(htblColNameValue,clusteringKeyColumn));
	        }

	        // Update the target page with the new tuples
	        targetPage.setTuples(temp);
	    } else {
	        // If the target page is full
	        if (targetTable.getPages().indexOf(targetPage) == targetTable.getPages().size() - 1) {
	            // If it's the last page, create a new page
	            targetPage = new Page();
	            String lastPage = targetTable.getLastPage();
	            int lastPageNumber = getPageNumber(lastPage);
	            incPageNumber = lastPageNumber + 1;
	            targetPage.saveToFile(strTableName + (incPageNumber) + ".ser");
	            targetTable.getPages().add(strTableName + (incPageNumber) + ".ser");
	        } else {
	            // Otherwise, shift a row down to the following page
	            String nextPageString = targetTable.getPages().get(targetTable.getPages().indexOf(targetPage) + 1);
	            Page nextPage = Page.loadFromFile(nextPageString);
	            Tuple shiftedTuple = targetPage.getLastTuple();
	            targetPage.getTuples().remove(targetPage.getTuples().size() - 1);
	            Vector<Tuple> temp = new Vector<>();
	            boolean inserted = false;

	            // Iterate through existing tuples to find the correct position to insert the new tuple
	            for (Tuple tuple : targetPage.getTuples()) {
	                // Compare primary key values
	                Object primaryKeyValue = tuple.getValue(clusteringKeyColumn);
	                if (((Comparable) clusteringKeyValue).compareTo(primaryKeyValue) < 0 && !inserted) {
	                    temp.add(new Tuple(htblColNameValue,clusteringKeyColumn));
	                    inserted = true;
	                }
	                temp.add(tuple);
	            }

	            // If the new tuple hasn't been inserted yet, add it to the end
	            if (!inserted) {
	                temp.add(new Tuple(htblColNameValue,clusteringKeyColumn));
	            }

	            // Update the target page with the new tuples
	            targetPage.setTuples(temp);

	            Vector<Tuple> tmp = new Vector<>();
	            tmp.add(shiftedTuple);
	            for (Tuple tuple : nextPage.getTuples()) {
	                if (!tuple.equals(shiftedTuple)) {
	                    tmp.add(tuple);
	                }
	            }
	            nextPage.setTuples(tmp);
	        }
	    }
	    String lastPage = targetTable.getLastPage();
        int lastPageNumber = getPageNumber(lastPage);
       incPageNumber = lastPageNumber ;
	    targetPage.saveToFile(strTableName + (incPageNumber) + ".ser");
	    targetTable.saveToFile(strTableName + ".ser");

	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{

		Table targetTable = Table.loadFromFile(strTableName + ".ser");
        if (targetTable == null) {
            throw new DBAppException("Table not found: " + strTableName);
        }
        
        List<Page> targetPage = null;
		try {
			targetPage = targetTable.retrievePageByClusteringKey(strClusteringKeyValue);
//			System.out.println(targetTable.getIndexByClusteringKey(strClusteringKeyValue));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    if (targetPage == null) {
	        throw new RuntimeException("Row with clustering key " + strClusteringKeyValue + " not found in table " + strTableName);
	    }
	    String clusteringKeyColumn = targetTable.getStrClusteringKeyColumn();
	    // Find the tuple to update
	    Tuple targetTuple = null;
	    int pageIndex = 0;
//	    String indexType = targetTable.getIndexByClusteringKey(strClusteringKeyValue);
	    
//	    if ("B+tree".equals(indexType)) {
//	        // Handle B+ tree indexed table update
//	        BPlusTree<Object, Object> bPlusTree = targetTable.getBPlusTree();
//	        if (bPlusTree == null) {
//	            throw new DBAppException("B+ tree index not found for table: " + strTableName);
//	        }
//
//	        // Convert the clustering key value to the appropriate type
//	        clusteringKeyColumn = targetTable.getStrClusteringKeyColumn();
//	        Object clusteringKeyValueObject = convertToType(strClusteringKeyValue, targetTable.getTypeOfClusterkingKey(clusteringKeyColumn));
//
//	        // Update using B+ tree
//	        if (!bPlusTree.update(clusteringKeyValueObject, null, htblColNameValue)) {
//	            throw new DBAppException("Failed to update row with clustering key " + strClusteringKeyValue + " in table " + strTableName);
//	        }
//
//	        // Save the updated B+ tree and table
////	        bPlusTree.saveToFile(strTableName + "_BPlusTree.ser");
//	        targetTable.saveToFile(strTableName + ".ser");
//	    } else {
	    int j = 0;
	    int tupleIndex = 0;
		    for(int i = 0;i<targetPage.size();i++) {
//		    	System.out.println(((Page)targetPage.get(i)).getTuples());
			    for (Tuple tuple : ((Page)targetPage.get(i)).getTuples()) {
			    	Object primaryKeyValue = tuple.getValue(clusteringKeyColumn);;
			        if (strClusteringKeyValue.equals(primaryKeyValue.toString())) {
			        	pageIndex = i;
			        	tupleIndex = j;
			            targetTuple = tuple;
			            break;
			        }
			        j++;
			    }
		    }
		    if (targetTuple == null) {
		        throw new RuntimeException("Row with clustering key " + strClusteringKeyValue + " not found in table " + strTableName);
		    }
		    // Update the tuple with new values
		    for (String columnName : htblColNameValue.keySet()) {
		        if (!columnName.equals(targetTable.getStrClusteringKeyColumn())) {
		        	Object newValue;
		        	
		        	newValue = htblColNameValue.get(columnName);
		            targetTuple.updateTuple(columnName, newValue);
		        }
		    }
//			    for (Tuple tuple : ((Page)targetPage.get(pageIndex)).getTuples()) {
//			    	System.out.println(tuple);
//			    }
		    
		    // Save the updated page back to disk
//		}
		int pageIndexForSave = (int) Math.floor(tupleIndex/(targetPage.get(pageIndex)).noOfTuples());
//		System.out.println(pageIndexForSave);
	    (targetPage.get(pageIndex)).saveToFile("Student" + pageIndexForSave +".ser");
	    targetTable.saveToFile(strTableName + ".ser");

//		try {
//			targetPage = targetTable.retrievePageByClusteringKey(strClusteringKeyValue);
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
////		System.out.println(targetPage.size());
//	    targetTable = Table.loadFromFile(strTableName + ".ser");
//	    System.out.println(((Page)targetPage.get(0)));
//	    System.out.println(((Page)targetPage.get(1)));
////	    System.out.println(((Page)targetPage.get(0)));
//	    System.out.println(((Page)targetPage.get(2)));
//		    for (Tuple tuple : ((Page)targetPage.get(0)).getTuples()) {
////		    	System.out.println(targetPage.get(pageIndex));
//		    }
//		throw new DBAppException("not implemented yet");

//		throw new DBAppException("not implemented yet");
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

	public static void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
	    // Load the target table from file
	    Table targetTable = Table.loadFromFile(strTableName + ".ser");

	    // Check if the target table exists
	    if (targetTable == null) 
	        throw new DBAppException("Table not found: " + strTableName);
	    

	    // Iterate over page files in the table
	    for (String pageFile : targetTable.getPages()) {
	    	
	    	
	        
	        Page page = Page.loadFromFile(pageFile);

	        // Iterate over tuples in the page
	        Iterator<Tuple> tupleIterator = page.getTuples().iterator();
	        while (tupleIterator.hasNext()) {
	            Tuple tuple = tupleIterator.next();

	            // Check if the tuple matches all conditions in the hashtable
	            boolean allConditions = true;
	            for (String attributeName : htblColNameValue.keySet()) {
	                Object attributeValue = htblColNameValue.get(attributeName);
	                if (!tuple.getValue(attributeName).equals(attributeValue)) {
	                    allConditions = false;
	                    break;
	                }
	            }

	            // If the tuple matches all conditions, remove it from the tuple vector
	            if (allConditions) {
	                tupleIterator.remove();
	            }
	        }

	        // If the page is empty after removing tuples, remove it from the table
	        if (page.getTuples().isEmpty()) {
	            targetTable.getPages().remove(pageFile);
	            // Delete the page file from the file system
	            File file = new File(pageFile);
	            if (!file.delete()) 
	                System.err.println("Failed to delete page file: " + pageFile);
	            if (targetTable.getPages().isEmpty()) 
		            return;
	        } else {
	            // Save changes to page file if it's not empty
	            page.saveToFile(pageFile);
	        }
	    }

	    // Save changes to table file
	    targetTable.saveToFile(strTableName + ".ser");
	}




	public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
	    // List to hold the result set
	    Vector<Tuple> resultSet = new Vector<>();

	    // Iterate over each SQL term
	    for (int i = 0; i < arrSQLTerms.length; i++) {
	        // Get the result tuples for the current SQL term
	        Vector<Tuple> termResult = new Vector<>();
	        
	        
	        // Load the target table from file
	        Table targetTable = Table.loadFromFile(arrSQLTerms[i]._strTableName + ".ser");
	        
	        // Check if the table exists
	        if (targetTable == null) {
	            throw new DBAppException("Table not found: " + arrSQLTerms[i]._strTableName);
	        }
	        
	        // Iterate over page file paths in the table
	        for (String pageFilePath : targetTable.getPages()) {
	            // Load the page from file
	            Page page = Page.loadFromFile(pageFilePath);
	            
	            // Iterate over tuples in the page
	            for (Tuple tuple : page.getTuples()) {
	                // Check if the tuple matches the SQL term criteria
	                if (tupleMatchesCriteria(tuple, arrSQLTerms[i])) {
	                    termResult.add(tuple);
	                }
	            }
	        }
	        
	        // Apply logical operation between result tuples and resultSet
	        if (i == 0) {
	            resultSet.addAll(termResult);
	        } else {
	            String operator = strarrOperators[i - 1];
	            Vector<Tuple> result = new Vector<>();
	            
	            switch (operator) {
	                case "AND":
	                    for (Tuple tuple : resultSet) {
	                        if (containsTupleWithClusteringKey(termResult, tuple)) {
	                            result.add(tuple);
	                        }
	                    }
	                    resultSet = result;
	                    break;
	                    
	                case "OR":
	                    result.addAll(resultSet);
	                    for (Tuple tuple : termResult) {
	                        if (!containsTupleWithClusteringKey(result, tuple)) {
	                            result.add(tuple);
	                        }
	                    }
	                    resultSet = result;
	                    break;
	                    
	                case "XOR":
	                    for (Tuple tuple : resultSet) {
	                        if (!containsTupleWithClusteringKey(termResult, tuple)) {
	                            result.add(tuple);
	                        }
	                    }
	                    for (Tuple tuple : termResult) {
	                        if (!containsTupleWithClusteringKey(resultSet, tuple)) {
	                            result.add(tuple);
	                        }
	                    }
	                    resultSet = result;
	                    break;
	                    
	                default:
	                    throw new IllegalArgumentException("Unsupported operator: " + operator);
	            }
	        }

	        // Method to check if a vector of tuples contains a tuple with the same clustering key value
	        
	    }

	    return resultSet.iterator();

	}

	private boolean containsTupleWithClusteringKey(Vector<Tuple> tuples, Tuple targetTuple) {
        Object targetKeyValue = targetTuple.getClusteringKeyValue();
        for (Tuple tuple : tuples) {
            Object keyValue = tuple.getClusteringKeyValue();
            if (keyValue.equals(targetKeyValue)) {
                return true;
            }
        }
        return false;
    }


	private boolean tupleMatchesCriteria(Tuple tuple, SQLTerm sqlTerm) throws DBAppException{
	    
	    Object tupleValue = tuple.getValue(sqlTerm._strColumnName);
	    
	    
	    if (tupleValue instanceof String) {
	        
	        if (sqlTerm._strOperator.equals("=")) {
	            return tupleValue.equals(sqlTerm._objValue);
	        } else if (sqlTerm._strOperator.equals("!=")) {
	            return !tupleValue.equals(sqlTerm._objValue);
	        } else {
	            
	        	throw new DBAppException("Unsupported operator for strings: " + sqlTerm._strOperator);
	        }
	    } else if (tupleValue instanceof Number && sqlTerm._objValue instanceof Number) {
	       
	        double tupleDouble = ((Number) tupleValue).doubleValue();
	        double queryDouble = ((Number) sqlTerm._objValue).doubleValue();
	        switch (sqlTerm._strOperator) {
	            case "=":
	                return tupleDouble == queryDouble;
	            case "!=":
	                return tupleDouble != queryDouble;
	            case ">":
	                return tupleDouble > queryDouble;
	            case ">=":
	                return tupleDouble >= queryDouble;
	            case "<":
	                return tupleDouble < queryDouble;
	            case "<=":
	                return tupleDouble <= queryDouble;
	            default:
	                
	            	throw new DBAppException("Unsupported operator: " + sqlTerm._strOperator);
	        }
	    } else 
	        throw new DBAppException("Invalid Comparison: " + tupleValue.getClass().getName());
	    
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
			dbApp.createTable( strTableName, "id", htblColNameType );
			
//			//dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
////
			
			//Hashtable htblColNameValue = new Hashtable( );
			//htblColNameValue.put("id", new Integer( 1 ));

			
			
			Hashtable htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 1));

			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.5) );

			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2));
			htblColNameValue.put("name", new String("Mohamed" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );
			
//			
//			htblColNameValue = new Hashtable( );
//			htblColNameValue.put("id", new Integer( 3));
//			htblColNameValue.put("name", new String("sadjkasdk" ) );
//			htblColNameValue.put("gpa", new Double( 2.5 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//			
//			htblColNameValue = new Hashtable( );
//			htblColNameValue.put("id", new Integer( 4));
//			htblColNameValue.put("name", new String("amr" ) );
//			htblColNameValue.put("gpa", new Double( 1.8) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//			
//			htblColNameValue = new Hashtable( );
//			htblColNameValue.put("id", new Integer( 5));
//			htblColNameValue.put("name", new String("ziyad" ) );
//			htblColNameValue.put("gpa", new Double( 1.7) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
			
			htblColNameValue = new Hashtable( );
//			htblColNameValue.put("id", new Integer( 2343434));
			htblColNameValue.put("name", new String("yousef" ) );
			htblColNameValue.put("gpa", new Double( 0.75 ) );
			dbApp.updateTable( strTableName ,"2", htblColNameValue );
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
			
//			SQLTerm[] arrSQLTerms = new SQLTerm[3];
//			arrSQLTerms[0] = new SQLTerm(); // Initialize the first element
//			arrSQLTerms[0]._strTableName = "Student";
//			arrSQLTerms[0]._strColumnName= "name";
//			arrSQLTerms[0]._strOperator = "!=";
//			arrSQLTerms[0]._objValue = "Ahmed Noor";
//
//			arrSQLTerms[1] = new SQLTerm(); // Initialize the second element
//			arrSQLTerms[1]._strTableName = "Student";
//			arrSQLTerms[1]._strColumnName= "gpa";
//			arrSQLTerms[1]._strOperator = "!=";
//			arrSQLTerms[1]._objValue = new Double(0.95);
//			
//			arrSQLTerms[2] = new SQLTerm(); // Initialize the second element
//			arrSQLTerms[2]._strTableName = "Student";
//			arrSQLTerms[2]._strColumnName= "gpa";
//			arrSQLTerms[2]._strOperator = "=";
//			arrSQLTerms[2]._objValue = new Double(0.75);
//
//			String[] strarrOperators = new String[2];
//			strarrOperators[0] = "XOR";
//			strarrOperators[1] = "OR";
//			// select * from Student where name = “John Noor” or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//			System.out.println("Result Set:");
//			while (resultSet.hasNext()) {
//			    Tuple tuple = (Tuple) resultSet.next();
//			    System.out.println(tuple); // Assuming Tuple class overrides the toString() method
//			}
//			
//			
//			
//			///////////////////////////////////////////////////////////
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
