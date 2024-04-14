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
		
	}
	


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
	   
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue entries are ANDED together
	public static void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
	
		
		Table targetTable = null;
		for (Table table : theTables) {
		    if (table.getStrTableName().equals(strTableName)) {
		        targetTable = table;
		        break;
		    }
		}

		if (targetTable == null) {
		    throw new DBAppException("Table not found: " + strTableName);
		}
		
		// Iterate over pages in the table
		for (Page page : targetTable.getPages()) {
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
		            // Check if the page is empty after removing the tuple
		            if (page.getTuples().isEmpty()) {
		                // Remove the empty page from the table
		                targetTable.getPages().remove(page);
		            }
		        }
		    }
		}
	    
	    

	    // Save the updated page(s) back to disk
	    for (Page page : targetTable.getPages()) {
	        page.saveToFile(strTableName + ".ser");
	    }
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
	   
	    Vector<Tuple> resultSet = new Vector<>();

	    
	    for (int i = 0; i < arrSQLTerms.length; i++) {
	        
	        Vector<Tuple> termResult = new Vector<>();
	        boolean tableFound = false;
	        for (Table table : theTables) {
	            if (table.getStrTableName().equals(arrSQLTerms[i]._strTableName)) {
	                tableFound = true;
	                
	                for (Page page : table.getPages()) {
	                    
	                    for (Tuple tuple : page.getTuples()) {
	                        
	                        if (tupleMatchesCriteria(tuple, arrSQLTerms[i])) {
	                            termResult.add(tuple);
	                        }
	                    }
	                }
	                break; 
	            }
	        }
	        
	        if (!tableFound) 
	            throw new DBAppException("Table not found: " + arrSQLTerms[i]._strTableName);
	        

	        
	        if (i == 0) {
	            resultSet.addAll(termResult);
	        } else {
	            String operator = strarrOperators[i - 1];
	            Vector<Tuple> result = new Vector<>();
	            switch (operator) {
	                case "AND":
	            	    for (Tuple tuple : resultSet) 
	            	        if (termResult.contains(tuple)) 
	            	            result.add(tuple);
	            	    
	            	    resultSet = result;
	            	    break;
	                case "OR":
	                	result = new Vector<>(resultSet);
	            	    for (Tuple tuple : termResult) 
	            	        if (!result.contains(tuple)) 
	            	            result.add(tuple);
	            	    
	            	    resultSet = result;
	                    break;
	                case "XOR":
	                	result = new Vector<>();
	            	    for (Tuple tuple : resultSet) 
	            	        if (!termResult.contains(tuple)) 
	            	            result.add(tuple);
	            	    
	            	    for (Tuple tuple : termResult) 
	            	        if (!resultSet.contains(tuple)) 
	            	            result.add(tuple);
	            	    
	            	    resultSet = result;
	                    break;
	                default:
	                    throw new IllegalArgumentException("Unsupported starroperator: " + operator);
	            }
	        }
	    }

	    return resultSet.iterator();
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
			
//
			Hashtable htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343432));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			//dbApp.insertIntoTable( strTableName , htblColNameValue );
			Tuple x = new Tuple(htblColNameValue,"id");
			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343433));
			htblColNameValue.put("name", new String("Ahmed Soroor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			//dbApp.insertIntoTable( strTableName , htblColNameValue );
			Tuple y = new Tuple(htblColNameValue,"id");
			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343434));
			htblColNameValue.put("name", new String("Ahmed Ghandour" ) );
			htblColNameValue.put("gpa", new Double( 0.75 ) );
			Tuple z = new Tuple(htblColNameValue,"id");
			
			Page Page1 = new Page("id");
			theTables.get(0).pages.add(Page1);
			theTables.get(0).pages.elementAt(0).insertTuple(x);
			theTables.get(0).pages.elementAt(0).insertTuple(y);
			theTables.get(0).pages.elementAt(0).insertTuple(z);
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

			String[] strarrOperators = new String[1];
			strarrOperators[0] = "AND";
			// select * from Student where name = “John Noor” or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
			System.out.println("Result Set:");
			while (resultSet.hasNext()) {
			    Tuple tuple = (Tuple) resultSet.next();
			    System.out.println(tuple); // Assuming Tuple class overrides the toString() method
			}
			
			

//////		
			//htblColNameValue.clear( );
			//htblColNameValue.put("id", new Integer( 500000 ));
			//htblColNameValue.put("name", new String("Ahmed Noor" ) );
			//htblColNameValue.put("gpa", new Double( 0.8 ) );
////			dbApp.insertIntoTable( strTableName , htblColNameValue );
			//dbApp.updateTable(strTableName, "2343432", htblColNameValue);

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
