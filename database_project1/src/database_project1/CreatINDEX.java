package database_project1;

import java.io.IOException;

public class CreatINDEX {
	public void createIndex(String strTableName, String strRowName, String strIndexName) throws DBAppException, IOException, ClassNotFoundException {

	    // Adjusting metadata file with new index
	    boolean Isinserted = createcsv.updateIndex(strTableName, strRowName, strIndexName);
	    if (!Isinserted) {
	        DBAppException e = new DBAppException("The index " + strIndexName + " already exists for the table  " + strTableName);
	        throw e;
	    }

	    // Retrieving data type for desired column
	    String DataType = createcsv.getType(strTableName, strRowName);

	    // Initialising b+tree
	    BPTree BPlustree = null;
	    if (DataType.equalsIgnoreCase("java.lang.Integer")) {
	        BPlustree = new BPlusTree<Integer>(15);
	    } else if (DataType.equalsIgnoreCase("java.lang.String")) {
	        BPlustree = new BPlusTree<String>(15);
	    } else if (DataType.equalsIgnoreCase("java.lang.Double")) {
	        BPlustree = new BPlusTree<Double>(15);
	    } else {
	        DBAppException e = new DBAppException("Couldn't be read.");
	        throw e;
	    }

	   // Deserialize the current table
	Table currentTable = Table.loadFromFile(strTableName);

	// Iterate through each page in the table
	for (String serializedPage : currentTable.pages) {
	    // Deserialize the current page
	    Page currentPage = Page.loadFromFile(serializedPage);
	    
	    // Iterate through each tuple in the current page
	    for (Tuple currentTuple : currentPage.getTuples()) {
	        // Retrieve the value associated with the given column name
	        Object value = currentTuple.getValue().get(strRowName);
	        
	        // Insert the value and a reference to the tuple into the tree
	        BPlustree.insert((currentPage.PageNumber) , (Comparable) value);
	    }
	}

	BPlustree.serialize(strTableName, strIndexName);

	}
	//public void serialize(String tableName, String indexName) {
	 //   String filePath = "YOUR SERIALIZATION PATH";
	   // try (ObjectOutputStream objectOut = new ObjectOutputStream(new FileOutputStream(filePath))) {
	     //   objectOut.writeObject(this);
	    //} catch (IOException e) {
	      //  e.printStackTrace();
//	     //}
	// //}
	// public static BPTree deserialize(String tableName, String colName) {
//	     String indexFileName = createcsv.retrieveIndexName(tableName, colName);
//	     BPTree deserializedTree = null;
//	     String filePath = "YOUR SERIALIZATION PATH";

//	     try {
//	         FileInputStream fileInputStream = new FileInputStream(filePath);
//	         ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);

//	         deserializedTree = (BPTree) objectInputStream.readObject();
//	         objectInputStream.close();
//	         fileInputStream.close();
//	     } catch (IOException | ClassNotFoundException e) {
//	         e.printStackTrace();
//	     }

//	     return deserializedTree;
	// }
}
