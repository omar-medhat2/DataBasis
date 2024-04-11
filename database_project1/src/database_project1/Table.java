package database_project1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable{
	String strTableName;
	String strClusteringKeyColumn;
	Hashtable<String,String> htblColNameType;
	Vector<Page> pages;
	
	public Table()
	{
		this.pages = new Vector<>();
	}
	
	public Table(String strTableName, String strClusteringKeyColumn) {
		this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.pages = new Vector<>();
    }
	
	 public boolean contains(Object key) {
	        for (Page page : pages) {
	           
	                // Check if the key is within the range of clustering keys for the tuples in the page
	                Tuple firstTuple = page.getFirstTuple();
	                Tuple lastTuple = page.getLastTuple();
	                Object firstKey = firstTuple != null ? firstTuple.getValue(strClusteringKeyColumn) : null;
	                Object lastKey = lastTuple != null ? lastTuple.getValue(strClusteringKeyColumn) : null;
	                if ((firstKey == null || ((Comparable) key).compareTo(firstKey) >= 0) &&
	                    (lastKey == null || ((Comparable) key).compareTo(lastKey) <= 0)) {
	                    return true;
	                }
	            }
	        
	        return false;
	    }
	
	public String getStrClusteringKeyColumn() {
		return strClusteringKeyColumn;
	}

	public String getStrTableName() {
		return strTableName;
	}


	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}


	public Vector<Page> getPages() {
		return pages;
	}


	public void saveToFile(String filename) {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename))) {
            out.writeObject(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	public static Table loadFromFile(String filename) {
        Table table = null;
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename))) {
            table = (Table) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return table;
    }
}
