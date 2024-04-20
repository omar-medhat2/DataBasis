package database_project1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class Table implements Serializable{
	String strTableName;
	String strClusteringKeyColumn;
	Hashtable<String,String> htblColNameType;
	public Vector<String> pages;
	
	public Table()
	{
		this.pages = new Vector<>();
	}
	
	public Table(String strTableName, String strClusteringKeyColumn) {
		this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.pages = new Vector<>();
    }
	
	public String getLastPage()
	{
		if (pages.isEmpty())
			return "Page-1.ser";
		else
		return pages.lastElement();
	}
	public String getIndexType() throws IOException {
		String keyColumnName = createcsv.getCluster(strTableName);
		String keyColumnType = createcsv.getIndexType(strTableName, keyColumnName);
		if(keyColumnType == "B+tree") {
			return "B+tree";
		}
		return null;
	}
	public String getIndexName() throws IOException {
		String keyColumnName = createcsv.getCluster(strTableName);
		String keyColumnType = createcsv.getIndexName(strTableName, keyColumnName);
		return keyColumnType;
	}
	public String getTypeOfClusterkingKey(Object clusteringKeyValue) throws IOException, ClassNotFoundException {

	    String keyColumnName = createcsv.getCluster(strTableName);
	    String keyColumnType = createcsv.getType(strTableName, keyColumnName);

	    if ("java.lang.double".equalsIgnoreCase(keyColumnType)) {
	    	return "double";
	    } 
	    else if ("java.lang.string".equalsIgnoreCase(keyColumnType)) {
	    	return "string";
	    } 
	    else {
	    	return "integer";
	    }
	}
	
	public List<Page> retrievePages() throws IOException, ClassNotFoundException {
	    List<Page> currentPage = new ArrayList<Page>();

	        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
	            currentPage.add(Page.loadFromFile(strTableName + pageIndex + ".ser"));
	        }

	    
	    return currentPage;
	}
	public List<String> retrievePageNames() throws IOException, ClassNotFoundException {
	    List<String> currentPage = new ArrayList<String>();

	        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
	            currentPage.add(strTableName + pageIndex + ".ser");
	        }

	    
	    return currentPage;
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


	public Vector<String> getPages() {
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
