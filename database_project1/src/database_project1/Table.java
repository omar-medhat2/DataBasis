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
	
	public Page retrievePageByClusteringKey(Object clusteringKeyValue) throws IOException, ClassNotFoundException {
	    int comparisonResult = 1;
	    Page currentPage = null;

	    String keyColumnName = createcsv.getCluster(strTableName);
	    String keyColumnType = createcsv.getType(strTableName, keyColumnName);

	    if ("java.lang.double".equalsIgnoreCase(keyColumnType)) {
	        for (int pageIndex = 0; pageIndex < pages.size() && comparisonResult != -1 && comparisonResult != 0; pageIndex++) {
	            currentPage = Page.loadFromFile(strTableName + "_" + pageIndex);
//	            comparisonResult = Double.compare(Double.parseDouble((String) clusteringKeyValue), (Double) currentPage.max);
	        }
	    } 
	    else if ("java.lang.string".equalsIgnoreCase(keyColumnType)) {
	        for (int pageIndex = 0; pageIndex < pages.size() - 1 && comparisonResult != -1 && comparisonResult != 0; pageIndex++) {
	            currentPage = Page.loadFromFile(strTableName + "_" + pageIndex);
//	            comparisonResult = ((String) clusteringKeyValue).compareTo((String) currentPage.max);
	        }
	    } 
	    else {
	        for (int pageIndex = 0; pageIndex < pages.size() && comparisonResult != -1 && comparisonResult != 0; pageIndex++) {
	            currentPage = Page.loadFromFile(strTableName + "_" + pageIndex);
//	            comparisonResult = Integer.compare(Integer.parseInt((String) clusteringKeyValue), (Integer) currentPage.max);
	        }
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
