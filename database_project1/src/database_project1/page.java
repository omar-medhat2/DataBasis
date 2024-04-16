package database_project1;

import java.io.*;
import java.util.*;

class Tuple implements Serializable {
	private Hashtable<String, Object> tuples;
	private String clusteringKeyColumn;
	
    public Tuple() {
        this.tuples = new Hashtable<>();
    }

    public Tuple(Hashtable<String, Object> tuples) {
        this.tuples = tuples;
    }
    
    public Tuple(Hashtable<String, Object> tuples, String clusteringKeyColumn) {
        this.tuples = tuples;
        this.clusteringKeyColumn = clusteringKeyColumn;
    }
    
    public void addTuple(String attribute, Object value) {
        tuples.put(attribute, value);
    }
    public void updateTuple(String attribute, Object newValue) {
        if (tuples.containsKey(attribute)) {
            tuples.put(attribute, newValue);
        } else {
            System.out.println("Attribute '" + attribute + "' does not exist in the tuple.");
        }
    }


    public Object getValue(String attribute) {
        return tuples.get(attribute);
    }
    public Object getClusteringKeyValue() {
        return tuples.get(clusteringKeyColumn);
    }
    
    public void saveToFile(String filename) {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename))) {
            out.writeObject(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Tuple loadFromFile(String filename) {
        Tuple tuple = null;
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename))) {
            tuple = (Tuple) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return tuple;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, Object> entry : tuples.entrySet()) {
            stringBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        return stringBuilder.toString();
    }
    
}




class Page implements Serializable {
    private static final int N = 200; 
    private Vector<Tuple> tuples;
    private String strClusteringKeyColumn;
    private Table parentTable;
    
    public Page() {
        tuples = new Vector<>(N);
    }

    public Page(String strClusteringKeyColumn) {
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        tuples = new Vector<>();
    }
    
    public Page(String strClusteringKeyColumn, Table parentTable) {
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.parentTable = parentTable;
        tuples = new Vector<>();
    }
    
    public void insertTuple(Tuple tuple) {
        tuples.add(tuple);
    }
    
    public void setTuples(Vector<Tuple> tuples) {
        this.tuples = tuples;
    }
    
    public boolean isFull() {
        return tuples.size() >= N;
    }
    
    public void shiftRow(Page sourcePage) {
        if (!sourcePage.tuples.isEmpty()) {
            Tuple lastTuple = sourcePage.tuples.remove(sourcePage.tuples.size() - 1);
            tuples.add(0, lastTuple);
        }
    }
    
    public Vector<Tuple> getTuples() {
        return tuples;
    }
    
    public static int getPageNumber(String pageName) {
    	
        StringBuilder pageNumberStr = new StringBuilder();

        for (int i = 0; i < pageName.length(); i++) {
            char ch = pageName.charAt(i);
            if (Character.isDigit(ch)) {
                pageNumberStr.append(ch);
            } else if (pageNumberStr.length() > 0) {
                break;
            }
        }
        int pageNumber = Integer.parseInt(pageNumberStr.toString());

        return pageNumber;
    }
    @Override
    public String toString() {
        StringBuilder curr = new StringBuilder();
        for (Tuple tuple : tuples) {
            curr.append(tuple.toString());
            curr.append(",");
        }
        return curr.toString();
    }
    
    public Tuple getFirstTuple() {
        if (!tuples.isEmpty()) {
            return tuples.firstElement(); // Return the first tuple
        }
        return null; // Return null if page is empty
    }

    public Tuple getLastTuple() {
        if (!tuples.isEmpty()) {
            return tuples.lastElement(); // Return the last tuple
        }
        return null; // Return null if page is empty
    }
    
    public void saveToFile(String filename) {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename))) {
            out.writeObject(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Page loadFromFile(String filename) {
        Page page = null;
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename))) {
            page = (Page) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return page;
    }
}
//public void addTuple(Tuple tuple) {
//if (tuples.size() < N) {
//  tuples.add(tuple);
//} else {
//  System.out.println("Page is full. Creating a new page");
//  Page newPage = new Page();
//  newPage.addTuple(tuple);
//  int currentPageNumber = 0;
//  newPage.saveToFile("page" + (currentPageNumber + 1) + ".bin");
//}
//}

