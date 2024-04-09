package database_project1;

import java.io.*;
import java.util.*;

class Tuple implements Serializable {
	private Hashtable<String, String> hashtable;

    public Tuple(Hashtable<String, String> hashtable) {
        this.hashtable = new Hashtable<>(hashtable);
        createAttributes();
    }

    private void createAttributes() {
        for (String attributeName : hashtable.keySet()) {
            String attributeType = hashtable.get(attributeName);
            switch (attributeType) {
                case "java.lang.String":
                    String stringValue = "";
                    try {
                        this.getClass().getDeclaredField(attributeName).set(this, stringValue);
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        e.printStackTrace();
                    }
                    break;
                case "java.lang.Integer":
                    int intValue = 0;
                    try {
                        this.getClass().getDeclaredField(attributeName).set(this, intValue);
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        e.printStackTrace();
                    }
                    break;
                case "java.lang.Double":
                    double doubleValue = 0.0;
                    try {
                        this.getClass().getDeclaredField(attributeName).set(this, doubleValue);
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    System.out.println("Unsupported attribute type");
            }
        }
    }
    
	
	
}

class Page implements Serializable {
    private static final int N = 200; 
    private Vector<Tuple> tuples;

    public Page() {
        tuples = new Vector<>(N);
    }

    public int CurrentPageNumber(String PageName) {
        int pageNumber = 0;
            String[] parts = PageName.split("\\.");
            String[] nameParts = parts[0].split("page");
            pageNumber = Integer.parseInt(nameParts[1]);
       
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

