package database_project1;

import java.io.*;
import java.util.*;

class Tuple implements Serializable {
    private String name;
    private int age;
    private String address;

    public Tuple(String name, int age, String address) {
        this.name = name;
        this.age = age;
        this.address = address;
    }

    @Override
    public String toString() {
       return name + "," + age + "," + address;
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
    
    public void addTuple(Tuple tuple) {
        if (tuples.size() < N) {
            tuples.add(tuple);
        } else {
            System.out.println("Page is full. Creating a new page");
            Page newPage = new Page();
            newPage.addTuple(tuple);
            int currentPageNumber = 0;
            newPage.saveToFile("page" + (currentPageNumber + 1) + ".bin");
        }
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


