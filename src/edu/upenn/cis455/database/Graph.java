package edu.upenn.cis455.database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Graph {
	
	private DBInstance database = null;	
	public Graph(String dataDir) {
		DBManager.createDBInstance(dataDir);
		database = DBManager.getDBInstance(dataDir);
	}
	
	public void buildGraphFromFile(String path) throws IllegalStateException {
		
		File srcFile = new File (path);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(srcFile));
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		if (reader == null) throw new IllegalStateException();
		while(true) {
			
			String line = null;
			try {
				line = reader.readLine();
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			
			if (line == null) break;
			String[] parts = line.split(" -> ");
			
			Node node = new Node(parts[0]);
			for (String neighbor: parts[1].split(", ")) {
				node.addNeighbor(neighbor);
			}
			database.addNode(node);
		}
		
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public Node getNode(String id) {
		return database.getNode(id);
	}

}
