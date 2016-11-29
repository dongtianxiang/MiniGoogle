package edu.upenn.cis455.database;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class DBManager {
	
	public static Map<String, DBInstance> instances = new HashMap<>();
	public DBManager() {
		
	}
	
	public static void createDBInstance(String directory) {
		
		DBInstance instance = new DBInstance(directory);
		instances.put(directory, instance);
	}
	
	public static DBInstance getDBInstance(String directory) 
			throws IllegalArgumentException {
		
		if (!instances.containsKey(directory)) {
			createDBInstance(directory);
		}
		return instances.get(directory);
	}
	
	public static void deleteInstance(String directory) throws IllegalArgumentException {
		
		File dir = new File(directory);
		if (!dir.exists()||!dir.isDirectory()) throw new IllegalArgumentException();
		File[] files = dir.listFiles();
		for (File f: files) {
			f.delete();
		}
		
		synchronized(instances) {
			instances.remove(directory);
		}
	}
	
	

}
