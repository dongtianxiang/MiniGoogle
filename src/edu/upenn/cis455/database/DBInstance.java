package edu.upenn.cis455.database;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class DBInstance {
	
	public String envDirectory = null;
	public DBInstance instance = null;
	public Environment myEnv;
	public EntityStore store;
	
	public DBInstance(String envDir) throws IllegalArgumentException {
		
		envDirectory = envDir;
		try {
			
			EnvironmentConfig econfig = new EnvironmentConfig();			
			econfig.setAllowCreate(true);
			econfig.setTransactional(true);			
			File dataDir = new File(envDirectory);
			
			if (!dataDir.exists()) {
				if(!dataDir.mkdirs()) {
					throw new IllegalArgumentException();
				}
				dataDir.setReadable(true);
				dataDir.setWritable(true);
			}
			
			myEnv = new Environment(dataDir, econfig);			
			StoreConfig sconfig = new StoreConfig();			
			
			sconfig.setAllowCreate(true);
			sconfig.setTransactional(true);
			store = new EntityStore(myEnv, "DBStore", sconfig);

			print("BerkleyDB instance created at " + dataDir.getAbsolutePath());				
		}
		catch (DatabaseException dbe) {
			dbe.printStackTrace();
		}
		catch (Exception ge) {
			ge.printStackTrace();
		}
	}
	
	public Environment getEnvironment() {
		return myEnv;
	}
	
	public EntityStore getStore() {
		return store;
	}
	
	public void print(String s) {
		System.out.println("DB Instance: " + s);
	}
	
	/**
	 * Do synchronize whenever something
	 * is added to or removed from the database
	 */
	public void synchronize() {
		try {
			store.sync();
			myEnv.sync();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		
		if(store!=null) {
			try {
				store.close();
			}
			catch(DatabaseException e) {
				e.printStackTrace();
			}
		}
		if(myEnv!=null) {
			try {
				myEnv.close();
			}
			catch(DatabaseException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void addKeyValue(String boltID, String key, String val) {
		
		PrimaryIndex<String, BoltData> index = store.getPrimaryIndex(String.class, BoltData.class);		
		if (!index.contains(boltID)) {			
			BoltData data = new BoltData(boltID);			
			index.put(data);
		}
		BoltData data = index.get(boltID);
		data.addKeyValue(key, val);
		index.put(data);
	}
	
	public List<String> getValues(String boltID, String key) {
		
		PrimaryIndex<String, BoltData> index = store.getPrimaryIndex(String.class, BoltData.class);
		if (!index.contains(boltID)) return null;
		BoltData data = index.get(boltID);
		return data.getValues(key);
	}
	
	public Map<String, List<String>> getTable(String boltID) {
		
		PrimaryIndex<String, BoltData> index = store.getPrimaryIndex(String.class, BoltData.class);
		if (!index.contains(boltID)) return null;
		BoltData data = index.get(boltID);
		
		return data.table;
	}
	
	public void addNode(Node node) {
		PrimaryIndex<String, Node> index = store.getPrimaryIndex(String.class, Node.class);
		index.put(node);
	}
	
	public Node getNode(String ID) {
		PrimaryIndex<String, Node> index = store.getPrimaryIndex(String.class, Node.class);
		return index.get(ID);
	}
	
	public boolean hasNode(String ID) {
		PrimaryIndex<String, Node> index = store.getPrimaryIndex(String.class, Node.class);
		return index.contains(ID);
	}
	
	public void removeNode(String ID) {
		PrimaryIndex<String, Node> index = store.getPrimaryIndex(String.class, Node.class);
		index.delete(ID);
	}
	
	public void clearTempData() {
		
		PrimaryIndex<String, BoltData> index = store.getPrimaryIndex(String.class, BoltData.class);
		
		EntityCursor<BoltData> cursor = index.entities();				
		List<String> boltIDs = new LinkedList<>();		
		while (true) {
			BoltData bolt = cursor.next();
			if (bolt == null) break;
			boltIDs.add(bolt.getID());
		}		
		cursor.close(); // close cursor otherwise you'll get deadlock		
		for (String boltID: boltIDs) {
			index.delete(boltID);
		}
		System.out.println("DBInstance: Temporary data in database " + envDirectory + " has been cleared.");
	}
	
	public void clearGraphData() {
		
		PrimaryIndex<String, Node> index = store.getPrimaryIndex(String.class, Node.class);		
		EntityCursor<Node> cursor = index.entities();				
		List<String> nodeIDs = new LinkedList<>();		
		while (true) {
			Node node = cursor.next();
			if (node == null) break;
			nodeIDs.add(node.getID());
		}		
		cursor.close(); // close cursor otherwise you'll get deadlock		
		for (String nodeID: nodeIDs) {
			index.delete(nodeID);
		}
		System.out.println("Graph data in database " + envDirectory + " has been cleared.");
		
	}
}
