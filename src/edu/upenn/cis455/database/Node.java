package edu.upenn.cis455.database;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Node {
	
	@PrimaryKey
	String id;
	
	private List<String> neighbors;
	private boolean converge;
	private double  rank;
	
	public Node() {}
	
	public Node(String id) {
		this.id = id;
		neighbors = new LinkedList<>();
		converge = false;
		rank = 100;
	}
	
	public void resetRank() {
		rank = 1;
	}
	
	public void setRank(double newRank) {
		rank = newRank;
	}
	
	public double getRank() {
		return rank;
	}
	
	public boolean convergenceTest() {
		// TODO
		return converge; 
	}
	
	public Iterator<String> getNeighborsIterator() {
		return neighbors.iterator();
	}
	
	public int getNumberNeighbors() {
		return neighbors.size();
	}
	
	public void addNeighbor(String id) {
		if (neighbors.contains(id)) {
			throw new IllegalStateException();
		}
		neighbors.add(id);
	}
	
	public void deleteNeighbor(String id) throws IllegalStateException{
		if (neighbors.contains(id)) {
			neighbors.remove(id);
		} 
		else throw new IllegalStateException();		
	}
	
	public String getID() {
		return id;
	}
	
	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return null;
		}
	}
}
