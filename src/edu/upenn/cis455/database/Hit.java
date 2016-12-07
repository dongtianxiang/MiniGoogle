package edu.upenn.cis455.database;

import java.io.*;
import java.util.*;

public class Hit {
	
	//	private int wordID;
	
	// primary key of a hit
	private String text;
	private long docID;
	private int frequency;
	private boolean isTitle, isHead;
	private PriorityQueue<Position> position;
	
	public Hit(String text){
		this.text = text;
		isTitle = false; 
		isHead = false;
		position = new PriorityQueue<>(11, new Comparator<Position>(){
			@Override
			public int compare(Position o1, Position o2) {
				return o2.weight - o1.weight;
			}
		});
		frequency = 1;
	}
	
	public void increaseFrequency(){
		frequency++;
	}
	
	public int getFrequency(){
		return frequency;
	}

	public long getDocID() {
		return docID;
	}

	public void setDocID(long docID) {
		this.docID = docID;
	}

	public boolean isTitle() {
		return isTitle;
	}

	public void setTitle(boolean isTitle) {
		this.isTitle = isTitle;
	}

	public boolean isHead() {
		return isHead;
	}

	public void setHead(boolean isHead) {
		this.isHead = isHead;
	}
	
	public void addPosition(Position pos) {
		position.add(pos);
	}

	public PriorityQueue<Position> getPosition() {
		return position;
	}

	public String getText() {
		return text;
	}
	
	public String toString(){
		return text + ": freq-" + frequency + " title-" + isTitle + " head-" + isHead + position.toString();
	}
}
