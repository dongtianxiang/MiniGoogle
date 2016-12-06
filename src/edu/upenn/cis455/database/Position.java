package edu.upenn.cis455.database;

public class Position {
	
	public int pos, weight;
	
	public Position(int pos, int weight){
		this.pos = pos;
		this.weight = weight;
	}

	public void setPos(int pos) {
		this.pos = pos;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}
	
	public String toString(){
		return pos + ":" + weight + " ||| ";
	}
	
}
