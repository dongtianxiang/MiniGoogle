package edu.upenn.cis455.SearchWorkerServer;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Word {
	@PrimaryKey
	String word;
	Map<String, Double> tf      = new HashMap<>();
	Map<String, Double> weights = new HashMap<>();  // weight with only Indexer
	Map<String, Boolean> title  = new HashMap<>();
	Map<String, String> pos     = new HashMap<>();  
	Map<String, Double> weightWithPG = new HashMap<>();  // weight with PageRank

	Double idf;
	
	public Word(){};
	
	public Word(String word) {
		this.word = word;
	}
	
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public Map<String, Double> getTf() {
		return tf;
	}
	public void setTf(Map<String, Double> tf) {
		this.tf = tf;
	}
	public Double getIdf() {
		return idf;
	}
	public void setIdf(Double idf) {
		this.idf = idf;
	}
	public Map<String, Double> getWeight() {
		return weights;
	}
	public void setWeight(Map<String, Double> weights) {
		this.weights = weights;
	}
	public Map<String, Boolean> getTitle() {
		return this.title;
	}
	public void setTitle(Map<String, Boolean> title) {
		this.title = title;
	}
	public Map<String, String> getPos() {
		return this.pos;
	}
	public void setPos(Map<String, String> pos) {
		this.pos = pos;
	}
	
	
	public void putTF(String url, Double value){
		this.tf.put(url, value);
	}
	
	public Double getTF(String url) {
		return tf.get(url);
	}
	
	public void putWeight(String url, Double value) {
		this.weights.put(url, value);
	}
	
	public Double getWeight(String url) {
		return weights.get(url);
	}
	
	public void putTitle(String url) {
		this.title.put(url, true);
	}
	
	public Map<String, Double> getWeightWithPG() {
		return weightWithPG;
	}

	public void setWeightWithPG(Map<String, Double> weightWithPG) {
		this.weightWithPG = weightWithPG;
	}
}
