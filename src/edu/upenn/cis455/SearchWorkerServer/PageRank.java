package edu.upenn.cis455.SearchWorkerServer;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class PageRank {
	@PrimaryKey
	String word;
	Double pagerank;
	
	public PageRank() {}
	
	public PageRank(String url, Double pagerank) {
		this.word = url;
		this.pagerank = pagerank;
	}
	
	public String getURL() {
		return word;
	}

	public void setURL(String url) {
		this.word = url;
	}

	public Double getPagerank() {
		return pagerank;
	}

	public void setPagerank(Double pagerank) {
		this.pagerank = pagerank;
	}
}
