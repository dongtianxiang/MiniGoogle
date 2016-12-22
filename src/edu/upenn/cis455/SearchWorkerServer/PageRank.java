package edu.upenn.cis455.SearchWorkerServer;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class PageRank {
	@PrimaryKey
	String url;
	Double pagerank;
	
	public PageRank() {}
	
	public PageRank(String url, Double pagerank) {
		this.url = url;
		this.pagerank = pagerank;
	}
	
	public String getURL() {
		return url;
	}

	public void setURL(String url) {
		this.url = url;
	}

	public Double getPagerank() {
		return pagerank;
	}

	public void setPagerank(Double pagerank) {
		this.pagerank = pagerank;
	}
}
