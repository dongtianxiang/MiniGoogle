package edu.upenn.cis455.crawler.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class VisitedURL {
	@PrimaryKey
	private String url;
	private Long lastVisited;
	
	public VisitedURL() {}
	
	public VisitedURL(String url, Long lastVisited) {
		this.url = url;
		this.lastVisited = lastVisited;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Long getLastVisited() {
		return lastVisited;
	}

	public void setLastVisited(Long lastVisited) {
		this.lastVisited = lastVisited;
	}
	
}
