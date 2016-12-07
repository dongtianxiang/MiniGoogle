package edu.upenn.cis455.crawler.storage;

import java.util.List;
import java.util.ArrayList;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Entity class used to store All Out Links from a page
 * @author cis555
 *
 */

@Entity
public class OutLinks {
	
	@PrimaryKey
	private String url;
	private List<String> links = new ArrayList<>();
	
	public OutLinks(){}
	
	public OutLinks(String url){
		this.url = url;
	}
	
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public List<String> getLinks() {
		return links;
	}

	public void setLinks(List<String> links) {
		this.links = links;
	}
	
	public void addLinks(String link) {
		this.links.add(link);
	}
	
}
