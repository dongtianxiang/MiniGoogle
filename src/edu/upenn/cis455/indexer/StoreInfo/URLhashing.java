package edu.upenn.cis455.indexer.StoreInfo;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class URLhashing {
	@PrimaryKey
	private String hashing;
	private String url;
	
	public URLhashing() {}
	
	public URLhashing(String hashing, String url) {
		this.hashing = hashing;
		this.url     = url;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getHashing() {
		return hashing;
	}

	public void setHashing(String hashing) {
		this.hashing = hashing;
	}
}
