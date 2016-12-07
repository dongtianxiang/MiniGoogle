package edu.upenn.cis455.crawler.storage;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

import java.io.InputStream;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Entity class used to store Page information into database
 * @author cis555
 *
 */
@Entity
public class Page {
	
	@PrimaryKey
	private String url;
	
	@SecondaryKey(relate=MANY_TO_ONE)
	private String type;
	
	private byte[] content;
	private long crawl_time;
	
	public Page(){}
	
	public Page(String url, byte[] content, String type){
		this.url = url;
		this.content = content;
		this.type = type;
	}
	
	public void setURL(String url){
		this.url = url;
	}
	
	public void setContent(byte[] content){
		this.content = content;
	}
	
	public void setType(String type){
		this.type = type;
	}
	
	public void setCrawlTime(long current_time){
		this.crawl_time = current_time;
	}
	
	public String getURL(){
		return url;
	}
	
	public byte[] getContent(){
		return content;
	}
	
	public String getType(){
		return type;
	}
	
	public long getCrawlTime(){
		return crawl_time;
	}
}
