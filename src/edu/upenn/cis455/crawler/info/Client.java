package edu.upenn.cis455.crawler.info;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;

import java.net.URL;
import java.net.UnknownHostException;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

//import edu.upenn.cis455.crawler.bolts.*;

/**
 * Helper Class to get the information of the given link. Including execute HEAD request for basic info,
 * as well as execute GET method for html body.
 * @author cis555
 *
 */
public class Client {
	private String url;
	private String hostName;
	private String path;
	private int portNumber;
	private long contentLength;
	private String contentType = "text/html";
	private long last_modified;
	static Logger log = Logger.getLogger(Client.class);
	
	public static final int HTTP_TIMEOUT = 60000;
	public static final int READ_TIMEOUT = 60000;
	
	public Client(String url){
		this.url = url;
		URLInfo urlinfo = new URLInfo(url);
		this.hostName = urlinfo.getHostName();
		this.path = urlinfo.getFilePath();
		this.portNumber = urlinfo.getPortNo();
		executeGET(false);
	}

	/**
	 * This is the method excuting GET or HEAD. Basically HEAD is called in the Client's Constructor.
	 * @param isGET true means excuting GET request; false means excuting HEAD request.
	 * @return InputStream returned by server for the respond's body ONLY.
	 */
	public InputStream executeGET(boolean isGET){	
		
//		String method = isGET ? "GET" : "HEAD";	
		if(url.startsWith("https")) {
			HttpsURLConnection c = null;
			
			try{
				URL httpsURL = new URL(url);
				if(httpsURL.getHost() == null) return null;
				
				c = (HttpsURLConnection)httpsURL.openConnection();
				c.setRequestProperty("User-Agent", "cis455crawler");
				c.setRequestProperty("Connection", "close");
				if(!isGET) c.setRequestMethod("HEAD");
				c.setConnectTimeout(HTTP_TIMEOUT);
				c.setReadTimeout(READ_TIMEOUT);
				c.connect();
				contentLength = c.getContentLength();
				contentType = c.getContentType();
				last_modified = c.getLastModified();
				if(isGET) return c.getInputStream();
				else return null;
			} catch(MalformedURLException e){
				log.error(url);
				log.error(ExceptionUtils.getStackTrace(e));
			} catch(IOException e){
				log.error(url);
				log.error(ExceptionUtils.getStackTrace(e));
			} finally {
				if (null != c){
					try {
						if(isGET) c.getInputStream().close();
						c.disconnect();
					} catch (IOException e) {
						StringWriter sw = new StringWriter();
						PrintWriter pw = new PrintWriter(sw);
						e.printStackTrace(pw);
						log.error(sw.toString()); // stack trace as a string
					}
				}
			}
			
		} else if(url.startsWith("http")){
			HttpURLConnection c = null;
			
			try{
				URL httpURL = new URL(url);
				if(httpURL.getHost() == null) return null;
				
				c = (HttpURLConnection)httpURL.openConnection();
				c.setRequestProperty("User-Agent", "cis455crawler");
				c.setRequestProperty("Connection", "close");
				if(!isGET) c.setRequestMethod("HEAD");
				c.setConnectTimeout(HTTP_TIMEOUT);
				c.setReadTimeout(READ_TIMEOUT);
				
				c.connect();
				contentLength = c.getContentLength();
				contentType = c.getContentType();
				last_modified = c.getLastModified();
				if(isGET) return c.getInputStream();
				else return null;
			} catch (UnknownHostException e) {
				log.error(url);
				log.error(ExceptionUtils.getStackTrace(e));
			} catch (IOException e) {
				log.error(url);
				log.error(ExceptionUtils.getStackTrace(e));
			} finally {
				if (null != c){
					try {
						if(isGET) c.getInputStream().close();
						c.disconnect();
					} catch (IOException e) {
						StringWriter sw = new StringWriter();
						PrintWriter pw = new PrintWriter(sw);
						e.printStackTrace(pw);
						log.error(sw.toString()); // stack trace as a string
					}
				}
			}
		} 
		
		return null;
	}
	
//	public boolean processInitialLine(String s){
//		Pattern p = Pattern.compile("HTTP/1.1 (\\d{3}) .*");
//		Matcher m = p.matcher(s);
//		if(m.find()){
//			int status_code = Integer.parseInt(m.group(1));
//			if(status_code < 400) return true;
//		}
//		return false;
//	}
	
	/**
	 * To check if the given url is valid in this maxSize.
	 * @param maxSize
	 * @return
	 */
	public boolean isValid(int maxSize){
		return isValidType() && isValidLength(maxSize);
	}
	
	public boolean isValidType(){
		if(contentType == null) return false;
		if(contentType.startsWith("text/html")) return true;
		if(contentType.startsWith("application/xml")) return true;
		if(contentType.startsWith("text/xml")) return true;
		if(contentType.endsWith("+xml")) return true;
		return false;
	}
	
	public boolean isValidLength(int maxSize){
		if(contentLength > maxSize*1024*1024) return false;
		return true;
	}
	
	public long getLast_modified() {
		return last_modified;
	}
	public void setLast_modified(long last_modified) {
		this.last_modified = last_modified;
	}
	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	public long getContentLength() {
		return contentLength;
	}
	public void setContentLength(int contentLength) {
		this.contentLength = contentLength;
	}
	public int getPortNumber() {
		return portNumber;
	}
	public void setPortNumber(int portNumber) {
		this.portNumber = portNumber;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	
	public static void main(String[] args) throws IOException {
		String url = "http://crawltest.cis.upenn.edu/marie/tpc/";
		Client client = new Client(url);
		System.out.println("URLInfo:" + client.getPath());
		InputStream inputStream = client.executeGET(true);
		if(inputStream != null) {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			int next;
			try{
				while((next = inputStream.read())!=-1){
					bos.write(next);
				}
				bos.flush();
				byte[] content = bos.toByteArray();
				System.out.println(new String(content, "UTF-8"));
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println(client.getContentType());
		System.out.println(client.getContentLength());
		System.out.println(client.getLast_modified());
	}
}
