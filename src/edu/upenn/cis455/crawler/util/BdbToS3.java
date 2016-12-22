package edu.upenn.cis455.crawler.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import edu.upenn.cis455.crawler.PageDownloader;
import edu.upenn.cis455.crawler.storage.DBWrapper;

public class BdbToS3 {
	
	static Logger log = Logger.getLogger(PageDownloader.class);
	
	public static void uploadfileS3(AWSCredentials credentials, byte[] content, String bucketName, String S3fileName) {
		AmazonS3 s3client = new AmazonS3Client(credentials);
		
		try {
	         ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(content);
	         ObjectMetadata md = new ObjectMetadata();
	         md.setContentLength(content.length);
	         
	         String keyName = S3fileName;
	         s3client.putObject(new PutObjectRequest(bucketName, keyName, contentsAsStream, md));         
         } catch (AmazonServiceException ase) {
            log.error("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            log.error("Error Message:    " + ase.getMessage());
            log.error("HTTP Status Code: " + ase.getStatusCode());
            log.error("AWS Error Code:   " + ase.getErrorCode());
            log.error("Error Type:       " + ase.getErrorType());
            log.error("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            log.error("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            log.error("Error Message: " + ace.getMessage());
        }
	}
	
	public void generateLocalFile(String dbpath, String outputName) throws FileNotFoundException, UnsupportedEncodingException{
		DBWrapper db = DBWrapper.getInstance(dbpath);
		List<String> links = db.outLinksList();
		PrintWriter writer = new PrintWriter(outputName, "UTF-8");
		for(String url : links) {
			writer.println(url);
		}
		writer.close();
	}
	
	public byte[] generateMemoryStringPageRank(String dbpath){
		DBWrapper db = DBWrapper.getInstance(dbpath);
		List<String> links = db.outLinksList();
		StringBuilder sb = new StringBuilder();
		for(String url : links) {
			sb.append(url);
			sb.append(" -> ");
			List<String> outlinks = db.getOutLinksList(url);
			for(int i = 0; i < outlinks.size(); i++) {
				sb.append(outlinks.get(i) + " ");
			}
			sb.append("\n");
		}
		return sb.toString().getBytes();
	}
	
	public byte[] generateMemoryStringIndexer(String dbpath){
		DBWrapper db = DBWrapper.getInstance(dbpath);
		List<String> links = db.outLinksList();
		StringBuilder sb = new StringBuilder();
		for(String url : links) {
			String keyName = DigestUtils.sha1Hex(url);
			sb.append(keyName);
			sb.append("\n");
		}
		return sb.toString().getBytes();
	}
	
	public void generateLocalText(String dbpath, String fileName){
		DBWrapper db = DBWrapper.getInstance(dbpath);
		
		BufferedWriter bw = null;
		FileWriter fw = null;
		try {
			File file = new File(fileName);
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			
			// true = append file
			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);
			
			List<String> links = db.outLinksList();
			
			for(String url : links) {
				StringBuilder sb = new StringBuilder();
				String hashing = DigestUtils.sha1Hex(url); 
				sb.append(hashing + " ");
				sb.append(url);
				sb.append("\n");
				bw.write(sb.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				db.close();
				
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
    	Properties props = new Properties();
    	props.load(new FileInputStream("./resources/log4j.properties"));
    	PropertyConfigurator.configure(props);
		
    	/* Setting KEY for AWS S3 */
		FileReader f = null;
		try {
			f = new FileReader(new File("./conf/config.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		BufferedReader bf = new BufferedReader(f);
		System.setProperty("KEY", bf.readLine());
		System.setProperty("ID", bf.readLine());
		
		BdbToS3 util = new BdbToS3();
		util.generateLocalText("/Users/dongtianxiang/git/CrawlerDB/dtianx0", "HashingTransfer.txt");
		System.out.println("dtianx0 completed");
		util.generateLocalText("/Users/dongtianxiang/git/CrawlerDB/dtianx1", "HashingTransfer.txt");
		System.out.println("dtianx1 completed");
		util.generateLocalText("/Users/dongtianxiang/git/CrawlerDB/dtianx2", "HashingTransfer.txt");
		System.out.println("dtianx2 completed");
		util.generateLocalText("/Users/dongtianxiang/git/CrawlerDB/dtianx3", "HashingTransfer.txt");
		System.out.println("dtianx3 completed");
		util.generateLocalText("/Users/dongtianxiang/git/CrawlerDB/dtianx4", "HashingTransfer.txt");
		System.out.println("dtianx4 completed");

	}
}
