package edu.upenn.cis455.crawler;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.util.Calendar;

import org.apache.commons.codec.digest.DigestUtils;
import org.jsoup.nodes.Document;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.log4j.Logger;
import com.amazonaws.services.s3.model.ObjectMetadata;

import edu.upenn.cis455.crawler.info.Client;
import edu.upenn.cis455.crawler.storage.DBWrapper;

/**
 * Download the webpage and store to the database
 * @author dongtianxiang
 *
 */
public class PageDownloader {
	public static DBWrapper db;
	static Logger log = Logger.getLogger(PageDownloader.class);
	
	public static void setup(DBWrapper instance){
		db = instance;
	}
	
//	public static void download(String url){
//		long c0 = Calendar.getInstance().getTime().getTime();
//		Client client = new Client(url);
//		InputStream inputStream = client.executeGET(true);
//		ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
//		int next = -1;
//		try{
//			while((next = inputStream.read())!=-1){
//				byteOutput.write(next);
//			}
//			byteOutput.flush();
//			byte[] content = byteOutput.toByteArray();
//			db.putPage(url, content, client.getContentType());	
//		}
//		catch(IOException e){
//			e.printStackTrace();
//		}
//		RobotCache.setCurrentTime(url);
//		db.sync();
//	}
	
	public static void download(String url, Document doc, String type){	
		String body = doc.toString();
		byte[] content = body.getBytes();
		RobotCache.setCurrentTime(url);
		
		// Local BDB has been replaced by Amazon S3
		//db.putPage(url, content, type);	 
		//db.sync();
		
		AWSCredentials credentials = new BasicAWSCredentials(
				System.getProperty("KEY"),  
				System.getProperty("ID"));   
				
		uploadfileS3(credentials, url, content);
		
	}
	
	public static void uploadfileS3(AWSCredentials credentials, String url, byte[] content) {
		AmazonS3 s3client = new AmazonS3Client(credentials);
		String bucketName   = "quantumfilesystem";
		
		try {
	         ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(content);
	         ObjectMetadata md = new ObjectMetadata();
	         md.setContentLength(content.length);
	         
	         String keyName = DigestUtils.sha1Hex(url); 
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
}
