package edu.upenn.cis455.indexer.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class DownloadUtils implements Serializable{
	private static Logger log = Logger.getLogger(DownloadUtils.class);
	private static AWSCredentials credentials = AWSCredentialReader.getCredential();
	public DownloadUtils() {
		// TODO Auto-generated constructor stub
	}
	
	public static InputStream downloadfileS3(String keyName) throws IOException {
		
		AmazonS3 s3client = new AmazonS3Client(credentials);
		String bucketName = "quantumfilesystem";
		try {
			S3Object object = s3client.getObject(new GetObjectRequest(bucketName, keyName));
			InputStream objectData = object.getObjectContent();
			return objectData;
		} catch (AmazonServiceException ase) {
			log.error("URL: " + keyName);
			log.error("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			log.error("Error Message:    " + ase.getMessage());
			log.error("HTTP Status Code: " + ase.getStatusCode());
			log.error("AWS Error Code:   " + ase.getErrorCode());
			log.error("Error Type:       " + ase.getErrorType());
			log.error("Request ID:       " + ase.getRequestId());
			return null;
		} catch (AmazonClientException ace) {
			log.error("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			log.error("Error Message: " + ace.getMessage());
			return null;
		}
	}
}
