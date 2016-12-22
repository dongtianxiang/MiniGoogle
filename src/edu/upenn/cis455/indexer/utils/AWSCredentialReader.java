package edu.upenn.cis455.indexer.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

public class AWSCredentialReader {
	public static AWSCredentials credentials = null;
	// public AWSCredentialReader() {
	// // TODO Auto-generated constructor stub
	// }

	public static AWSCredentials getCredential() {
		Properties prop = new Properties();
		
		String propFileName = "./conf/config.properties";
		try {
			BufferedReader in = new BufferedReader(new FileReader(propFileName));
			prop.setProperty("AWS_KEY", in.readLine());
			prop.setProperty("AWS_SECRET", in.readLine());
			in.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new BasicAWSCredentials(prop.getProperty("AWS_KEY"), prop.getProperty("AWS_SECRET"));
	}
	
	public static String getKEY() {
		if(credentials == null) {
			credentials = getCredential();
		}
		return credentials.getAWSAccessKeyId();
	}
	
	public static String getSecret() {
		if(credentials == null) {
			credentials = getCredential();
		}
		return credentials.getAWSSecretKey();
	}

//	public static void main(String[] args) throws Exception {
//		Properties prop = new Properties();
////		String propFileName = "./conf/authConfig.properties";
//		InputStream in = null;
//		try {
////			in = new FileInputStream(propFileName);
//			in = AWSCredentials.class.getClassLoader().getResourceAsStream("spark/authConfig.properties");
//			prop.load(in);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		System.err.println(prop.getProperty("AWS_KEY"));
//		System.err.println(prop.getProperty("AWS_SECRET"));
////		System.err.println(getCredential());
//	}

}
