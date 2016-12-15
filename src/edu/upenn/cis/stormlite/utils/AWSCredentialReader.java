package edu.upenn.cis.stormlite.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

public class AWSCredentialReader {

	// public AWSCredentialReader() {
	// // TODO Auto-generated constructor stub
	// }

	public static AWSCredentials getCredential() throws IOException {
		Properties prop = new Properties();
		String propFileName = "./conf/authConfig.properties";
		InputStream in = new FileInputStream(propFileName);
		prop.load(in);
		return new BasicAWSCredentials(prop.getProperty("AWS_KEY"), prop.getProperty("AWS_SECRET"));
	}

//	public static void main(String[] args) throws Exception {
//		Properties prop = new Properties();
//		String propFileName = "./conf/authConfig.properties";
//		InputStream in = new FileInputStream(propFileName);
//		prop.load(in);
//
//		System.err.println(prop.getProperty("AWS_KEY"));
//		System.err.println(prop.getProperty("AWS_SECRET"));
//		System.err.println(getCredential());
//	}

}
