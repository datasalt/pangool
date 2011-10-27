package com.datasalt.pangolin.commons;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.log4j.Logger;

public class SSHUtils {

	private static Logger log = Logger.getLogger(SSHUtils.class);
	
	public static Process executeShellCommand(String command) throws IOException, InterruptedException{
		Runtime runtime = Runtime.getRuntime();
		Process process =runtime.exec(command);
		process.waitFor();
		if (process.exitValue() != 0){
			String error = convertStreamToString(process.getErrorStream());
			String output = convertStreamToString(process.getInputStream());
			throw new RuntimeException(output+ "\n" + error); //TODO change this for a proper named exception
		}
		
		return process;
	}
	
	public static void executeSSHCommand(String remoteUser, String remoteHost, String order) throws IOException,
	    InterruptedException {
		String command = "ssh " + remoteUser + "@" + remoteHost + " " + order;
		log.info("Executing " + command);
		executeShellCommand(command);
	}
	
	
	/**
	 * Executes 
	 * @param remoteUser
	 * @param remoteHost
	 * @param localFileOrFolder
	 * @param remoteFolder
	 * @param recursive
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void executeSCPTo(String remoteUser,String remoteHost,String localFileOrFolder,String remoteFolder,boolean recursive) throws IOException, InterruptedException{
		String command = "scp ";
		if (recursive){
			command += " -r ";
		}
		command += localFileOrFolder + " " + remoteUser +"@"+remoteHost + ":" + remoteFolder;
		log.info("Executing " + command);
		long start = System.currentTimeMillis();
		executeShellCommand(command);
		long end = System.currentTimeMillis();
		log.info("SCP finished in " + (end-start)/1000.0 + " seconds");
		
	}
	
	
	/**
	 * Reads an InputStream and dumps it to a stream
	 * @param is
	 * @return
	 * @throws IOException
	 */
	public static String convertStreamToString(InputStream is)throws IOException {
	        /*
	         * To convert the InputStream to String we use the
	         * Reader.read(char[] buffer) method. We iterate until the
	         * Reader return -1 which means there's no more data to
	         * read. We use the StringWriter class to produce the string.
	         */
	        if (is != null) {
	            Writer writer = new StringWriter();
	 
	            char[] buffer = new char[1024];
	            try {
	                Reader reader = new InputStreamReader(is, "UTF-8");
	                int n;
	                while ((n = reader.read(buffer)) != -1) {
	                    writer.write(buffer, 0, n);
	                }
	            } finally {
	                is.close();
	            }
	            return writer.toString();
	        } else {       
	            return "";
	        }
	    }
}
