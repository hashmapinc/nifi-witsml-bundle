/**
 * 
 */
package com.hashmap.concho.csv;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author miteshrathore
 *
 */
public class DepthLogDataProcessing {
	private static final int BUFFER_SIZE = 6096;
	
	
	/**
	 * 
	 * @param srcFolderPath
	 * @param destFolderPath
	 */
	private static void unzipfiles(String srcFolderPath, String destFolderPath) {
		    DepthLogDataProcessing unzipper = new DepthLogDataProcessing();
	        try {
	        	File folder = new File(srcFolderPath);
	        	
	       int fileCount = 0;
	    	    for (File fileEntry : folder.listFiles()) {
	    	            if (fileEntry.isDirectory()) {
	    	            	System.out.println(" It is a directory -->> "+fileEntry.getName());
	    	            } else {
	    	           	 System.out.println("-- Unzipping file -- file name--  "+fileEntry.getName()  +  " --- file number -- "+fileCount);
	    	            	unzipper.unzip(fileEntry.getAbsolutePath(), destFolderPath,fileCount);
	    	            	
	    	            	fileCount++;
	    	         /*   	
	    	               ExecutorService executorService = Executors.newFixedThreadPool(5);
	    	               executorService.submit(new Runnable() {
	    	            	    public void run() {
	    	            	        System.out.println("Asynchronous task -->> " +fileEntry.getAbsolutePath());
	    	            	        try {
										unzipper.unzip(fileEntry.getAbsolutePath(), destFolderPath);
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
	    	            	    }
	    	            	});*/

	    	               
	    	            //   String fileName = destFolderPath + fileEntry.getName();
	    	            }
	    	    }
	        } catch (Exception ex) {
	            // some errors occurred
	            ex.printStackTrace();
	        }
	}
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		 //  String destDirectory = "/Users/miteshrathore/concho/2nd_assign/Data_Concho/concho_10s_depth/unziptest/unzip/";
	     //  String srcFilePath = "/Users/miteshrathore/concho/2nd_assign/Data_Concho/concho_10s_depth/unziptest/";
		if(args.length>1) {
		 	Long startTime = System.currentTimeMillis();
			//unzip all the files from a folder:
			System.out.println("Source DepthLog Zip Directory==>> : "+args[0] +" Destination DepthLog Unzip directory==>> "+args[1]  +" Start time "+startTime);
			unzipfiles(args[0],args[1]);
			
			Long endTime = System.currentTimeMillis();
			
			System.out.println("Total time taken in Milliseconds DepthLog Files ==>>> "+(endTime-startTime));
			
		}else {
			System.out.println("  === Please provide source and destination path for DepthLogs === ");
		}
    }

	/**
	 * 
	 * @param zipFilePath
	 * @param destDirectory
	 * @throws IOException
	 */
	public void unzip(String zipFilePath, String destDirectory,int fileCount) throws IOException {
		File destDir = new File(destDirectory);
		if (!destDir.exists()) {
			destDir.mkdir();
		}
		ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
		ZipEntry entry = zipIn.getNextEntry();
		// iterates over entries in the zip file
		while (entry != null) {
			String fileName =  entry.getName();
		
			//Remove spaces from the file name.
			fileName = fileName.replaceAll("\\s", "");
			 
			String filePath = destDirectory + File.separator + fileName;
			if (!entry.isDirectory()) {
				// if the entry is a file, extracts it
				extractFile(zipIn, filePath,fileCount);
				
			} else {
				// if the entry is a directory, make the directory
				File dir = new File(filePath);
				dir.mkdir();
			}
			zipIn.closeEntry();
			entry = zipIn.getNextEntry();
		}
		zipIn.close();
	}
	
	/**
	 * 
	 * @param zipIn
	 * @param filePath
	 * @throws IOException
	 */
	private void extractFile(ZipInputStream zipIn, String filePath,int fileCount) throws IOException {
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
		byte[] bytesIn = new byte[BUFFER_SIZE];
		int read = 0;
		while ((read = zipIn.read(bytesIn)) != -1) {
			bos.write(bytesIn, 0, read);
		}
		bos.close();
		DepthLogParsing.parseDepthLogData(filePath,fileCount);
	}
}
	
	
	
