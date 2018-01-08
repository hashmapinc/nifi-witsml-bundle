/**
 * 
 */
package com.hashmap.concho.csv;

import java.io.File;

/**
 * @author miteshrathore
 *
 */
public class TrajectoryStationProcessing {
 	
	
	/**
	 * 
	 * @param srcFolderPath
	 * @param destFolderPath
	 */
	private static void processTrajectoryStationData(String srcFolderPath) {
	     try {
	        	File folder = new File(srcFolderPath);
	        	int fileCount = 0;
	    	    for (final File fileEntry : folder.listFiles()) {
	    	            if (fileEntry.isDirectory()) {
	    	            	System.out.println(" It is a directory -->> "+fileEntry.getName());
	    	            } else {
	    	            	 System.out.println("-- Unzipping file -- file name--  "+fileEntry.getName()  +  " --- file number -- "+fileCount);
	    	            	 TrajectoryStationParsing.parseTrajectoryStationData(fileEntry.getAbsolutePath());    	            	 
	    	                fileCount++;
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
		if(args.length>0) {
			
			Long startTime = System.currentTimeMillis();
			//unzip all the files from a folder:
			System.out.println("Source  Directory for Trajectory Station ==>> : "+args[0] +" Start time "+startTime);
			processTrajectoryStationData(args[0]);
			
			Long endTime = System.currentTimeMillis();
			
			System.out.println("Total time taken in Milliseconds for Trajectory Station file ==>>> "+(endTime-startTime));
			
		}else {
			System.out.println("Please provide source  path for Trajectory Station ");
		}
    }

	
	
	 
}
	
	
	
