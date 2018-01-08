/**
 * 
 */
package com.hashmap.concho.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author miteshrathore
 *
 */
public class HDFSUtility {
	
	
	/**
	 * 
	 * @param sourceFileLoc
	 * @param destFileLoc
	 * @throws IOException
	 */
	public static void copyFilesToHDFS(String sourceFileLoc,String hdfsLocation) throws IOException {
		
	    String fileName = sourceFileLoc.substring(sourceFileLoc.lastIndexOf("/")+1, sourceFileLoc.length());
		String destFileLoc =	hdfsLocation + fileName;
        Path sourcePath = new Path(sourceFileLoc);
        Configuration myConf = new Configuration();
        Path destPath = new Path(destFileLoc);
        myConf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        myConf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        myConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        myConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fSystem = FileSystem.get(URI.create(sourceFileLoc),myConf);
        
        try{
        	
        	//Delete files from Source location 
           fSystem.copyFromLocalFile(true,sourcePath,destPath);
        }
        catch(IOException e){
            e.printStackTrace();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{
        	fSystem.close();
        }
    }
	
	

}
