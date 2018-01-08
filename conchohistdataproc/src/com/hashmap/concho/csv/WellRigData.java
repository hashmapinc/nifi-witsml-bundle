/**
 * 
 */
package com.hashmap.concho.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author miteshrathore
 *
 */
public class WellRigData {
	
	
	public static StringBuilder wellBuilder = new StringBuilder();
	public static StringBuilder rigBuilder = new StringBuilder();
	
	public static void createWellCSV(String wellid, String wellName, String operator){
		wellBuilder.append(wellid+",");
		wellBuilder.append(wellName+",");
		wellBuilder.append(operator+",");
		wellBuilder.append('\n');
		
		}
	
	public static void createRigCSV(String wellid, String rigId, String rigName){
		rigBuilder.append(wellid+",");
		rigBuilder.append(rigId+",");
		rigBuilder.append(rigName+",");
		rigBuilder.append('\n');
		}
	public static void main(String[] args) {
		PrintWriter wellpw = null;
		PrintWriter rigpw = null;
		
		if(args.length>2) {
			String srcFileLoc = args[0];
			String wellFileLoc = args[1];
			String rigFileLoc = args[2];
			
			try {
			//	wellpw = new PrintWriter(new File("/Users/miteshrathore/concho/2nd_assign/Data_Concho/converted/concho_wells.csv"));
			//	rigpw = new PrintWriter(new File("/Users/miteshrathore/concho/2nd_assign/Data_Concho/converted/concho_rigs.csv"));
				
				wellpw = new PrintWriter(new File(wellFileLoc));
				rigpw = new PrintWriter(new File(rigFileLoc));
				
			} catch (FileNotFoundException e) {
			    e.printStackTrace();
			}
			
	      //  String srcFileLoc = "/Users/miteshrathore/concho/2nd_assign/Data_Concho/concho_mapping_csv.csv";
	        BufferedReader br = null;
	        String line = "";
	        String cvsSplitBy = ",";

	        try {
	        	
	        	   int i =0;

	            br = new BufferedReader(new FileReader(srcFileLoc));
	            while ((line = br.readLine()) != null) {

	                // use comma as separator
	                String[] wellAndRigInfo = line.split(cvsSplitBy);
	                
	                String wellId = wellAndRigInfo[0];
	                String rigName = wellAndRigInfo[1];
	                String rigId = wellAndRigInfo[2];
	                String wellName = wellAndRigInfo[3];
	                String operator = wellAndRigInfo[4];
	                
	                if(i==0) {
	                	wellId = "Well Id";
	                }
	                createWellCSV(wellId,wellName,operator);
	                createRigCSV(wellId,rigId,rigName);
	                
	               // if(i==3)break;
	                i++;
	            }
	            
	            wellpw.write(wellBuilder.toString());
	            wellpw.close();
	            
	            rigpw.write(rigBuilder.toString());
	            rigpw.close();
	            
	    		   System.out.println("well and rig processing done!");

	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (br != null) {
	                try {
	                    br.close();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
			
		}else {
			System.out.println(" Please provide Source file location and Well and Rigs files location to process ");
		}
		

    }

}
