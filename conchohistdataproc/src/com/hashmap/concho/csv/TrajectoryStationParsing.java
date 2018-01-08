/**
 * 
 */
package com.hashmap.concho.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import com.hashmap.concho.util.HDFSUtility;

/**
 * @author miteshrathore
 *
 */
public class TrajectoryStationParsing {
	
	private static final Properties mnemonicProperty = readPropertyFile();
	
	 private static final String [] TRAJECTORY_HEADER = {
			 "WELLID","TDATE","ID", "NAME","MD","INCL","AZIMUTH","STATUS"
		};
	 
	 public static void parseTrajectoryStationData(String srcFilePath) {
	  
				 PrintWriter depthLogPW = null;
				String parseFile = null;
				String fileName = null;
				File sourceFile = null;
				String hdfsDepthLoc = "/data_lake/pason/trajstation/";
				File destinationFile = null;
				try {
				    sourceFile = new File(srcFilePath);
					String parentFolder = sourceFile.getParent();
				    fileName = sourceFile.getName();
				    destinationFile = new File(parentFolder + "/..");
				    parseFile =  destinationFile.getCanonicalPath() +"/parsedfiles/"+ fileName;
				    destinationFile = new File(parseFile);
					depthLogPW = new PrintWriter(parseFile);
				} catch (FileNotFoundException e) {
				    e.printStackTrace();
				}catch (Exception e) {
				    e.printStackTrace();
				}
			    BufferedReader br = null;
		        String line = "";
		        String cvsSplitBy = ",";
		        try {        	
		        	  FileReader fileReader = new FileReader(sourceFile);
		           br = new BufferedReader(fileReader);
		           String wellname = fileName.substring(0, fileName.indexOf("_")).trim();
		            StringBuffer headInfo = new StringBuffer();
		            
		            Map<String, Integer> mnemonicMap = new HashMap<String, Integer>();
		            int index =0;
		         	 for(String mnemonic :TRAJECTORY_HEADER) {
						 headInfo.append(mnemonic).append(",");
						 mnemonicMap.put(mnemonic, index);
						 index++;
					 }
					 
					List<Integer> indexList = new ArrayList<Integer>();
					 
					int lineNumber = 0;
					List<CVSColumnPostion> columnPostionList = null;
					columnPostionList = new ArrayList<CVSColumnPostion>();
					
		            while ((line = br.readLine()) != null) {
		            	StringBuilder depthLogBuilder = new StringBuilder();
		            	    // use comma as separator
		                String[] depthLogInfo = line.split(cvsSplitBy);
		               if(lineNumber ==0) {
		            	   StringTokenizer stringTokenizer = new StringTokenizer(line, cvsSplitBy);
		            	   CVSColumnPostion columnPostion = null;
		            	   int counter = 0;
		                      while(stringTokenizer.hasMoreTokens()) {
		                    	  columnPostion = new CVSColumnPostion();
		                      	String tokenValue = stringTokenizer.nextToken();
		                      	
		                      if(tokenValue.contains("("))
		                      	tokenValue = tokenValue.substring(0,tokenValue.indexOf("(")).trim();
		                      
		                      tokenValue = tokenValue.toUpperCase();
		                      String mnemonicMapping = mnemonicProperty.getProperty(tokenValue);
		                       
		                      Integer mnemonicIndex = 0;
		                       
		                      if(mnemonicMapping!=null) {
		                    	   mnemonicIndex = mnemonicMap.get(mnemonicMapping.toUpperCase());
		                    	   
		                      }else {
		                    	   System.out.println(" <<<----- No Mnemomnic mapping found for CSV header value ---->>> "+tokenValue);
		                      }
		                     
		                      if(mnemonicIndex!=null) {
		                    	   columnPostion.setDestinationIndex(mnemonicIndex);
		                    	   indexList.add(mnemonicIndex);
		                      }
		                      
		                      columnPostion.setSourceName(tokenValue);
		                      columnPostion.setDestinationName(mnemonicMapping);
		                      columnPostion.setSourceIndex(counter);
		                    
		                      counter++;
		                      columnPostionList.add(columnPostion);
		                    		                      
		                      }
		                      
		                      
		                      
		                      Collections.sort(columnPostionList, new CVSColumnPostion());
		                      
		                     depthLogBuilder.append(headInfo);
		                     depthLogBuilder.append('\n');
			        		     depthLogPW.write(depthLogBuilder.toString());
			           
		               }
		                
		               else {
		            	   
		            	   int arrLength = columnPostionList.get(columnPostionList.size()-1).getDestinationIndex();
		            	   String arr[] = new String[arrLength];
		            	   if(depthLogInfo.length>1)
		            	   depthLogBuilder.append(wellname+",");
		            	  
		            	    
		            	   for(CVSColumnPostion columnPostion:columnPostionList) {
		            		   int srcIdx = columnPostion.getSourceIndex();
		            		   int destIdx = columnPostion.getDestinationIndex();
		            		   try {
		            		 	  if(destIdx != 0 && srcIdx<depthLogInfo.length) {
		            		 		  	   arr[destIdx-1] = depthLogInfo[srcIdx];
		              			    
		            		 	  } 
		            		   }catch(Exception exp) {
		            			   exp.printStackTrace();
		            		   }
		            	   }
		            	   
		            	   
		            	   for(String str : arr) {
		            		   
		            		  if(str!=null&& str!="") {
		            			   depthLogBuilder.append(str).append(",");
		            		   }else {
		            			   depthLogBuilder.append(",");
		            		   }
		            		}
		            	   
		            	   
		            	    depthLogBuilder.append('\n');
		       			depthLogPW.write(depthLogBuilder.toString());
		       			
		       	
		               }
		            
		                lineNumber++;
		            }
		            System.out.println("total line number processed from a file "+lineNumber);
		            depthLogPW.close();
		            
		        //    ConvertUtils.convertCsvToParquet(destinationFile, parquetFile);
		            
		            
		            //copy CSV file to HDFS
		            HDFSUtility.copyFilesToHDFS(parseFile,hdfsDepthLoc);
		        
		            System.out.println(" Processing done!");
		    		   
		    		   

		        } catch (FileNotFoundException e) {
		            e.printStackTrace();
		        } catch (IOException e) {
		            e.printStackTrace();
		        }catch (Exception e) {
		            e.printStackTrace();
		        } finally {
		            if (br != null) {
		                try {
		                    br.close();
		                    //Delete Source file
		                    sourceFile.delete();
		                } catch (IOException e) {
		                    e.printStackTrace();
		                }
		            }
		        }
		    }
	 
	 //Reading Mnemonic mapping file from property file
		public static Properties readPropertyFile() {
			Properties mnemonicProp = new Properties();
			InputStream input = null;
			try {
				input = DepthLogParsing.class.getClassLoader().getResourceAsStream("tstation.properties");
				// load a properties file
				mnemonicProp.load(input);
			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				if (input != null) {
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
			return mnemonicProp;

		  }

}
