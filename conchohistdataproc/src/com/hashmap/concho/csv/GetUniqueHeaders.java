package com.hashmap.concho.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class GetUniqueHeaders {
	
	
	private static final Properties mnemonicProperty = DepthLogParsing.readPropertyFile();
	
	public static void main (String args[]) {
		
		
		
	//	String depthHeaders = "/Users/miteshrathore/concho/2nd_assign/headers_10sec_depth.csv";
		
		String sec10timeHeaders = "/Users/miteshrathore/concho/2nd_assign/headers_10sec_time.csv";
		
		String sec1timeHeaders = "/Users/miteshrathore/concho/2nd_assign/headers_1sec_time.csv";
		
		SortedSet<String> mnemonicNameList = new  TreeSet<String>();
		File sourceFile =null;
		FileReader fileReader = null;
		
		BufferedReader   br = null;
		String line = null;
		SortedSet<String> headerNameList = new  TreeSet<String>();
		int count = 0;
		
		List<String> mnemonicNameNotFoundList = new  ArrayList<String>();

		try {
			   sourceFile = new File(sec1timeHeaders);
			   
			 //  System.out.println(sourceFile.getAbsolutePath());
			   
			  fileReader = new FileReader(sourceFile);
			  br = new BufferedReader(fileReader);
			 
			 while ((line = br.readLine()) != null) {
				 StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
				 while(stringTokenizer.hasMoreTokens()) {
					 String tokenValue = stringTokenizer.nextToken();
					 if(tokenValue.contains("("))
						 tokenValue = tokenValue.substring(0,tokenValue.indexOf("(")).trim();
					 headerNameList.add(tokenValue);
				 }
				 count++;
			 }
			 
			 System.out.println(" Total records "+count);
			 
			 System.out.println(" headerNameList "+headerNameList.size());
			 
			  sourceFile = new File(sec10timeHeaders);
			  fileReader = new FileReader(sourceFile);
			    br = new BufferedReader(fileReader);
				 line = null;
		  		 while ((line = br.readLine()) != null) {
					 StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
					 while(stringTokenizer.hasMoreTokens()) {
						 String tokenValue = stringTokenizer.nextToken();
						 if(tokenValue.contains("("))
						 tokenValue = tokenValue.substring(0,tokenValue.indexOf("(")).trim();
						 headerNameList.add(tokenValue);
					 }
					 count++;
				 }
		  		 
		  		StringBuilder headerStringBuilder = new StringBuilder();
				 
				 for(String str:headerNameList) {
					 
					 headerStringBuilder.append("").append(str).append("");//.append(",");
					 
		                String  mnemonicName = (String)mnemonicProperty.get(str.toUpperCase());
		                
		                headerStringBuilder.append("=").append(mnemonicName).append(" ").append("\n");
						 
						 if(mnemonicName != null) {
							 mnemonicNameList.add(mnemonicName);
							// System.out.println(mnemonicName);
						 }else {
							 mnemonicNameNotFoundList.add(str);
							// System.out.println(line);
						 }
				 }
				 
				 
				 
				 System.out.println(" headerStringBuilder "+headerStringBuilder);
				
				 StringBuilder stringBuilder = new StringBuilder();
				 
				 
				  
				 
			 
				
				 for(String stst :mnemonicNameList) {
					 stringBuilder.append("\"").append(stst).append("\"").append(",");
				 }
				
				 
				/* 
				 * 
				 *  
				 Collections.sort(mnemonicNameNotFoundList);
				 for(String str :mnemonicNameNotFoundList) {
					 str = str.toUpperCase();
					 String str1 = str.replaceAll("\\W", "");
						
					 if(str1.length()>8) {
						 str = str +"=" + str1.substring(0,8);
					 }
					 
					 else {
						 str = str +"=" + str1;
					 }
					 
					 System.out.println(str);
					 
				 }*/
				 
				 
				 System.out.println(" stringBuilder "+stringBuilder);
			 
			 System.out.println(" Total records "+count);
			 System.out.println(" headerNameList "+headerNameList.size());
			 System.out.println(" mnemonicNameList "+mnemonicNameList.size());
			 System.out.println(" mnemonicNameNotFoundList "+mnemonicNameNotFoundList.size());
		//	 System.out.println(" mnemonicNameNotFoundList "+mnemonicNameNotFoundList.size());
		}catch(Exception exp) {
			exp.printStackTrace();
		}
	  	
		
		
	}

}
