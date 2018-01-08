/**
 * 
 */
package com.hashmap.concho.util;

/**
 * @author miteshrathore
 *
 */
public class ConvertUtils {/*

import parquet.example.data.Group;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

	
	//private static final Log LOG = Log.getLog(ConvertUtils.class);

	  public static final String CSV_DELIMITER= "|";

	  private static String readFile(String path) throws IOException {
		  
		System.out.println("<<<<<------ path-------->>> "+path);  
	    BufferedReader reader = new BufferedReader(new FileReader(path));
	    StringBuilder stringBuilder = new StringBuilder();

	    try {
	      String line = null;
	      String ls = System.getProperty("line.separator");

	      while ((line = reader.readLine()) != null ) {
	        stringBuilder.append(line);
	        stringBuilder.append(ls);
	      }
	    } finally {
	     	reader.close();
	    }

	    return stringBuilder.toString();
	  }

	  public static String getSchema(File csvFile) throws IOException {
	    String fileName = csvFile.getName().substring(
	        0, csvFile.getName().length() - ".csv".length()) + ".schema"
	    		;
	    File schemaFile = new File(csvFile.getParentFile(), fileName);
	    return readFile(schemaFile.getAbsolutePath());
	  }
	  
	  public static void convertCsvToParquet(File csvFile, File outputParquetFile) throws IOException {
	    convertCsvToParquet(csvFile, outputParquetFile, false);
	  }

	  public static void convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary) throws IOException {
		System.out.println("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
	    String rawSchema = getSchema(csvFile);
	    if(outputParquetFile.exists()) {
	      throw new IOException("Output file " + outputParquetFile.getAbsolutePath() + 
	          " already exists");
	    }

	    Path path = new Path(outputParquetFile.toURI());

	    MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
	    CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);

	    BufferedReader br = new BufferedReader(new FileReader(csvFile));
	    String line;
	    int lineNumber = 0;
	    try {
	      while ((line = br.readLine()) != null) {
	        String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
	        writer.write(Arrays.asList(fields));
	        ++lineNumber;
	      }

	      writer.close();
	     
	    } finally {
	      System.out.println("Number of lines: " + lineNumber);
	      br.close();
	    } 
	  }

	  public static void convertParquetToCSV(File parquetFile, File csvOutputFile) throws IOException {
	    Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"), 
	        "parquet file should have .parquet extension");
	    Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"), 
	        "csv file should have .csv extension");
	    Preconditions.checkArgument(!csvOutputFile.exists(), 
	        "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

	    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());


	    Path parquetFilePath = new Path(parquetFile.toURI());

	    Configuration configuration = new Configuration(true);

	    GroupReadSupport readSupport = new GroupReadSupport();
	    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
	    MessageType schema = readFooter.getFileMetaData().getSchema();

	    readSupport.init(configuration, null, schema);
	    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
	    ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);
	    try{
	      Group g = null;
	      while( (g = reader.read())!= null) {
	        writeGroup(w, g, schema);
	      }
	      reader.close();
	    }
	    finally {
	      Utils.closeQuietly(w);
	    }
	  }

	  private static void writeGroup(BufferedWriter w, Group g, MessageType schema) 
	      throws IOException{
	    for (int j = 0; j < schema.getFieldCount(); j++) {
	      if (j > 0) {
	        w.write(CSV_DELIMITER);
	      }
	      String valueToString = g.getValueToString(j, 0);
	      w.write(valueToString);
	    }
	    w.write('\n');
	  }

	  @Deprecated
	  public static void convertParquetToCSVEx(File parquetFile, File csvOutputFile) throws IOException {
	    Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"), 
	        "parquet file should have .parquet extension");
	    Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"), 
	        "csv file should have .csv extension");
	    Preconditions.checkArgument(!csvOutputFile.exists(), 
	        "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

	    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());

	    Path parquetFilePath = new Path(parquetFile.toURI());

	    Configuration configuration = new Configuration(true);

	    // TODO Following can be changed by using ParquetReader instead of ParquetFileReader
	    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
	    MessageType schema = readFooter.getFileMetaData().getSchema();
	    ParquetFileReader parquetFileReader = new ParquetFileReader(
	        configuration, parquetFilePath, readFooter.getBlocks(), schema.getColumns());
	    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
	    PageReadStore pages = null;
	    try {
	      while (null != (pages = parquetFileReader.readNextRowGroup())) {
	        final long rows = pages.getRowCount();
	        LOG.info("Number of rows: " + rows);

	        final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
	        final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
	        for (int i = 0; i < rows; i++) {
	          final Group g = recordReader.read();
	          writeGroup(w, g, schema);
	        }
	      } 
	    } finally {
	      Utils.closeQuietly(parquetFileReader);
	      Utils.closeQuietly(w);
	    }
	  }

*/}
