package org.hashmapinc.tempus.processors.witsml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hashmapinc.tempus.WitsmlObjects.Util.log.ColumnarDataTrace;
import com.hashmapinc.tempus.WitsmlObjects.Util.log.LogDataHelper;
import com.hashmapinc.tempus.WitsmlObjects.v1411.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import javax.xml.datatype.XMLGregorianCalendar;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Chris on 6/2/17.
 */
@Tags({"WITSML", "LOG", "MUDLOG", "TRAJECTORY"})
@CapabilityDescription("Get Data from Witsml Server for Objects Log, Mudlog and Trajectory.")
@ReadsAttributes({
        @ReadsAttribute(attribute="last.query.time", description="The time at which to end the query for the batch (upper limit for the index in the query")})
@WritesAttributes({
        @WritesAttribute(attribute="object.type", description="The WITSML type of the object being returned"),
        @WritesAttribute(attribute="next.query.depth", description="The depth to start the next query"),
        @WritesAttribute(attribute="next.query.time", description="The time to start the next query"),
        @WritesAttribute(attribute = "batch.order", description = "The monotonically increasing id that orders the data")})
public class GetData extends AbstractProcessor {

    private static ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor WITSML_SERVICE = new PropertyDescriptor
            .Builder().name("WITSML SERVICE")
            .displayName("WITSML Service")
            .description("The service to be used to connect to the server.")
            .required(true)
            .identifiesControllerService(IWitsmlServiceApi.class)
            .build();

    public static final PropertyDescriptor WELL_ID = new PropertyDescriptor
            .Builder().name("WELL ID")
            .displayName("Well ID")
            .description("Specify the Well Id")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WELLBORE_ID = new PropertyDescriptor
            .Builder().name("WELLBORE ID")
            .displayName("Wellbore ID")
            .description("Specify the Wellbore Id")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OBJECT_TYPE = new PropertyDescriptor
            .Builder().name("OBJECT TYPE")
            .displayName("Object Type")
            .description("Specify the type of the object to query for. Must only be trajectory or logData or logMetadata.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor OBJECT_ID = new PropertyDescriptor
            .Builder().name("OBJECT ID")
            .displayName("Object ID")
            .description("Specify the Object Id")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
  

    public static final PropertyDescriptor INDEX_TYPE = new PropertyDescriptor
            .Builder().name("INDEX TYPE")
            .displayName("Index Type")
            .description("The index type of the object.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor QUERY_START_DEPTH = new PropertyDescriptor
            .Builder().name("QUERY START DEPTH")
            .displayName("Query Start Depth")
            .description("The depth at which to start the query from.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY_START_TIME = new PropertyDescriptor
            .Builder().name("QUERY START TIME")
            .displayName("Query Start Time")
            .description("The time at which to start the query from.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY_END_DEPTH = new PropertyDescriptor
            .Builder().name("QUERY END DEPTH")
            .displayName("Query End Depth")
            .description("The depth at which to end the query at.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY_END_TIME = new PropertyDescriptor
            .Builder().name("QUERY END TIME")
            .displayName("Query End Time")
            .description("The time at which to end the query at.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOG_DATA_FORMAT = new PropertyDescriptor
            .Builder().name("LOG DATA FORMAT")
            .displayName("Log Data Format")
            .description("The format to return log data in, either CSV, or Columnar JSON (will result in one flowfile per mneumonic")
            .allowableValues("CSV", "JSON")
            .required(true)
            .defaultValue("JSON")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REQUERY_INDICATOR = new PropertyDescriptor
            .Builder().name("REQUERY INDICATOR")
            .displayName("Re-query Indicator")
            .description("The mechanism to use to determine whether all of the data has been received or not")
            .allowableValues("OBJECT_GROWING", "MAX_INDEX", "BATCH_INDEX")
            .required(true)
            .defaultValue("OBJECT_GROWING")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REPORTING_SERVICE = new PropertyDescriptor
            .Builder().name("REPORTING SERVICE")
            .displayName("Reporting Service")
            .description("The controller service used to report WITSML Server metrics.")
            .required(false)
            .identifiesControllerService(IStatsDReportingController.class)
            .build();
    
    public static final PropertyDescriptor LOG_INDEX_TYPE_CONVERT_FILTER = new PropertyDescriptor
            .Builder().name("TYPE CONVERSION FILTER")
            .displayName("Log Index Type Filter")
            .description("Converts the type of index field to String by specified length: 10")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship TRAJECTORY = new Relationship.Builder()
            .name("Trajectory")
            .description("Trajectory Data successfully received from the server")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Object Data not successfully received from the server")
            .build();

    public static final Relationship REQUERY = new Relationship.Builder()
            .name("Requery")
            .description("Data needs to be queried again, because the object is still growing.")
            .build();

    public static final Relationship TIME_INDEXED = new Relationship.Builder()
            .name("Time Indexed")
            .description("Time Indexed Log Data")
            .build();

    public static final Relationship DEPTH_INDEXED = new Relationship.Builder()
            .name("Depth Indexed")
            .description("Depth Indexed Log Data")
            .build();


    public static final String OBJECT_TYPE_ATTRIBUTE = "object.type";
    public static final String NEXT_QUERY_DEPTH_ATTRIBUTE = "next.query.depth";
    public static final String NEXT_QUERY_TIME_ATTRIBUTE = "next.query.time";
    public static final String BATCH_ORDER = "batch.order";

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(WITSML_SERVICE);
        descriptors.add(WELL_ID);
        descriptors.add(WELLBORE_ID);
        descriptors.add(OBJECT_ID);
        descriptors.add(LOG_DATA_FORMAT);
        descriptors.add(OBJECT_TYPE);
        descriptors.add(REQUERY_INDICATOR);
        descriptors.add(QUERY_START_TIME);
        descriptors.add(QUERY_START_DEPTH);
        descriptors.add(QUERY_END_TIME);
        descriptors.add(QUERY_END_DEPTH);
        descriptors.add(INDEX_TYPE);
        descriptors.add(REPORTING_SERVICE);
        descriptors.add(LOG_INDEX_TYPE_CONVERT_FILTER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(TRAJECTORY);
        relationships.add(FAILURE);
        relationships.add(REQUERY);
        relationships.add(TIME_INDEXED);
        relationships.add(DEPTH_INDEXED);
        this.relationships = Collections.unmodifiableSet(relationships);
        setMapper();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ComponentLog logger = getLogger();
        IWitsmlServiceApi witsmlServiceApi;

        // Get the Witsml Controller Service
        try {
            witsmlServiceApi = context.getProperty(WITSML_SERVICE).asControllerService(IWitsmlServiceApi.class);
        } catch (Exception ex) {
            logger.error("Error resolving the WITSML controller service in the GetData processor " + ex.getMessage());
            return;
        }

        IStatsDReportingController reportingService = null;

        boolean reporting = true;
        // Get the Witsml Controller Service
        try {
            reportingService = context.getProperty(REPORTING_SERVICE).asControllerService(IStatsDReportingController.class);
        } catch (Exception ex) {
            reporting = false;
        }

        // Gets the incoming flowfile
        FlowFile flowFile = session.get();

        // If the flowfile is null this is the first processor in the flow (hopefully, or something is very wrong)
        if (flowFile == null) {
            flowFile = session.create();
        }

        boolean flowFileHandled = processData(context, session, witsmlServiceApi, flowFile, reporting, reportingService);

        if (!flowFileHandled)
            session.remove(flowFile);
    }

    // returns a value of whether the incoming flowfile was processed.
    private boolean processData(ProcessContext context, ProcessSession session, IWitsmlServiceApi witsmlServiceApi, FlowFile parentFlowFile, boolean reporting, IStatsDReportingController reportingController){
        String queryType = context.getProperty(OBJECT_TYPE).evaluateAttributeExpressions(parentFlowFile).getValue();
        switch (queryType) {
            case "log": {
                return getLogData(context, session, witsmlServiceApi, parentFlowFile, reportingController);
            }
            case "trajectory": {
                return getTrajectoryData(context, session, witsmlServiceApi, parentFlowFile);
            }
            default: {
                return false;
            }
        }
    }

    private boolean getLogData(ProcessContext context, ProcessSession session, IWitsmlServiceApi witsmlServiceApi, FlowFile flowFile, IStatsDReportingController reportingController) {
        String wellId = context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String wellboreId = context.getProperty(WELLBORE_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");

        // Get the properties
        String logId = context.getProperty(OBJECT_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String startTime = context.getProperty(QUERY_START_TIME).evaluateAttributeExpressions(flowFile).getValue();
        String startDepth = context.getProperty(QUERY_START_DEPTH).evaluateAttributeExpressions(flowFile).getValue();
        String endDepth = context.getProperty(QUERY_END_DEPTH).evaluateAttributeExpressions(flowFile).getValue();
        String endTime = context.getProperty(QUERY_END_TIME).evaluateAttributeExpressions(flowFile).getValue();
        String typeConvertFilter = context.getProperty(LOG_INDEX_TYPE_CONVERT_FILTER).evaluateAttributeExpressions(flowFile).getValue();
        String timeZone = flowFile.getAttribute("timeZone");
        String logMax = flowFile.getAttribute("log.max");
        String logMin = flowFile.getAttribute("log.min");

        if (endDepth == null)
            endDepth = "";

        // Make the query
        StopWatch watch = new StopWatch();
        watch.start();
        ObjLogs logs = witsmlServiceApi.getLogData(wellId, wellboreId, logId, startDepth, startTime, endTime, endDepth, timeZone);
        watch.stop();

        if (reportingController != null){
            reportingController.incrementQueryCounter();
            reportingController.recordWitsmlQueryTime((int)watch.getDuration(TimeUnit.SECONDS));
        }

        if (logs == null){
            session.transfer(flowFile, FAILURE);
            return true;
        }

        try {
            if (logs.getLog().size() == 0) {
                session.transfer(flowFile, FAILURE);
                return true;
            }
        }
        catch (Exception ex)
        {
            getLogger().error(ex.getMessage());
            return true;
        }

        boolean isTime = logs.getLog().get(0).getIndexType().value().toLowerCase().contains("time");

        ObjLog targetLog = logs.getLog().get(0);

        String result;

        if (context.getProperty(LOG_DATA_FORMAT).getValue().equals("CSV")) {
            // Get the CSV data
            if (targetLog.getLogData().size() == 0){
                session.remove(flowFile);
                return true;
            }
            if(reportingController != null) {
                reportingController.recordNumberOfPointsReceived(
                        targetLog.getLogData().get(0).getData().size() * targetLog.getLogCurveInfo().size());
                String startRange = getISODate(targetLog.getStartDateTimeIndex(), timeZone);
                long start = iso8601toMillis(startRange);
                String endRange = getISODate(targetLog.getEndDateTimeIndex(), timeZone);
                long end = iso8601toMillis(endRange);
                reportingController.recordTimeSpanPerQuery((int)((end-start)/1000)/60);
                if (logMax != null && !logMax.equals("") && logMin != null && !logMin.equals("")){
                    long max = iso8601toMillis(logMax);
                    long min = iso8601toMillis(logMin);
                    long range = max - min;
                    long current = end - min;
                    long complete = (current/range)*100;
                    reportingController.recordPercentToDone((int)complete);
                    reportingController.recordLastTimeProcessed(end);
                }
            }
            result = LogDataHelper.getCSV(targetLog, true);
            // Create the new flowfile
            if (!result.equals("")) {
                FlowFile logDataFlowfile = session.create(flowFile);
                logDataFlowfile = session.write(logDataFlowfile, out -> out.write(result.getBytes()));
                if (isTime)
                    session.transfer(logDataFlowfile, TIME_INDEXED);
                else
                    session.transfer(logDataFlowfile, DEPTH_INDEXED);
            }
        } else {
            // Get data as columnar json
            if (targetLog.getLogData().size() == 0) {
                session.transfer(flowFile, REQUERY);
                return true;
            }
            if (logs.getLog().get(0).getLogData().size()== 0){
                session.transfer(flowFile, REQUERY);
                return true;
            }

            List<ColumnarDataTrace> data = LogDataHelper.getColumnarDataPoints(logs, true,typeConvertFilter);
            List<FlowFile> logDataFlowFiles = new ArrayList<>();
            if (isTime) {
            	if (getISODate(targetLog.getEndDateTimeIndex(), timeZone).compareToIgnoreCase(startTime)>0) {
		            for (ColumnarDataTrace dt : data){
		                FlowFile logDataFlowfile = session.create(flowFile);
		                session.putAttribute(logDataFlowfile, "id", dt.getLogUid());
		                session.putAttribute(logDataFlowfile, "name", dt.getLogName());
		                session.putAttribute(logDataFlowfile, "mnemonic", dt.getMnemonic());
		                session.putAttribute(logDataFlowfile, "uom", dt.getUnitOfMeasure());
		                
		                String results = null;
		                try {
		                    if (dt.getDataPoints().size() == 0) {
		                        session.remove(logDataFlowfile);
		                    } else {
		                        results = mapper.writeValueAsString(dt.getDataPoints());
		                        String finalResults = results;
		                        if (finalResults == null)
		                            continue;
		                        logDataFlowfile = session.write(logDataFlowfile, out -> out.write(finalResults.getBytes()));
		                        logDataFlowFiles.add(logDataFlowfile);
		                    }
		                } catch (JsonProcessingException e) {
		                    getLogger().error("Could not process columnar JSON");
		                }
		            }
            	}
            } else {
                double startDepthValue = 0.0;
                try {startDepthValue = Double.parseDouble(startDepth);} catch(Exception de) {}
                double endDepthValue = 1.0;
                try {endDepthValue = targetLog.getEndIndex().getValue();} catch(Exception de) {}
	            if (endDepthValue >= startDepthValue) {
		            for (ColumnarDataTrace dt : data){
		                FlowFile logDataFlowfile = session.create(flowFile);
		                session.putAttribute(logDataFlowfile, "id", dt.getLogUid());
		                session.putAttribute(logDataFlowfile, "name", dt.getLogName());
		                session.putAttribute(logDataFlowfile, "mnemonic", dt.getMnemonic());
		                session.putAttribute(logDataFlowfile, "uom", dt.getUnitOfMeasure());
		                
		                String results = null;
		                try {
		                    if (dt.getDataPoints().size() == 0) {
		                        session.remove(logDataFlowfile);
		                    } else {
		                        results = mapper.writeValueAsString(dt.getDataPoints());
		                        String finalResults = results;
		                        if (finalResults == null)
		                            continue;
		                        logDataFlowfile = session.write(logDataFlowfile, out -> out.write(finalResults.getBytes()));
		                        logDataFlowFiles.add(logDataFlowfile);
		                    }
		                } catch (JsonProcessingException e) {
		                    getLogger().error("Could not process columnar JSON");
		                }
		            }
	            }
            }

            //transfer all the flowfiles to success
            if (logDataFlowFiles.size() > 0) {
                if (isTime)
                    session.transfer(logDataFlowFiles, TIME_INDEXED);
                else
                    session.transfer(logDataFlowFiles, DEPTH_INDEXED);
            }
        }

        // Check for requery
        //return false to signal to caller that the original flowfile still needs to be handled
        String requeryIndicator = context.getProperty(REQUERY_INDICATOR).getValue();
        String objectType = "depth";
        if (isTime){
            objectType = "date time";
        }
        if (isLogGrowing(targetLog, requeryIndicator, getISODate(logs.getLog().get(0).getEndDateTimeIndex(), timeZone), endTime, timeZone)){
            if (objectType.equals("depth")) {

                if (targetLog.getEndIndex()!=null) {
                    String endIndex = Double.toString(targetLog.getEndIndex().getValue());
                    flowFile = session.putAttribute(flowFile,
                            NEXT_QUERY_DEPTH_ATTRIBUTE, endIndex+1);
                }
            }
            else {
                String currentTime = getISODate(targetLog.getEndDateTimeIndex(), timeZone);
                String nextQueryTime = getNextQuery(currentTime);
                flowFile = session.putAttribute(flowFile,
                        NEXT_QUERY_TIME_ATTRIBUTE, nextQueryTime);
            }
            session.transfer(flowFile, REQUERY);
        }
        else
            session.remove(flowFile);
        return true;
    }

    private boolean isLogGrowing(ObjLog targetLog, String requeryIndicator, String logResponseMax, String endBatchIndex, String timeZone){
        switch (requeryIndicator) {
            case "OBJECT_GROWING":
                return targetLog.isObjectGrowing();
            case "MAX_INDEX":
                LogIndexType logType = targetLog.getIndexType();
                if (logType == LogIndexType.DATE_TIME || logType == LogIndexType.ELAPSED_TIME) {
                    long logMaxMilli = iso8601toMillis(endBatchIndex);
                    long currentLogMax = iso8601toMillis(logResponseMax);
                    return (!(logMaxMilli <= currentLogMax));
                } else {
                    return (!logResponseMax.equals(Double.toString(targetLog.getEndIndex().getValue())));
                }
            case "BATCH_INDEX":
                long logMaxMilli = iso8601toMillis(endBatchIndex);
                long currentLogMax = iso8601toMillis(logResponseMax);
                return (!(logMaxMilli <= currentLogMax));
        }
        return false;
    }

    private long iso8601toMillis(String input){
        ZonedDateTime time = ZonedDateTime.parse(input,
                DateTimeFormatter.ofPattern(WitsmlConstants.TIMEZONE_FORMAT));
        return time.toInstant().toEpochMilli();
    }

    private boolean getTrajectoryData(ProcessContext context, ProcessSession session, IWitsmlServiceApi witsmlServiceApi, FlowFile flowFile) {
        // Get the properties
        String wellId = context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String wellboreId = context.getProperty(WELLBORE_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String trajectoryId = context.getProperty(OBJECT_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String startDepth = context.getProperty(QUERY_START_DEPTH).evaluateAttributeExpressions(flowFile).getValue();

        ObjTrajectorys trajectorys;
        if (startDepth == null)
            startDepth = "";
        if (trajectoryId == null) {
            session.transfer(flowFile, FAILURE);
            return true;
        }
        trajectorys = witsmlServiceApi.getTrajectoryData(wellId, wellboreId, trajectoryId, startDepth);

        // Something went wrong, route the flowfile to the failure relationship
        if (trajectorys == null) {
            session.transfer(flowFile, FAILURE);
            return true;
        }
        if (trajectorys.getTrajectory().size() == 0){
            session.transfer(flowFile, FAILURE);
            return true;
        }
        ObjTrajectory targetTrajectory = trajectorys.getTrajectory().get(0);

        List<CsTrajectoryStation> trajectoryStations = targetTrajectory.getTrajectoryStation();

        // Create JSON trajectory station array
        String jsonTrajectoryStation = "";

        List<FlowFile> trajectoryStationsFlowFiles = new ArrayList<>();
      
        
      //Splitting trajectory station data.
        if(trajectoryStations != null){
        	
        	for(CsTrajectoryStation trajStation:trajectoryStations){
        		try{
        			String jasonTrajStation = mapper.writeValueAsString(trajStation);
            		FlowFile trajectoryStnFlowfile = session.create(flowFile);
            		
            		trajectoryStnFlowfile = session.write(trajectoryStnFlowfile, outputStream -> outputStream.write(jasonTrajStation.getBytes()));
            		trajectoryStnFlowfile = session.putAttribute(trajectoryStnFlowfile, OBJECT_TYPE_ATTRIBUTE, "trajectory");
            		
            		trajectoryStationsFlowFiles.add(trajectoryStnFlowfile);
        		}catch (JsonProcessingException ex) {
                    getLogger().error("Error in converting TrajectoryStations to Json");
                }
        		
        	}
        	
        	if (targetTrajectory.isObjectGrowing()){
                session.transfer(flowFile, REQUERY);
            }else{
            	session.remove(flowFile);
            }
        }
        
        session.transfer(trajectoryStationsFlowFiles, TRAJECTORY);
     
        
        return true;
    }


    private void setMapper() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.setDateFormat(new SimpleDateFormat(WitsmlConstants.TIMEZONE_FORMAT));
    }

    private String getNextQuery(String timeStamp){
        ZonedDateTime currentTime = ZonedDateTime.parse(timeStamp, DateTimeFormatter.ISO_DATE_TIME);
        currentTime = currentTime.plusSeconds(1);
        return currentTime.format(DateTimeFormatter.ofPattern(WitsmlConstants.TIMEZONE_FORMAT));
    }

    private String getISODate(XMLGregorianCalendar date, String timeZone){
    	try {
            return date.toString();
    	} catch (Exception ex) {}
    	return "";
    }
}
