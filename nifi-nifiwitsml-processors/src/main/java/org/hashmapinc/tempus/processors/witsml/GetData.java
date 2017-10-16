package org.hashmapinc.tempus.processors.witsml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hashmapinc.tempus.WitsmlObjects.Util.log.AbstractDataTrace;
import com.hashmapinc.tempus.WitsmlObjects.Util.log.LogDataHelper;
import com.hashmapinc.tempus.WitsmlObjects.v1411.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.xml.datatype.XMLGregorianCalendar;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by Chris on 6/2/17.
 */
@Tags({"WITSML", "LOG", "MUDLOG", "TRAJECTORY"})
@CapabilityDescription("Get Data from Witsml Server for Objects Log, Mudlog and Trajectory.")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
        @WritesAttribute(attribute="object.type", description="The WITSML type of the object being returned"),
        @WritesAttribute(attribute="next.query.depth", description="The depth to start the next query"),
        @WritesAttribute(attribute="next.query.time", description="The time to start the next query")})
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
            .required(true)
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
            .description("The format to return log data in, either CSV or JSON")
            .allowableValues("JSON", "CSV")
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

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Object Data successfully received from the server")
            .build();

    public static final Relationship PARTIAL = new Relationship.Builder()
            .name("Partial")
            .description("The growing object data has not full been received yet and should be routed back for a re-query.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Object Data not successfully received from the server")
            .build();

    public static final String OBJECT_TYPE_ATTRIBUTE = "object.type";
    public static final String NEXT_QUERY_DEPTH_ATTRIBUTE = "next.query.depth";
    public static final String NEXT_QUERY_TIME_ATTRIBUTE = "next.query.time";

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
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships.add(PARTIAL);
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

        // Gets the incoming flowfile
        FlowFile flowFile = session.get();

        // If the flowfile is null this is the first processor in the flow (hopefully, or something is very wrong)
        if (flowFile == null) {
            flowFile = session.create();
        }

        boolean flowFileHandled = processData(context, session, witsmlServiceApi, flowFile);

        if (!flowFileHandled)
            session.remove(flowFile);
    }

    // returns a value of whether the incoming flowfile was processed.
    private boolean processData(ProcessContext context, ProcessSession session, IWitsmlServiceApi witsmlServiceApi, FlowFile parentFlowFile){
        String queryType = context.getProperty(OBJECT_TYPE).evaluateAttributeExpressions(parentFlowFile).getValue();
        switch (queryType) {
            case "log": {
                return getLogData(context, session, witsmlServiceApi, parentFlowFile);
            }
            case "trajectory": {
                return getTrajectoryData(context, session, witsmlServiceApi, parentFlowFile);
            }
            default: {
                return false;
            }
        }
    }

    private boolean getLogData(ProcessContext context, ProcessSession session, IWitsmlServiceApi witsmlServiceApi, FlowFile flowFile) {
        String wellId = context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String wellboreId = context.getProperty(WELLBORE_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");

        // Get the properties
        String logId = context.getProperty(OBJECT_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String startTime = context.getProperty(QUERY_START_TIME).evaluateAttributeExpressions(flowFile).getValue();
        String startDepth = context.getProperty(QUERY_START_DEPTH).evaluateAttributeExpressions(flowFile).getValue();
        String endDepth = context.getProperty(QUERY_END_DEPTH).evaluateAttributeExpressions(flowFile).getValue();
        String endTime = context.getProperty(QUERY_END_TIME).evaluateAttributeExpressions(flowFile).getValue();
        String timeZone = flowFile.getAttribute("timeZone");

        if (endDepth == null)
            endDepth = "";

        // Make the query
        ObjLogs logs = witsmlServiceApi.getLogData(wellId, wellboreId, logId, startDepth, startTime, endTime, endDepth, timeZone);
        if (logs == null){
            session.transfer(flowFile, FAILURE);
            return true;
        }

        String endBatchTime = flowFile.getAttribute("endBatchTime");

        if (logs.getLog().size() == 0){
            session.transfer(flowFile, FAILURE);
             return true;
        }

        ObjLog targetLog = logs.getLog().get(0);


        String result = "";

        // Determine if we have to convert to JSON
        if (context.getProperty(LOG_DATA_FORMAT).getValue().equals("JSON")) {
            List<AbstractDataTrace> process = LogDataHelper.processData(logs);
            ObjectMapper mapper = new ObjectMapper();

            try {
                result = mapper.writeValueAsString(process);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        else {
            // Get the CSV data
            result = LogDataHelper.getCSV(logs.getLog().get(0), true);
        }

        // Create the new flowfile
        FlowFile logDataFlowfile = session.create(flowFile);

        if (logDataFlowfile == null) {
            session.transfer(flowFile, FAILURE);
            return true;
        }

        // The final data to write to the flow file
        final String logDataToWrite = result;

        // Write the flowfile data
        logDataFlowfile = session.write(logDataFlowfile, out -> out.write(logDataToWrite.getBytes()));

        String indexType = flowFile.getAttribute("indexType");

        // Set attributes
        String objectType = "depth";
        if (indexType.toLowerCase().contains("time")){
            objectType = "date time";
        }

        String requeryIndicator = context.getProperty(REQUERY_INDICATOR).getValue();

        // Determine where to route the data
        logDataFlowfile = session.putAttribute(logDataFlowfile, OBJECT_TYPE_ATTRIBUTE, objectType);
        if (isLogGrowing(targetLog, requeryIndicator, getISODate(logs.getLog().get(0).getEndDateTimeIndex(), timeZone), endBatchTime, timeZone)){
            if (objectType.equals("depth")) {
                logDataFlowfile = session.putAttribute(logDataFlowfile,
                        NEXT_QUERY_DEPTH_ATTRIBUTE, Double.toString(targetLog.getEndIndex().getValue()));
            }
            else {
                    logDataFlowfile = session.putAttribute(logDataFlowfile,
                            NEXT_QUERY_TIME_ATTRIBUTE, getISODate(targetLog.getEndDateTimeIndex(), timeZone));
            }
            session.transfer(logDataFlowfile, PARTIAL);
        }
        else
            session.transfer(logDataFlowfile, SUCCESS);

        return false;
    }

    private boolean isLogGrowing(ObjLog targetLog, String requeryIndicator, String logResponseMax, String endBatchIndex, String timeZone){
        if (requeryIndicator.equals("OBJECT_GROWING")){
            return targetLog.isObjectGrowing();
        } else if (requeryIndicator.equals("MAX_INDEX")){
            LogIndexType logType = targetLog.getIndexType();
            if (logType == LogIndexType.DATE_TIME || logType == LogIndexType.ELAPSED_TIME){
                long logMaxMilli = iso8601toMillis(logResponseMax);
                long currentLogMax = iso8601toMillis(getISODate(targetLog.getEndDateTimeIndex(), timeZone));
                return (!(logMaxMilli <= currentLogMax));
            } else{
                return (!logResponseMax.equals(Double.toString(targetLog.getEndIndex().getValue())));
            }
        } else if(requeryIndicator.equals("BATCH_INDEX")){
            long logMaxMilli = iso8601toMillis(endBatchIndex);
            long currentLogMax = iso8601toMillis(logResponseMax);
            return (!(logMaxMilli <= currentLogMax));
        }
        return false;
    }


    private long iso8601toMillis(String input){
        ZonedDateTime time = ZonedDateTime.parse("2014-11-17T23:59:59.000-0600",
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
        return time.getSecond()*1000;
    }

    private boolean getLogCurveInfos(ProcessSession session, ObjLog targetLog, FlowFile flowFile){
        List<CsLogCurveInfo> logCurveInfos = targetLog.getLogCurveInfo();

        String jsonLogCurveInfo = "";

        if (!logCurveInfos.isEmpty()) {
            try {
                jsonLogCurveInfo = mapper.writeValueAsString(logCurveInfos);
            } catch (JsonProcessingException ex) {
                getLogger().error("Error in converting LogCurveInfo to JSON :" + ex.getMessage());
                session.transfer(flowFile, FAILURE);
                return true;
            }
        }
        if (!jsonLogCurveInfo.equals("")) {
            String finalData = jsonLogCurveInfo;
            FlowFile logCurveInfoFlowfile = session.create(flowFile);
            if (logCurveInfoFlowfile == null) {
                session.transfer(flowFile, FAILURE);
                return true;
            }
            logCurveInfoFlowfile = session.write(logCurveInfoFlowfile, out -> out.write(finalData.getBytes()));
            logCurveInfoFlowfile = session.putAttribute(logCurveInfoFlowfile, OBJECT_TYPE_ATTRIBUTE, "log curve info");
            session.transfer(logCurveInfoFlowfile, SUCCESS);
        }
        return false;
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

        try {
            jsonTrajectoryStation = mapper.writeValueAsString(trajectoryStations);
        } catch (JsonProcessingException ex) {
            getLogger().error("Error in converting TrajectoryStations to Json");
        }
        if (!jsonTrajectoryStation.equals("")) {
            String finalTrajectoryData = jsonTrajectoryStation;
            FlowFile trajectoryFlowfile = session.create(flowFile);
            if (trajectoryFlowfile == null) {
                session.transfer(flowFile, FAILURE);
                return true;
            }
            trajectoryFlowfile = session.write(trajectoryFlowfile, outputStream -> outputStream.write(finalTrajectoryData.getBytes()));
            trajectoryFlowfile = session.putAttribute(trajectoryFlowfile, OBJECT_TYPE_ATTRIBUTE, "trajectory");
            if (targetTrajectory.isObjectGrowing()){
                session.transfer(trajectoryFlowfile, PARTIAL);
            }
            else {
                session.transfer(trajectoryFlowfile, SUCCESS);
            }
        }
        return false;
    }

    private String convertLogDataToJson(String logData, String logId) {
        String[] logDataArray = logData.split("\n");
        String[] mnemonicsArray = logDataArray[0].split(",");

        JSONArray jsonArray = new JSONArray();
        String[] values;
        for (int i = 2; i < logDataArray.length; i++) {
            values = logDataArray[i].split(",");

            for (int j = 1; j < mnemonicsArray.length; j++) {
                JSONObject jsonObject = new JSONObject()
                                        .put("uri", logId+"/"+mnemonicsArray[j])
                                        .put("value", values[j])
                                        .put("index", values[0]);
                jsonArray.put(jsonObject);
            }
        }
        JSONObject jsonObject = new JSONObject()
                                .put("list", jsonArray);
        return jsonObject.get("list").toString();
    }

    private void setMapper() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    }

    private String getISODate(XMLGregorianCalendar date, String timeZone){
        return String.format("%04d", date.getYear()) + "-" + String.format("%02d", date.getMonth()) + "-" +
                String.format("%02d", date.getDay()) + "T" + String.format("%02d", date.getHour()) + ":" +
                String.format("%02d", date.getMinute()) + ":" + String.format("%02d", date.getSecond()) + "." +
                String.format("%03d", date.getMillisecond()) + timeZone;
    }
}
