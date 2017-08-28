package org.hashmapinc.tempus.processors.witsml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hashmapinc.tempus.WitsmlObjects.Util.log.LogDataHelper;
import com.hashmapinc.tempus.WitsmlObjects.v1411.*;
import org.apache.axis.session.Session;
import org.apache.axis.utils.JavaUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;


import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Chris on 6/2/17.
 */
@Tags({"WITSML", "LOG", "MUDLOG", "TRAJECTORY"})
@CapabilityDescription("Get Data from Witsml Server for Objects Log, Mudlog and Trajectory.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetData extends AbstractProcessor {

    private static long logHashCode = 0;
    private static long mudLogHashCode = 0;
    private static long trajectoryHashCode = 0;
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

    public static final PropertyDescriptor LOG_ID = new PropertyDescriptor
            .Builder().name("LOG ID")
            .displayName("Log ID")
            .description("Specify the Log Id")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MUDLOG_ID = new PropertyDescriptor
            .Builder().name("MUDLOG ID")
            .displayName("MudLog ID")
            .description("Specify the Mudlog Id")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TRAJECTORY_ID = new PropertyDescriptor
            .Builder().name("TRAJECTORY ID")
            .displayName("Trajectory ID")
            .description("Specify the Trajectory Id")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship LOG_DATA_SUCCESS = new Relationship.Builder()
            .name("Log Data Success")
            .description("Log Data in CSV format received form Server")
            .build();

    public static final Relationship LOG_DATA_FAILURE = new Relationship.Builder()
            .name("Log Data Failure")
            .description("Log Data is not received from server")
            .build();

    public static final Relationship LOGCURVEINFO_SUCCESS = new Relationship.Builder()
            .name("LogCurveInfo Success")
            .description("Log Data successfully received from the server")
            .build();

    public static final Relationship LOGCURVEINFO_FAILURE = new Relationship.Builder()
            .name("LogCurveInfo Failure")
            .description("Log Data not successfully received from the server")
            .build();

    public static final Relationship MUDLOG_SUCCESS = new Relationship.Builder()
            .name("ModLog Success")
            .description("MudLog GeologyInterval Data successfully received from the server")
            .build();

    public static final Relationship MUDLOG_FAILURE = new Relationship.Builder()
            .name("MudLog Failure")
            .description("MudLog GeologyInterval Data not successfully received from the server")
            .build();

    public static final Relationship TRAJECTORY_SUCCESS = new Relationship.Builder()
            .name("Trajectory Success")
            .description("TrajectoryStation Data successfully received from the server")
            .build();

    public static final Relationship TRAJECTORY_FAILURE = new Relationship.Builder()
            .name("Trajectory Failure")
            .description("TrajectoryStation Data not successfully received from the server")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(WITSML_SERVICE);
        descriptors.add(WELL_ID);
        descriptors.add(WELLBORE_ID);
        descriptors.add(LOG_ID);
        descriptors.add(MUDLOG_ID);
        descriptors.add(TRAJECTORY_ID);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(LOG_DATA_SUCCESS);
        relationships.add(LOG_DATA_FAILURE);
        relationships.add(LOGCURVEINFO_SUCCESS);
        relationships.add(LOGCURVEINFO_FAILURE);
        relationships.add(MUDLOG_SUCCESS);
        relationships.add(MUDLOG_FAILURE);
        relationships.add(TRAJECTORY_SUCCESS);
        relationships.add(TRAJECTORY_FAILURE);
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

        try {
            witsmlServiceApi = context.getProperty(WITSML_SERVICE).asControllerService(IWitsmlServiceApi.class);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return;
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }

        /**********LOG DATA***********/
        writeLogData(context, session, witsmlServiceApi, flowFile);

        /*************MUDLOG DATA**********/
        writeMudLogData(context, session, witsmlServiceApi, flowFile);

        /*********TRAJECTORY DATA***********/
        writeTrajectoryData(context, session, witsmlServiceApi, flowFile);

        session.remove(flowFile);
    }

    private void writeLogData(ProcessContext context, ProcessSession session, IWitsmlServiceApi witsmlServiceApi, FlowFile flowFile) {
        ObjLogs logs = null;
        if (context.getProperty(LOG_ID).evaluateAttributeExpressions(flowFile).getValue() != null) {
            logs = witsmlServiceApi.getLogData(context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue().toString().replaceAll("[;\\s\t]", ""),
                    context.getProperty(WELLBORE_ID).evaluateAttributeExpressions(flowFile).getValue().toString().replaceAll("[;\\s\t]", ""),
                    context.getProperty(LOG_ID).evaluateAttributeExpressions(flowFile).getValue().toString().replaceAll("[;\\s\t]", ""));
        }
        if (logs != null) {
            String logData = LogDataHelper.getCSV(logs.getLog().get(0), true);

            if (logHashCode != logData.hashCode()) {
                logHashCode = logData.hashCode();
                FlowFile logDataFlowfile = session.create(flowFile);
                if (logDataFlowfile == null) {
                    return;
                }
                try {
                    logDataFlowfile = session.write(logDataFlowfile, new OutputStreamCallback() {

                        @Override
                        public void process(OutputStream out) throws IOException {
                            out.write(logData.toString().getBytes());
                        }

                    });
                    session.transfer(logDataFlowfile, LOG_DATA_SUCCESS);
                } catch (ProcessException ex) {
                    getLogger().error("Error in Log Data : " + ex);
                    session.transfer(logDataFlowfile, LOG_DATA_FAILURE);
                }


                List<CsLogCurveInfo> logCurveInfos = logs.getLog().get(0).getLogCurveInfo();
                String jsonLogCurveInfo = "";
                if (!logCurveInfos.isEmpty()) {
                    try {
                        jsonLogCurveInfo = mapper.writeValueAsString(logCurveInfos);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting LogCureveInfo to Json :" + ex);
                    }

                }
                if (jsonLogCurveInfo != "") {
                    String finalData = jsonLogCurveInfo;
                    FlowFile logFlowfile = session.create(flowFile);
                    if (logFlowfile == null) {
                        return;
                    }
                    try {
                        logFlowfile = session.write(logFlowfile, new OutputStreamCallback() {

                            @Override
                            public void process(OutputStream out) throws IOException {
                                out.write(finalData.toString().getBytes());
                            }

                        });
                        session.transfer(logFlowfile, LOGCURVEINFO_SUCCESS);
                    } catch (ProcessException ex) {
                        getLogger().error("Error in Log Data : " + ex);
                        session.transfer(logFlowfile, LOGCURVEINFO_FAILURE);
                    }
                }
            }
        }
    }

    private void writeMudLogData(ProcessContext context, ProcessSession session, IWitsmlServiceApi witsmlServiceApi, FlowFile flowFile) {
        ObjMudLogs mudLogs = null;
        if (context.getProperty(MUDLOG_ID).evaluateAttributeExpressions(flowFile).getValue() != null) {
            mudLogs = witsmlServiceApi.getMudLogData(context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue().toString().replaceAll("[;\\s\t]", ""),
                    context.getProperty(WELLBORE_ID).evaluateAttributeExpressions(flowFile).getValue().toString().replaceAll("[;\\s\t]", ""),
                    context.getProperty(MUDLOG_ID).evaluateAttributeExpressions(flowFile).getValue().toString().replaceAll("[;\\s\t]", ""));
        }
        if (mudLogs != null) {
            List<CsGeologyInterval> geologyIntervals = mudLogs.getMudLog().get(0).getGeologyInterval();
            String lastGeologyIntervalUid = geologyIntervals.get(geologyIntervals.size() - 1).getUid();
            String jsonGeologyInterval = "";

            if (mudLogHashCode != lastGeologyIntervalUid.hashCode()) {
                mudLogHashCode = lastGeologyIntervalUid.hashCode();
                try {
                    jsonGeologyInterval = mapper.writeValueAsString(geologyIntervals);
                } catch (JsonProcessingException ex) {
                    getLogger().error("Error in converting GeologyInterval to json");
                }
            }
            if (jsonGeologyInterval != "") {
                String finalMudlogData = jsonGeologyInterval;
                FlowFile mudLogFlowfile = session.create(flowFile);
                if (mudLogFlowfile == null) {
                    return;
                }
                try {
                    mudLogFlowfile = session.write(mudLogFlowfile, new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream outputStream) throws IOException {
                            outputStream.write(finalMudlogData.toString().getBytes());
                        }
                    });
                    session.transfer(mudLogFlowfile, MUDLOG_SUCCESS);
                } catch (ProcessException ex) {
                    getLogger().error("Error in MudLog data : " + ex);
                    session.transfer(mudLogFlowfile, MUDLOG_FAILURE);
                }
            }
        }
    }

    private void writeTrajectoryData(ProcessContext context, ProcessSession session, IWitsmlServiceApi witsmlServiceApi, FlowFile flowFile) {
        ObjTrajectorys trajectorys = null;
        if (context.getProperty(TRAJECTORY_ID).evaluateAttributeExpressions(flowFile).getValue() != null) {
            trajectorys = witsmlServiceApi.getTrajectoryData(context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue().toString().replaceAll("[;\\s\t]", ""),
                    context.getProperty(WELLBORE_ID).evaluateAttributeExpressions(flowFile).getValue().toString().replaceAll("[;\\s\t]", ""),
                    context.getProperty(TRAJECTORY_ID).evaluateAttributeExpressions(flowFile).getValue().toString().replaceAll("[;\\s\t]", ""));
        }
        if (trajectorys != null) {
            List<CsTrajectoryStation> trajectoryStations = trajectorys.getTrajectory().get(0).getTrajectoryStation();
            String lastTrajectoryStationUid = trajectoryStations.get(trajectoryStations.size() - 1).getUid();
            String jsonTrajectoryStation = "";

            if (trajectoryHashCode != lastTrajectoryStationUid.hashCode()) {
                trajectoryHashCode = lastTrajectoryStationUid.hashCode();
                try {
                    jsonTrajectoryStation = mapper.writeValueAsString(trajectoryStations);
                } catch (JsonProcessingException ex) {
                    getLogger().error("Error in converting TrajectoryStations to Json");
                }
            }
            if (jsonTrajectoryStation != "") {
                String finalTrajectoryData = jsonTrajectoryStation;
                FlowFile trajectoryFlowfile = session.create(flowFile);
                if (trajectoryFlowfile == null) {
                    return;
                }
                try {
                    trajectoryFlowfile = session.write(trajectoryFlowfile, new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream outputStream) throws IOException {
                            outputStream.write(finalTrajectoryData.toString().getBytes());
                        }
                    });
                    session.transfer(trajectoryFlowfile, TRAJECTORY_SUCCESS);
                } catch (ProcessException ex) {
                    getLogger().error("Error in Trajectory Data : " + ex);
                    session.transfer(trajectoryFlowfile, TRAJECTORY_FAILURE);
                }
            }
        }
    }

    private void setMapper() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    }
}
