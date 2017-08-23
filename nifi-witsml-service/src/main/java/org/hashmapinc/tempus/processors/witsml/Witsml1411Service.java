package org.hashmapinc.tempus.processors.witsml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hashmapinc.tempus.WitsmlObjects.Util.log.LogDataHelper;
import com.hashmapinc.tempus.WitsmlObjects.v1411.*;
import com.hashmapinc.tempus.witsml.api.LogRequestTracker;
import com.hashmapinc.tempus.witsml.api.MudlogRequestTracker;
import com.hashmapinc.tempus.witsml.api.TrajectoryRequestTracker;
import javafx.scene.control.IndexRange;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import com.hashmapinc.tempus.witsml.client.Client;
import com.hashmapinc.tempus.witsml.api.WitsmlVersion;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Chris on 6/2/17.
 */

@Tags({"WITSML", "Well", "Wellbore", "Log", "Mudlog", "Trajectory", "Version1.4.1.1"})
@CapabilityDescription("Provides session management for Witsml processors")
public class Witsml1411Service extends AbstractControllerService implements IWitsmlServiceApi{

    // Global session variables used by all processors using an instance
    private static Client myClient = null;
    private static LogRequestTracker logTracker = new LogRequestTracker();
    private static MudlogRequestTracker mudLogTracker = new MudlogRequestTracker();
    private static TrajectoryRequestTracker trajectoryTracker = new TrajectoryRequestTracker();
    private static ObjectMapper mapper = new ObjectMapper();


    //Properties
    public static final PropertyDescriptor ENDPOINT_URL = new PropertyDescriptor
            .Builder().name("Endpoint URL")
            .description("Specify the witsml.tcp address of the witsml server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder().name("Username")
            .description("Specify the username for Witsml Server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("Password")
            .description("Specify the password for Witsml Server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ENDPOINT_URL);
        props.add(USERNAME);
        props.add(PASSWORD);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final ComponentLog logger = getLogger();
        logger.info("Creating Witsml Client");

        myClient = new Client(context.getProperty(ENDPOINT_URL).getValue());
        myClient.setUserName(context.getProperty(USERNAME).getValue());
        myClient.setPassword(context.getProperty(PASSWORD).getValue());
        myClient.setVersion(WitsmlVersion.VERSION_1411);
        myClient.connect();
        setMapper();
    }

    @Override
    public String getObject(String wellId, String wellboreId, String object) {
        try {
            switch (object) {
                case "ATTACHMENT" :
                    ObjAttachments attachments = myClient.getAttachmentsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(attachments);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting Attachments Object to Json" + ex);
                    }
                    break;
                case "BHARUN":
                    ObjBhaRuns bhaRuns = myClient.getBhaRunsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(bhaRuns);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting BhaRuns Object to Json" + ex);
                    }
                    break;
                case "CEMENTJOB":
                    ObjCementJobs cementJobs = myClient.getCementJobsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(cementJobs);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting CementJobs to Json : " + ex);
                    }
                    break;
                case "CHANGELOG" :
                    ObjChangeLogs changeLogs = myClient.getChangeLogsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(changeLogs);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting ChangeLogs Object to Json" + ex);
                    }
                    break;
                case "CONVCORE":
                    ObjConvCores convCores = myClient.getConvCoresAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(convCores);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting ConvCores to Json : " + ex);
                    }
                    break;
                case "DRILLREPORT" :
                    ObjDrillReports drillReports = myClient.getDrillReportsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(drillReports);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting DrillReports Object to Json" + ex);
                    }
                    break;
                case "FLUIDREPORT":
                    ObjFluidsReports fluidsReports = myClient.getFluidsReportsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(fluidsReports);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting FluidsReports to Json : " + ex);
                    }
                    break;
                case "FORMATIONMARKER":
                    ObjFormationMarkers formationMarkers = myClient.getFormationMarkersAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(formationMarkers);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting FormationMarkers to Json : " + ex);
                    }
                    break;
                case "LOG":
                    ObjLogs logs = myClient.getLogMetadataAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(logs);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting Logs to Json : " + ex);
                    }
                    break;
                case "MESSAGE":
                    ObjMessages messages = myClient.getMessagesAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(messages);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting Messages to Json : " + ex);
                    }
                    break;
                case "MUDLOG":
                    ObjMudLogs mudLogs = myClient.getMudLogsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(mudLogs);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting MudLogs to Json : " + ex);
                    }
                    break;
                case "OBJECTGROUP" :
                    ObjObjectGroups objObjectGroups = myClient.getObjectGroupsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(objObjectGroups);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting ObjectGroups Object to Json" + ex);
                    }
                    break;
                case "OPSREPORT":
                    ObjOpsReports opsReports = myClient.getOpsReportsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(opsReports);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting OpsReports to Json : " + ex);
                    }
                    break;
                case "RIG":
                    ObjRigs rigs = myClient.getRigsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(rigs);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting Rigs to Json : " + ex);
                    }
                    break;
                case "RISK":
                    ObjRisks risks = myClient.getRisksAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(risks);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting Risks to Json : " + ex);
                    }
                    break;
                case "SIDEWALLCORE":
                    ObjSidewallCores sidewallCores = myClient.getSideWallCoresAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(sidewallCores);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting SideWallCores to Json : " + ex);
                    }
                    break;
                case "STIMJOB" :
                    ObjStimJobs stimJobs = myClient.getStimJobsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(stimJobs);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting StimJobs Object to Json" + ex);
                    }
                    break;
                case "SURVEYPROGRAM":
                    ObjSurveyPrograms surveyPrograms = myClient.getSurveyProgramsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(surveyPrograms);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting SurveyPrograms to Json : " + ex);
                    }
                    break;
                case "TARGET":
                    ObjTargets targets = myClient.getTargetsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(targets);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting Targets to Json : " + ex);
                    }
                    break;
                case "TRAJECTORY":
                    ObjTrajectorys trajectorys = myClient.getTrajectorysAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(trajectorys);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting Trajectorys to Json : " + ex);
                    }
                    break;
                case "TUBULAR":
                    ObjTubulars tubulars = myClient.getTubularsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(tubulars);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting Tubulars to Json : " + ex);
                    }
                    break;
                case "WBGEOMETRY":
                    ObjWbGeometrys wbGeometrys = myClient.getWbGeometrysAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(wbGeometrys);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting WbGeometrys to Json : " + ex);
                    }
                    break;
                default:
                    getLogger().error("The Object : " + object + " is not supported/present");
                    break;
            }

        } catch (Exception ex) {
            getLogger().error("Error in getting data from WITSML server");
        }
        return null;
    }

    @Override
    public ObjLogs getLogData(String wellId, String wellboreId, String logId) {
        logTracker.setVersion(WitsmlVersion.VERSION_1411);
        logTracker.setLogId(logId);
        logTracker.initalize(myClient, wellId, wellboreId);

        return logTracker.ExecuteRequest();
    }

    @Override
    public ObjMudLogs getMudLogData(String wellId, String wellboreId, String mudLogId) {
        mudLogTracker.setVersion(WitsmlVersion.VERSION_1411);
        mudLogTracker.setMudlogId(mudLogId);
        mudLogTracker.initalize(myClient, wellId, wellboreId);

        return mudLogTracker.ExecuteRequest();
    }

    @Override
    public ObjTrajectorys getTrajectoryData(String wellId, String wellboreId, String trajectoryid) {
        trajectoryTracker.setVersion(WitsmlVersion.VERSION_1411);
        trajectoryTracker.setTrajectoryId(trajectoryid);
        trajectoryTracker.initalize(myClient, wellId, wellboreId);

        return trajectoryTracker.ExecuteRequest();
    }

    private void setMapper() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    }
}
