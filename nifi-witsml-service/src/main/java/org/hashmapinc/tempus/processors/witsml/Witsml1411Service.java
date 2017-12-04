package org.hashmapinc.tempus.processors.witsml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hashmapinc.tempus.WitsmlObjects.v1411.*;
import com.hashmapinc.tempus.witsml.api.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import com.hashmapinc.tempus.witsml.client.Client;

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
            .sensitive(true)
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
    }

    @Override
    public Object getObject(String wellId, String wellboreId, String object) {
        try {
            switch (object) {
                case "ATTACHMENT" :
                    return myClient.getAttachmentsAsObj(wellId, wellboreId);
                case "BHARUN":
                    return myClient.getBhaRunsAsObj(wellId, wellboreId);
                case "CEMENTJOB":
                    return myClient.getCementJobsAsObj(wellId, wellboreId);
                case "CHANGELOG" :
                    return myClient.getChangeLogsAsObj(wellId, wellboreId);
                case "CONVCORE":
                    return myClient.getConvCoresAsObj(wellId, wellboreId);
                case "DRILLREPORT" :
                    return myClient.getDrillReportsAsObj(wellId, wellboreId);
                case "FLUIDREPORT":
                    return myClient.getFluidsReportsAsObj(wellId, wellboreId);
                case "FORMATIONMARKER":
                    return myClient.getFormationMarkersAsObj(wellId, wellboreId);
                case "LOG":
                    return myClient.getLogMetadataAsObj(wellId, wellboreId);
                case "MESSAGE":
                    return myClient.getMessagesAsObj(wellId, wellboreId);
                case "MUDLOG":
                    return myClient.getMudLogsAsObj(wellId, wellboreId);
                case "OBJECTGROUP" :
                    return myClient.getObjectGroupsAsObj(wellId, wellboreId);
                case "OPSREPORT":
                    return myClient.getOpsReportsAsObj(wellId, wellboreId);
                case "RIG":
                    return myClient.getRigsAsObj(wellId, wellboreId);
                case "RISK":
                    return myClient.getRisksAsObj(wellId, wellboreId);
                case "SIDEWALLCORE":
                    return myClient.getSideWallCoresAsObj(wellId, wellboreId);
                case "STIMJOB" :
                    return myClient.getStimJobsAsObj(wellId, wellboreId);
                case "SURVEYPROGRAM":
                    return myClient.getSurveyProgramsAsObj(wellId, wellboreId);
                case "TARGET":
                    return myClient.getTargetsAsObj(wellId, wellboreId);
                case "TRAJECTORY":
                    return myClient.getTrajectorysAsObj(wellId, wellboreId);
                case "TUBULAR":
                    return myClient.getTubularsAsObj(wellId, wellboreId);
                case "WBGEOMETRY":
                    return myClient.getWbGeometrysAsObj(wellId, wellboreId);
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
    public String getUrl() {
        return myClient.getUrl();
    }

    @Override
    public ObjLogs getLogData(String wellId, String wellboreId, String logId, LogRequestTracker logTracker) {
        logTracker.setVersion(WitsmlVersion.VERSION_1411);
        logTracker.setLogId(logId);
        logTracker.initalize(myClient, wellId, wellboreId);

        return logTracker.ExecuteRequest();
    }

    @Override
    public ObjLogs getLogData(String wellId, String wellboreId, String logId, String startDepth, String startTime, String endTime, String endDepth, String timeZone) {
        return null;
    }

    @Override
    public ObjMudLogs getMudLogData(String wellId, String wellboreId, String mudLogId, MudlogRequestTracker mudLogTracker) {
        mudLogTracker.setVersion(WitsmlVersion.VERSION_1411);
        mudLogTracker.setMudlogId(mudLogId);
        mudLogTracker.initalize(myClient, wellId, wellboreId);

        return mudLogTracker.ExecuteRequest();
    }

    @Override
    public ObjTrajectorys getTrajectoryData(String wellId, String wellboreId, String trajectoryId, String startDepth) {
        return null;
    }

    @Override
    public ObjTrajectorys getTrajectoryData(String wellId, String wellboreId, String trajectoryid, TrajectoryRequestTracker trajectoryTracker) {
        trajectoryTracker.setVersion(WitsmlVersion.VERSION_1411);
        trajectoryTracker.setTrajectoryId(trajectoryid);
        trajectoryTracker.initalize(myClient, wellId, wellboreId);

        return trajectoryTracker.ExecuteRequest();
    }

    @Override
    public List<WitsmlObjectId> getAvailableObjects(String uri, List<String> objectTypes, String wellFilter){
        QueryTarget target = QueryTarget.parseURI(uri, objectTypes);
        List<WitsmlObjectId> ids = new ArrayList<>();
        switch (target.getQueryLevel()){
            case Server: {
                ObjWells wells = getWell("", wellFilter);

                if (wells == null)
                    return null;
                for (ObjWell w:wells.getWell()) {
                    if (w == null)
                        continue;
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                    String data = null;
                    try {
                        data = mapper.writeValueAsString(w);
                    } catch (JsonProcessingException e) {
                        getLogger().error("unable to parse wellbore data in getAvailable objects: " + e.getMessage());
                    }
                    WitsmlObjectId objId = new WitsmlObjectId(w.getName(), w.getUid(), "well", "", data);
                    ids.add(objId);
                }
                break;
            }
            case Well: {
                ObjWellbores wellbores = getWellboreData(target.getWell());
                if (wellbores == null)
                    return null;
                for (ObjWellbore wb:wellbores.getWellbore()){
                    if (wb == null)
                        continue;
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                    String data = null;
                    try {
                        data = mapper.writeValueAsString(wb);
                    } catch (JsonProcessingException e) {
                        getLogger().error("unable to parse wellbore data in getAvailable objects: " + e.getMessage());
                    }
                    WitsmlObjectId objId = new WitsmlObjectId(wb.getName(), wb.getUid(), "wellbore", "/" + wb.getNameWell() + "(" + wb.getUidWell() + ")", data);
                    ids.add(objId);
                }
                break;
            }
            case Wellbore:
                ids = queryForTypes(target);
                break;
        }
        return ids;
    }

    private ObjWells getWellData(){
        try {
            return myClient.getWellsAsObj();
        } catch (Exception e) {
            getLogger().error("Error in getWells: " + e.getMessage());
            return null;
        }
    }

    private ObjWellbores getWellboreData(WitsmlObjectId well){
        try {
            return myClient.getWellboresForWellAsObj(well.getId());
        } catch (Exception e) {
            getLogger().error("Error in getWellbores: " + e.getMessage());
            return null;
        }
    }

    private List<WitsmlObjectId> queryForTypes(QueryTarget targetObj){
        List<WitsmlObjectId> ids = new ArrayList<>();
        List<String> types = targetObj.getObjectsToQuery();
        String wellId = targetObj.getWell().getId();
        String wellboreId = targetObj.getWellbore().getId();
        String parentURI = "/" + targetObj.getWell().getName() + "(" + targetObj.getWell().getId() + ")/" + targetObj.getWellbore().getName() + "(" + targetObj.getWellbore().getId() + ")";

        for (String type : types) {
            try {
                switch (type.toUpperCase()) {
                    case "LOG":
                        ObjLogs logs = myClient.getLogMetadataAsObj(wellId, wellboreId);
                        if (logs == null) {
                            continue;
                        }
                        for (ObjLog log : logs.getLog()) {
                            if (log == null)
                                continue;
                            ObjectMapper mapper = new ObjectMapper();
                            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                            String data = mapper.writeValueAsString(log);
                            ids.add(new WitsmlObjectId(log.getName(), log.getUid(), "log", parentURI, data));
                        }
                        break;
                    case "MESSAGE":
                        ObjMessages messages = myClient.getMessagesAsObj(wellId, wellboreId);
                        if (messages == null) {
                            continue;
                        }
                        for (ObjMessage message : messages.getMessage()) {
                            if (message == null)
                                continue;
                            ObjectMapper mapper = new ObjectMapper();
                            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                            String data = mapper.writeValueAsString(message);
                            ids.add(new WitsmlObjectId(message.getName(), message.getUid(), "message", parentURI, data));
                        }
                        break;
                    case "RIG":
                        ObjRigs rigs = myClient.getRigsAsObj(wellId, wellboreId);
                        if (rigs == null) {
                            continue;
                        }
                        for (ObjRig rig : rigs.getRig()) {
                            if (rig == null)
                                continue;
                            ObjectMapper mapper = new ObjectMapper();
                            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                            String data = mapper.writeValueAsString(rig);
                            ids.add(new WitsmlObjectId(rig.getName(), rig.getUid(), "rig", parentURI, data));
                        }
                        break;
                    case "TRAJECTORY":
                        ObjTrajectorys trajectorys = myClient.getTrajectorysAsObj(wellId, wellboreId);
                        if (trajectorys == null) {
                            continue;
                        }
                        for (ObjTrajectory trajectory: trajectorys.getTrajectory()) {
                            if (trajectory == null)
                                continue;
                            ObjectMapper mapper = new ObjectMapper();
                            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                            String data = mapper.writeValueAsString(trajectory);
                            ids.add(new WitsmlObjectId(trajectory.getName(), trajectory.getUid(), "trajectory", parentURI, data));
                        }
                        break;
                    default:
                        getLogger().error("The Object : " + type + " is not supported/present");
                        break;
                }
            } catch (Exception ex) {
                getLogger().error("Error in getting data from WITSML server");
            }
        }
        return ids;
    }

    @Override
    public Object getObjectData(String wellId, String wellboreId, String objType, String objectId, ObjectRequestTracker objectTracker) {
        objectTracker.initalize(myClient, wellId, wellboreId);
        objectTracker.setVersion(WitsmlVersion.VERSION_1411);
        objectTracker.setObjectId(objectId);
        objectTracker.setObjectType(objType);
        return objectTracker.ExecuteRequest();
    }

    @Override
    public ObjWells getWell(String wellId, String status) {
        ObjWells wells = null;
        try {
            wells = myClient.getWellsAsObj(wellId, status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return wells;
    }

    @Override
    public LogMetadataInfo getLogMetaData(String wellId, String wellboreId, String logId) {
        return null;
    }

    @Override
    public com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWells getWell1311(String wellId, String status) {
        return null;
    }

    @Override
    public ObjWellbores getWellbore(String wellId, String wellboreId) {
        ObjWellbores wellbores = null;
        try {
            wellbores = myClient.getWellboresForWellAsObj(wellId, wellboreId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return wellbores;
    }

    @Override
    public com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellbores getWellbore1311(String wellId, String wellboreId) {
        return null;
    }
}
