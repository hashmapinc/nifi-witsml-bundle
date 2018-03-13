package org.hashmapinc.tempus.processors.witsml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hashmapinc.tempus.WitsmlObjects.Util.WitsmlMarshal;
import com.hashmapinc.tempus.WitsmlObjects.Util.WitsmlVersionTransformer;
import com.hashmapinc.tempus.WitsmlObjects.v1411.*;
import com.hashmapinc.tempus.witsml.api.*;
import com.hashmapinc.tempus.witsml.client.Client;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import javax.xml.bind.JAXBException;
import javax.xml.transform.TransformerException;
import java.io.*;
import java.rmi.RemoteException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Chris on 6/2/17.
 */
@Tags({"WITSML", "Well", "Wellbore", "baseLogQuery", "Mudlog", "Trajectory", "Version1.3.1.1"})
@CapabilityDescription("Provides session management for Witsml processors")
public class Witsml1311Service extends AbstractControllerService implements IWitsmlServiceApi {

    // Global session variables used by all processors using an instance
    private Client myClient = null;
    private String baseLogQuery = "";
    private String baseLogMetadataQuery = "";
    private String baseTrajQuery = "";

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
        myClient.setVersion(WitsmlVersion.VERSION_1311);
        myClient.connect();
    }

    @Override
    public Object getObject(String wellId, String wellboreId, String object) {
        try {
            switch (object) {
                case "BHARUN":
                    return myClient.getBhaRunsAsObj(wellId, wellboreId);
                case "CEMENTJOB":
                    return myClient.getCementJobsAsObj(wellId, wellboreId);
                case "CONVCORE":
                    return myClient.getConvCoresAsObj(wellId, wellboreId);
                case "DTSINSTALLEDSYSTEM" :
                    return myClient.getDtsInstalledSystemsAsObj(wellId, wellboreId);
                case "DTSMEASUREMENT" :
                    return myClient.getDtsMeasurementsAsObj(wellId, wellboreId);
                case "FLUIDREPORT":
                    return myClient.getFluidsReportsAsObj(wellId, wellboreId);
                case "FORMATIONMARKER":
                    return myClient.getFormationMarkersAsObj(wellId, wellboreId);
                case "LOG":
                    return myClient.getLogMetadataAsObj(wellId, wellboreId);
                case "MESSAGE": {
                    if (wellId.equals("")) return null;
                    if (wellboreId.equals("")) return null;
                    String messages = myClient.getMessages(wellId, wellboreId);
                    if (messages == null) return null;
                    if (messages.equals("")) return null;
                    return WitsmlMarshal.deserialize(messages, com.hashmapinc.tempus.WitsmlObjects.v1311.ObjMessages.class);
                }
                case "MUDLOG":
                    return myClient.getMudLogsAsObj(wellId, wellboreId);
                case "OPSREPORT":
                    return myClient.getOpsReportsAsObj(wellId, wellboreId);
                case "REALTIME" :
                    return myClient.getRealtimesAsObj(wellId, wellboreId);
                case "RIG": {
                    if (wellId.equals("")) return null;
                    if (wellboreId.equals("")) return null;
                    String rigs =  myClient.getRigs(wellId, wellboreId);
                    if (rigs == null) return null;
                    if (rigs.equals("")) return null;
                    return WitsmlMarshal.deserialize(rigs, com.hashmapinc.tempus.WitsmlObjects.v1311.ObjRigs.class);
                }
                case "RISK":
                    return myClient.getRisksAsObj(wellId, wellboreId);
                case "SIDEWALLCORE":
                    return myClient.getSideWallCoresAsObj(wellId, wellboreId);
                case "SURVEYPROGRAM":
                    return myClient.getSurveyProgramsAsObj(wellId, wellboreId);
                case "TARGET":
                    return myClient.getTargetsAsObj(wellId, wellboreId);
                case "TRAJECTORY": {
                    if (wellId.equals("")) return null;
                    if (wellboreId.equals("")) return null;
                    String trajs = myClient.getTrajectorys(wellId, wellboreId);
                    if (trajs == null) return null;
                    if (trajs.equals("")) return null;
                    return WitsmlMarshal.deserialize(trajs, com.hashmapinc.tempus.WitsmlObjects.v1311.ObjTrajectorys.class);
                }
                case "TUBULAR":
                    return myClient.getTubularsAsObj(wellId, wellboreId);
                case "WBGEOMETRY":
                    return myClient.getWbGeometrysAsObj(wellId, wellboreId);
                case "WELLLOG" :
                    return myClient.getWellLogsAsObj(wellId, wellboreId);
                default:
                    getLogger().error("The Object : " + object + " is not supported/present");
                    break;
            }
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            getLogger().error("Error in getting data from WITSML server (Witsml1311Service::getObject): " + ex.getMessage() + System.lineSeparator() + exceptionAsString);
        }
        return null;
    }

    @Override
    public ObjLogs getLogData(String wellId, String wellboreId, String logId, String startDepth, String startTime, String endTime, String endDepth, String timeZone){

        // Create Query
        String query = "";

        if (startDepth == null)
            startDepth = "";

        if (startTime == null)
            startTime = "";

        if (baseLogQuery.equals("")) {
            try {
                baseLogQuery = getQuery("/1311/GetLogDataQuery.xml");
            } catch (IOException e) {
                getLogger().error("Error reading base log query from /1311/GetLogData.xml: in GetData" + e.getMessage());
            }
        }

        query = baseLogQuery;
        query = query.replace("%uidWell%", wellId);
        query = query.replace("%uidWellbore%", wellboreId);
        query = query.replace("%uidLog%", logId);
        query = query.replace("%startIndex%", startDepth);
        query = query.replace("%startDateTimeIndex%", startTime);
        query = query.replace("%endDateTimeIndex%", removeTimeZone(endTime));
        query = query.replace("%endIndex%", endDepth);

        // Execute query to the server
        String returnedLogData = "";
        try {
            returnedLogData = myClient.executeLogQuery(query, "","");
        } catch (RemoteException e) {
            getLogger().error("Error executing GetFromStoreQuery in getLogData for Witsml1311Service: " + e.getMessage());
            return null;
        }

        if (returnedLogData == null)
            return null;
        if (returnedLogData.equals(""))
            return null;

        // Convert to 1.4.1.1 to be able to use the helper methods
        String convertedLogData = "";

        try {
            WitsmlVersionTransformer transformer = new WitsmlVersionTransformer();
            convertedLogData = transformer.convertVersion(returnedLogData);
        } catch (TransformerException e) {
            getLogger().error("Could not convert WITSML 1.3.1.1 response to 1.4.1.1");
            return null;
        }

        // Deserialize the object
        ObjLogs returnedLog = new ObjLogs();

        try {
            if (convertedLogData == null) return null;
            if (convertedLogData.equals("")) return null;
            returnedLog = WitsmlMarshal.deserialize(convertedLogData, ObjLogs.class);
        } catch (JAXBException e) {
            getLogger().error("Could not deserialize object in getLogData for the Witsml1311Service: " + e.getMessage());
            return null;
        }

        return returnedLog;
    }

    private String removeTimeZone(String timeStamp){
    	try {
        ZonedDateTime zdt = ZonedDateTime.parse(timeStamp, DateTimeFormatter.ofPattern(WitsmlConstants.TIMEZONE_FORMAT));
        return zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
    	} catch (Exception ex) {
    		System.out.println(timeStamp);
    	}
    	return "";
    }

    private String getQuery(String resourcePath) throws IOException {
        InputStream stream = getClass().getResourceAsStream(resourcePath);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(stream));
        return reader.lines().collect(Collectors.joining(
                System.getProperty("line.separator")));
    }

    @Override
    public ObjTrajectorys getTrajectoryData(String wellId, String wellboreId, String trajectoryId, String startDepth) {
        // Create Query
        String query = "";
        if (baseTrajQuery.equals("")) {
            try {
                baseTrajQuery = getQuery("/1311/GetTrajectoryData.xml");
            } catch (IOException e) {
                getLogger().error("Error reading base log query from /1311/GetLogData.xml: in GetData" + e.getMessage());
            }
        } else {
            query = baseTrajQuery;
            query = query.replace("%uidWell%", wellId);
            query = query.replace("%uidWellbore%", wellboreId);
            query = query.replace("%uidTrajectory%", trajectoryId);
            query = query.replace("%mdMn%", startDepth);
        }

        // Execute query to the server
        String returnedTrajectoryData = "";

        try {
            returnedTrajectoryData = myClient.executeTrajectoryQuery(query, "","");
        } catch (RemoteException e) {
            getLogger().error("Error executing GetFromStoreQuery in getTrajectoryData for Witsml1311Service: " + e.getMessage());
            return null;
        }

        if (returnedTrajectoryData == null)
            return null;
        if (returnedTrajectoryData.equals(""))
            return null;

        // Convert to 1.4.1.1 to be able to use the helper methods
        String convertedTrajectoryData = "";

        try {
            WitsmlVersionTransformer transformer = new WitsmlVersionTransformer();
            convertedTrajectoryData = transformer.convertVersion(returnedTrajectoryData);
        } catch (TransformerException e) {
            getLogger().error("Could not convert WITSML 1.3.1.1 response to 1.4.1.1");
            return null;
        }

        // Deserialize the object
        ObjTrajectorys returnedTrajectory = new ObjTrajectorys();

        try {
            returnedTrajectory = WitsmlMarshal.deserialize(convertedTrajectoryData, ObjTrajectorys.class);
        } catch (JAXBException e) {
            getLogger().error("Could not deserialize object in getTrajectoryData for the Witsml1311Service: " + e.getMessage());
            return null;
        }

        return returnedTrajectory;
    }

    @Override
    public List<WitsmlObjectId> getAvailableObjects(String uri, List<String> objectTypes, String wellFilter){
        QueryTarget target = null;
        try {
            target = QueryTarget.parseURI(uri, objectTypes);
        } catch (ArrayIndexOutOfBoundsException ex){
            getLogger().error("Error parsing URI: " + uri + " in getAvailableObjects");
        }

        if (target == null)
            return new ArrayList<>();

        List<WitsmlObjectId> ids = new ArrayList<>();
        switch (target.getQueryLevel()){
            case Server: {
                com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWells wells =
                        getWell1311("", wellFilter);

                if (wells == null)
                    return null;
                for (com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWell w:wells.getWell()) {
                    if (w == null)
                        continue;
                    LocalDateTime timeChanged = null;
                    if (w.getCommonData() != null)
                        timeChanged = w.getCommonData().getDTimLastChange().toGregorianCalendar().toZonedDateTime().toLocalDateTime();
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                    String data = null;
                    try {
                        data = mapper.writeValueAsString(w);
                    } catch (JsonProcessingException e) {
                        getLogger().error("Could not process well metadata");
                    }
                    WitsmlObjectId objId = new WitsmlObjectId(w.getName(), w.getUid(), "well", "", timeChanged, data);

                    ids.add(objId);
                }
                break;
            }
            case Well: {
                try {
                    com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellbores wellbores =
                            getWellboreData(target.getWell());
                    if (wellbores == null)
                        return null;
                    for (com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellbore wb : wellbores.getWellbore()) {
                        if (wb == null)
                            continue;

                        LocalDateTime timeChanged = null;
                        if (wb.getCommonData() != null)
                            timeChanged = wb.getCommonData().getDTimLastChange().toGregorianCalendar().toZonedDateTime().toLocalDateTime();
                        ObjectMapper mapper = new ObjectMapper();
                        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                        String data = mapper.writeValueAsString(wb);
                        WitsmlObjectId objId = new WitsmlObjectId(wb.getName(), wb.getUid(), "wellbore", "/" + wb.getNameWell() + "(" + wb.getUidWell() + ")", timeChanged, data);

                        ids.add(objId);
                    }
                } catch (Exception ex){
                    getLogger().error("Error in getAvailableObjects: " + ex.getMessage());
                }
                break;
            }
            case Wellbore:
                ids = queryForTypes(target);
                break;
        }
        return ids;
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
                            LocalDateTime timeChanged = null;
                            try { timeChanged=log.getCommonData().getDTimLastChange().toGregorianCalendar().toZonedDateTime().toLocalDateTime();} catch (Exception npe) {}
                            ObjectMapper mapper = new ObjectMapper();
                            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                            String data = mapper.writeValueAsString(log);
                            ids.add(new WitsmlObjectId(log.getName(), log.getUid(), "log", parentURI, timeChanged, data));
                        }
                        break;
                    case "MESSAGE":
                        String messageXml = myClient.getMessages(wellId, wellboreId);
                        if (messageXml == null) {
                            continue;
                        }
                        com.hashmapinc.tempus.WitsmlObjects.v1311.ObjMessages messages =
                                WitsmlMarshal.deserialize(messageXml,
                                        com.hashmapinc.tempus.WitsmlObjects.v1311.ObjMessages.class);
                        for (com.hashmapinc.tempus.WitsmlObjects.v1311.ObjMessage message : messages.getMessage()) {
                            if (message == null)
                                continue;
                            ObjectMapper mapper = new ObjectMapper();
                            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                            String data = mapper.writeValueAsString(message);
                            ids.add(new WitsmlObjectId(message.getName(), message.getUid(), "message", parentURI, null, data));
                        }
                        break;
                    case "RIG":
                        String rigsXml = myClient.getRigs(wellId, wellboreId);

                        com.hashmapinc.tempus.WitsmlObjects.v1311.ObjRigs rigs =
                                WitsmlMarshal.deserialize(rigsXml,
                                        com.hashmapinc.tempus.WitsmlObjects.v1311.ObjRigs.class);

                        if (rigs == null) {
                            continue;
                        }

                        if (rigs.getRig().size() == 0) {
                            continue;
                        }

                        for (com.hashmapinc.tempus.WitsmlObjects.v1311.ObjRig rig : rigs.getRig()) {
                            if (rig == null)
                                continue;
                            LocalDateTime timeChanged = rig.getCommonData().getDTimLastChange().toGregorianCalendar().toZonedDateTime().toLocalDateTime();
                            ObjectMapper mapper = new ObjectMapper();
                            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                            String data = mapper.writeValueAsString(rig);
                            ids.add(new WitsmlObjectId(rig.getName(), rig.getUid(), "rig", parentURI, timeChanged, data));
                        }
                        break;
                    case "TRAJECTORY":
                        String trajectorysXml = myClient.getTrajectorys(wellId, wellboreId);
                        com.hashmapinc.tempus.WitsmlObjects.v1311.ObjTrajectorys trajectorys =
                                WitsmlMarshal.deserialize(trajectorysXml,
                                        com.hashmapinc.tempus.WitsmlObjects.v1311.ObjTrajectorys.class);
                        if (trajectorys == null) {
                            continue;
                        }

                        if (trajectorys.getTrajectory().size() == 0) {
                            continue;
                        }

                        for (com.hashmapinc.tempus.WitsmlObjects.v1311.ObjTrajectory trajectory: trajectorys.getTrajectory()) {
                            if (trajectory == null)
                                continue;
                            //LocalDateTime timeChanged = trajectory.getCommonData().getDTimLastChange().toGregorianCalendar().toZonedDateTime().toLocalDateTime();
                            ObjectMapper mapper = new ObjectMapper();
                            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                            String data = mapper.writeValueAsString(trajectory);
                            ids.add(new WitsmlObjectId(trajectory.getName(), trajectory.getUid(), "trajectory", parentURI, data));
                        }
                        break;
                }
            } catch (Exception ex) {
                getLogger().error("Error in getting data from WITSML server (Witsml1311Service::queryForTypes): " + ex.getMessage());
            }
        }
        return ids;
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
        String query = "";
        if (baseLogMetadataQuery.equals("")) {
            try {
                baseLogMetadataQuery = getQuery("/1311/GetLogMetadataQuery.xml");
            } catch (IOException e) {
                getLogger().error("Error reading base log query from /1311/GetLogMetadataQuery.xml: in getLogMetaData" + e.getMessage());
            }
        }

        query = baseLogMetadataQuery;
        query = query.replace("%uidWell%", wellId);
        query = query.replace("%uidWellbore%", wellboreId);
        query = query.replace("%uid%", logId);


        String result = "";
        try {
            result = myClient.executeLogQuery(query, "", "");
        } catch (RemoteException e) {
            getLogger().error("Error querying server for log metadata. " + e.getMessage());
        }

        if (result.equals(""))
            return null;

        com.hashmapinc.tempus.WitsmlObjects.v1311.ObjLogs logs = null;

        try {
            logs = WitsmlMarshal.deserialize(result, com.hashmapinc.tempus.WitsmlObjects.v1311.ObjLogs.class);
        } catch (JAXBException e) {
            getLogger().error("Error deserialing log metadata response from server. " + e.getMessage());
            return null;
        }
        LogMetadataInfo info = new LogMetadataInfo();
        int zone = 0;	//Default timezone?
        try {zone = (logs.getLog().get(0).getStartDateTimeIndex().getTimezone());} catch (Exception ignrEx) {}

        getLogger().debug(getTimeZone(zone));
        info.timeZone = getTimeZone(zone);

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setDateFormat(WitsmlConstants.getSimpleDateTimeFormat(info.timeZone));
        String jsonResult = null;

        try {
            jsonResult = mapper.writeValueAsString(logs);
        } catch (JsonProcessingException e) {
            getLogger().error("Error serializing log metadata to JSON. " + e.getMessage());
        }

        info.metadata = jsonResult;
        return info;
    }

    @Override
    public String getUrl() {
        return myClient.getUrl();
    }

    @Override
    public com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWells getWell1311(String wellId, String status) {
        String wellsXml = null;
        com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWells wells = null;
        try {
            wellsXml = myClient.getWells(wellId, status);
            wells = WitsmlMarshal.deserialize(wellsXml, com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWells.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return wells;
    }

    private String getTimeZone(int timeZoneMinutesOffset){

        float hoursOffset = timeZoneMinutesOffset / 60;

        float partialHour = hoursOffset % 1;

        String hours = ((hoursOffset < 0) ? "-" : "") + String.format("%02d", Math.abs((int)hoursOffset));

        if (partialHour == 0){
            return hours + ":00";
        } else {
            String minutes = String.format("%02f", (60 * partialHour));
            return hours + ":" + minutes;
        }
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
        com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellbores wellbores = null;

        try {
            String wellboresXml = myClient.getWellboresForWell(wellId, wellboreId);
            wellbores = WitsmlMarshal.deserialize(wellboresXml,
                    com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellbores.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return wellbores;
    }

    private com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellbores getWellboreData(WitsmlObjectId well){
        try {
            String wellbores =  myClient.getWellboresForWell(well.getId());
            return WitsmlMarshal.deserialize(wellbores, com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellbores.class);
        } catch (Exception e) {
            getLogger().error("Error in getWellbores: " + e.getMessage());
            return null;
        }
    }
}
