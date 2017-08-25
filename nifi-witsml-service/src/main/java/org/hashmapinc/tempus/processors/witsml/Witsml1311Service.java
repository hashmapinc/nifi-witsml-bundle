package org.hashmapinc.tempus.processors.witsml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.hashmapinc.tempus.WitsmlObjects.v1311.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjBhaRun;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjBhaRuns;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjCementJob;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjCementJobs;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjConvCore;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjConvCores;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjFluidsReport;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjFluidsReports;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjFormationMarker;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjFormationMarkers;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjLog;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjLogs;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMessage;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMessages;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLogs;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjOpsReport;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjOpsReports;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjRig;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjRigs;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjRisk;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjRisks;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjSidewallCore;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjSidewallCores;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjSurveyProgram;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjSurveyPrograms;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjTarget;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjTargets;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjTrajectory;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjTrajectorys;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjTubular;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjTubulars;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWbGeometry;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWbGeometrys;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWell;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWellbore;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWellbores;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWells;
import com.hashmapinc.tempus.witsml.api.LogRequestTracker;
import com.hashmapinc.tempus.witsml.api.MudlogRequestTracker;
import com.hashmapinc.tempus.witsml.api.TrajectoryRequestTracker;
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
import com.hashmapinc.tempus.witsml.api.WitsmlVersion;
import com.hashmapinc.tempus.WitsmlObjects.Util.log.LogDataHelper;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.Tuple;

/**
 * Created by Chris on 6/2/17.
 */
@Tags({"WITSML", "Well", "Wellbore", "Log", "Mudlog", "Trajectory", "Version1.3.1.1"})
@CapabilityDescription("Provides session management for Witsml processors")
public class Witsml1311Service extends AbstractControllerService implements IWitsmlServiceApi {

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

        myClient = new Client(context.getProperty(ENDPOINT_URL).getValue().toString());
        myClient.setUserName(context.getProperty(USERNAME).getValue().toString());
        myClient.setPassword(context.getProperty(PASSWORD).getValue().toString());
        myClient.setVersion(WitsmlVersion.VERSION_1311);
        myClient.connect();
        setMapper();
    }

    @Override
    public String getObject(String wellId, String wellboreId, String object) {
        try {
            switch (object) {
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
                case "CONVCORE":
                    ObjConvCores convCores = myClient.getConvCoresAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(convCores);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting ConvCores to Json : " + ex);
                    }
                    break;
                case "DTSINSTALLEDSYSTEM" :
                    ObjDtsInstalledSystems dtsInstalledSystems = myClient.getDtsInstalledSystemsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(dtsInstalledSystems);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting DtsInstalledSystems to Json : " + ex);
                    }
                    break;
                case "DTSMEASUREMENT" :
                    ObjDtsMeasurements dtsMeasurements = myClient.getDtsMeasurementsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(dtsMeasurements);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting DtsMeasurements to Json : " + ex);
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
                case "OPSREPORT":
                    ObjOpsReports opsReports = myClient.getOpsReportsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(opsReports);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting OpsReports to Json : " + ex);
                    }
                    break;
                case "REALTIME" :
                    ObjRealtimes realtimes = myClient.getRealtimesAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(realtimes);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting RealTimes to Json : " + ex);
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
                case "WELLLOG" :
                    ObjWellLogs wellLogs = myClient.getWellLogsAsObj(wellId, wellboreId);
                    try {
                        return mapper.writeValueAsString(wellLogs);
                    } catch (JsonProcessingException ex) {
                        getLogger().error("Error in converting WellLogs to Json : " + ex);
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
        logTracker.setVersion(WitsmlVersion.VERSION_1311);
        logTracker.setLogId(logId);
        logTracker.initalize(myClient, wellId, wellboreId);

        return logTracker.ExecuteRequest();
    }

    @Override
    public ObjMudLogs getMudLogData(String wellId, String wellboreId, String mudLogId) {
        mudLogTracker.setVersion(WitsmlVersion.VERSION_1311);
        mudLogTracker.setMudlogId(mudLogId);
        mudLogTracker.initalize(myClient, wellId, wellboreId);

        return mudLogTracker.ExecuteRequest();
    }

    @Override
    public ObjTrajectorys getTrajectoryData(String wellId, String wellboreId, String trajectoryid) {
        trajectoryTracker.setVersion(WitsmlVersion.VERSION_1311);
        trajectoryTracker.setTrajectoryId(trajectoryid);
        trajectoryTracker.initalize(myClient, wellId, wellboreId);

        return  trajectoryTracker.ExecuteRequest();
    }

    @Override
    public List<WitsmlObjectId> getAvailableObjects(String uri, List<String> objectTypes){
        QueryTarget target = QueryTarget.parseURI(uri, objectTypes);
        List<WitsmlObjectId> ids = new ArrayList<>();
        switch (target.getQueryLevel()){
            case Server: {
                ObjWells wells = getWellData();
                if (wells == null)
                    return null;
                for (ObjWell w:wells.getWell()) {
                    if (w == null)
                        continue;
                    WitsmlObjectId objId = new WitsmlObjectId(w.getName(), w.getUid(), "well", "");
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
                    WitsmlObjectId objId = new WitsmlObjectId(wb.getName(), wb.getUid(), "wellbore", "/" + wb.getNameWell() + "(" + wb.getUidWell() + ")");
                    ids.add(objId);
                }
                break;
            }
            case Wellbore:
                ids = queryForTypes(target.getWell().getId(), target.getWellbore().getId(),target.getObjectsToQuery());
                break;
        }
        return ids;
    }

    private List<WitsmlObjectId> queryForTypes(String wellId, String wellboreId, List<String> types){
        List<WitsmlObjectId> ids = new ArrayList<>();
        
        for (String type : types) {
            try {
                switch (type.toUpperCase()) {
                    case "BHARUN":
                        ObjBhaRuns bhaRuns = myClient.getBhaRunsAsObj(wellId, wellboreId);
                        if (bhaRuns == null) {
                            continue;
                        }
                        for(ObjBhaRun bhaRun: bhaRuns.getBhaRun()) {
                            if (bhaRun == null)
                                continue;
                            ids.add(new WitsmlObjectId(bhaRun.getName(), bhaRun.getUid(), "bhaRun"));
                        }
                        break;
                    case "CEMENTJOB":
                        ObjCementJobs cementJobs = myClient.getCementJobsAsObj(wellId, wellboreId);
                        if (cementJobs == null){
                            continue;
                        }
                        for (ObjCementJob cementJob : cementJobs.getCementJob()) {
                            if (cementJob == null)
                                continue;
                            ids.add(new WitsmlObjectId(cementJob.getName(), cementJob.getUid(), "cementJob"));
                        }
                        break;
                    case "CONVCORE":
                        ObjConvCores convCores = myClient.getConvCoresAsObj(wellId, wellboreId);
                        if (convCores == null){
                            continue;
                        }
                        for (ObjConvCore convCore : convCores.getConvCore()) {
                            if (convCore == null) {
                                continue;
                            }
                            ids.add(new WitsmlObjectId(convCore.getName(), convCore.getUid(), "convCore"));
                        }
                        break;
                    case "DTSINSTALLEDSYSTEM":
                        ObjDtsInstalledSystems dtsInstalledSystems = myClient.getDtsInstalledSystemsAsObj(wellId, wellboreId);
                        if (dtsInstalledSystems == null) {
                            continue;
                        }
                        for (ObjDtsInstalledSystem dtsInstalledSystem : dtsInstalledSystems.getDtsInstalledSystem()) {
                            if (dtsInstalledSystem == null)
                                continue;
                            ids.add(new WitsmlObjectId(dtsInstalledSystem.getName(), dtsInstalledSystem.getUid(), "dtsInstalledSystem"));
                        }
                        break;
                    case "DTSMEASUREMENT":
                        ObjDtsMeasurements dtsMeasurements = myClient.getDtsMeasurementsAsObj(wellId, wellboreId);
                        if (dtsMeasurements == null) {
                            continue;
                        }
                        for (ObjDtsMeasurement dtsMeasurement : dtsMeasurements.getDtsMeasurement()) {
                            if (dtsMeasurement == null)
                                continue;
                            ids.add(new WitsmlObjectId(dtsMeasurement.getName(), dtsMeasurement.getUid(), "dtsMeasurement"));
                        }
                        break;
                    case "FLUIDREPORT":
                        ObjFluidsReports fluidsReports = myClient.getFluidsReportsAsObj(wellId, wellboreId);
                        if (fluidsReports == null){
                            continue;
                        }
                        for (ObjFluidsReport fluidsReport : fluidsReports.getFluidsReport()) {
                            if (fluidsReport == null)
                                continue;
                            ids.add(new WitsmlObjectId(fluidsReport.getName(), fluidsReport.getUid(), "fluidsReport"));
                        }
                        break;
                    case "FORMATIONMARKER":
                        ObjFormationMarkers formationMarkers = myClient.getFormationMarkersAsObj(wellId, wellboreId);
                        if (formationMarkers == null) {
                            continue;
                        }
                        for (ObjFormationMarker formationMarker: formationMarkers.getFormationMarker()) {
                            if (formationMarker == null)
                                continue;
                            ids.add(new WitsmlObjectId(formationMarker.getName(), formationMarker.getUid(), "formationMarker"));
                        }
                        break;
                    case "LOG":
                        ObjLogs logs = myClient.getLogMetadataAsObj(wellId, wellboreId);
                        if (logs == null) {
                            continue;
                        }
                        for (ObjLog log : logs.getLog()) {
                            if (log == null)
                                continue;
                            ids.add(new WitsmlObjectId(log.getName(), log.getUid(), "log"));
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
                            ids.add(new WitsmlObjectId(message.getName(), message.getUid(), "message"));
                        }
                        break;
                    case "MUDLOG":
                        ObjMudLogs mudLogs = myClient.getMudLogsAsObj(wellId, wellboreId);
                        if (mudLogs == null) {
                            continue;
                        }
                        for (ObjMudLog mudLog: mudLogs.getMudLog()) {
                            if (mudLog == null)
                                continue;
                            ids.add(new WitsmlObjectId(mudLog.getName(), mudLog.getUid(), "mudLog"));
                        }
                        break;
                    case "OPSREPORT":
                        ObjOpsReports opsReports = myClient.getOpsReportsAsObj(wellId, wellboreId);
                        if (opsReports == null) {
                            continue;
                        }
                        for (ObjOpsReport opsReport: opsReports.getOpsReport()) {
                            if (opsReport == null)
                                continue;
                            ids.add(new WitsmlObjectId(opsReport.getName(), opsReport.getUid(), "opsReport"));
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
                            ids.add(new WitsmlObjectId(rig.getName(), rig.getUid(), "rig"));
                        }
                        break;
                    case "RISK":
                        ObjRisks risks = myClient.getRisksAsObj(wellId, wellboreId);
                        if (risks == null) {
                            continue;
                        }
                        for (ObjRisk risk : risks.getRisk()) {
                            if (risk == null)
                                continue;
                            ids.add(new WitsmlObjectId(risk.getName(), risk.getUid(), "risk"));
                        }
                        break;
                    case "SIDEWALLCORE":
                        ObjSidewallCores sidewallCores = myClient.getSideWallCoresAsObj(wellId, wellboreId);
                        if (sidewallCores == null) {
                            continue;
                        }
                        for (ObjSidewallCore sidewallCore : sidewallCores.getSidewallCore()) {
                            if (sidewallCore == null)
                                continue;
                            ids.add(new WitsmlObjectId(sidewallCore.getName(), sidewallCore.getUid(), "sidewallCore"));
                        }
                        break;
                    case "SURVEYPROGRAM":
                        ObjSurveyPrograms surveyPrograms = myClient.getSurveyProgramsAsObj(wellId, wellboreId);
                        if (surveyPrograms == null) {
                            continue;
                        }
                        for (ObjSurveyProgram surveyProgram: surveyPrograms.getSurveyProgram()) {
                            if (surveyProgram == null)
                                continue;
                            ids.add(new WitsmlObjectId(surveyProgram.getName(), surveyProgram.getUid(), "surveyProgram"));
                        }
                        break;
                    case "TARGET":
                        ObjTargets targets = myClient.getTargetsAsObj(wellId, wellboreId);
                        if (targets == null) {
                            continue;
                        }
                        for (ObjTarget target : targets.getTarget()) {
                            if (target == null)
                                continue;
                            ids.add(new WitsmlObjectId(target.getName(), target.getUid(), "target"));
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
                            ids.add(new WitsmlObjectId(trajectory.getName(), trajectory.getUid(), "trajectory"));
                        }
                        break;
                    case "TUBULAR":
                        ObjTubulars tubulars = myClient.getTubularsAsObj(wellId, wellboreId);
                        if (tubulars == null) {
                            continue;
                        }
                        for (ObjTubular tubular: tubulars.getTubular()) {
                            if (tubular == null)
                                continue;
                            ids.add(new WitsmlObjectId(tubular.getName(), tubular.getUid(), "tubular"));
                        }
                        break;
                    case "WBGEOMETRY":
                        ObjWbGeometrys wbGeometrys = myClient.getWbGeometrysAsObj(wellId, wellboreId);
                        if (wbGeometrys == null) {
                            continue;
                        }
                        for (ObjWbGeometry wbGeometry: wbGeometrys.getWbGeometry()) {
                            if (wbGeometry == null)
                                continue;
                            ids.add(new WitsmlObjectId(wbGeometry.getName(), wbGeometry.getUid(), "wbGeometry"));
                        }
                        break;
                    case "WELLLOG":
                        ObjWellLogs wellLogs = myClient.getWellLogsAsObj(wellId, wellboreId);
                        if (wellLogs == null) {
                            continue;
                        }
                        for (ObjWellLog wellLog : wellLogs.getWellLog()) {
                            if (wellLog == null)
                                continue;
                            ids.add(new WitsmlObjectId(wellLog.getName(), wellLog.getUid(), "wellLog"));
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

    private void setMapper() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    }
}
