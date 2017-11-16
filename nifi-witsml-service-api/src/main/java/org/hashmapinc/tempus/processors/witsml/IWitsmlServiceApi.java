package org.hashmapinc.tempus.processors.witsml;

import com.hashmapinc.tempus.WitsmlObjects.v1411.*;
import com.hashmapinc.tempus.witsml.api.LogRequestTracker;
import com.hashmapinc.tempus.witsml.api.MudlogRequestTracker;
import com.hashmapinc.tempus.witsml.api.ObjectRequestTracker;
import com.hashmapinc.tempus.witsml.api.TrajectoryRequestTracker;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;

/**
 * Created by Chris on 6/2/17.
 */

@Tags({"WITSML Service API"})
@CapabilityDescription("Provides client API for working with WITSML servers")
public interface IWitsmlServiceApi extends ControllerService {
    Object getObject(String wellId, String wellboreId, String object);
    ObjLogs getLogData(String wellId, String wellboreId, String logId, LogRequestTracker logTracker);
    ObjLogs getLogData(String wellId, String wellboreId, String logId, String startDepth, String startTime, String endTime, String endDepth, String timeZone);
    ObjMudLogs getMudLogData(String wellId, String wellboreId, String mudLogId, MudlogRequestTracker mudlogTracker);
    ObjTrajectorys getTrajectoryData(String wellId, String wellboreId, String trajectoryId, String startDepth);
    ObjTrajectorys getTrajectoryData(String wellId, String wellboreId, String trajectoryId, TrajectoryRequestTracker trajectoryTracker);
    List<WitsmlObjectId> getAvailableObjects(String uri, List<String> objectTypes, String wellFilter);
    Object getObjectData(String wellId, String wellboreId, String objType, String objectId, ObjectRequestTracker objectTracker);
    ObjWells getWell(String wellId, String status);
    LogMetadataInfo getLogMetaData(String wellId, String wellboreId, String logId);
    String getUrl();
    com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWells getWell1311(String wellId, String status);

    ObjWellbores getWellbore(String wellId, String wellboreId);

    com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellbores getWellbore1311(String wellId, String wellboreId);
}
