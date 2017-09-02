package org.hashmapinc.tempus.processors.witsml;

import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjLogs;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLogs;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjTrajectorys;
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
    String getObject(String wellId, String wellboreId, String object);
    ObjLogs getLogData(String wellId, String wellboreId, String logId, LogRequestTracker logTracker);
    ObjMudLogs getMudLogData(String wellId, String wellboreId, String mudLogId, MudlogRequestTracker mudlogTracker);
    ObjTrajectorys getTrajectoryData(String wellId, String wellboreId, String trajectoryId, TrajectoryRequestTracker trajectoryTracker);
    List<WitsmlObjectId> getAvailableObjects(String uri, List<String> objectTypes);
    Object getObjectData(String wellId, String wellboreId, String objType, String objectId, ObjectRequestTracker objectTracker);
}
