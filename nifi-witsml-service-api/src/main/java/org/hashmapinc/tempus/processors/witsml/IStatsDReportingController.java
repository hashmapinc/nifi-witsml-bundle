package org.hashmapinc.tempus.processors.witsml;

import org.apache.nifi.controller.ControllerService;

public interface IStatsDReportingController extends ControllerService{
    void recordWitsmlQueryTime(int querySeconds);
    void recordNumberOfPointsReceived(int numberOfPoints);
    void recordTimeSpanPerQuery(int pointRangeRecieved);
    void recordPercentToDone(int recordPercentToDone);
    void recordLastTimeProcessed(long lastTimeProcessed);
    void incrementQueryCounter();
}
