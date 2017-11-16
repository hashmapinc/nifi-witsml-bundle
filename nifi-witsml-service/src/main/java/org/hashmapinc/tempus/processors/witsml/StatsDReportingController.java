package org.hashmapinc.tempus.processors.witsml;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Tags({"WITSML", "StatsD", "Reporting"})
@CapabilityDescription("Provides session management for Witsml processors")
public class StatsDReportingController extends AbstractControllerService implements IStatsDReportingController {

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
            .Builder().name("Endpoint")
            .description("The endpoint of the StatsD server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port where StatsD is listening")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PREFIX = new PropertyDescriptor
            .Builder().name("Prefix")
            .description("The StatsD prefix for the data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ENDPOINT);
        props.add(PORT);
        props.add(PREFIX);
        properties = Collections.unmodifiableList(props);
    }

    private StatsDClient statsClient;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final ComponentLog logger = getLogger();
        getLogger().info("Creating StatsD Client");
        String endpoint = context.getProperty(ENDPOINT).getValue();
        int port = context.getProperty(PORT).asInteger();
        String prefix = context.getProperty(PREFIX).getValue();
        statsClient = new NonBlockingStatsDClient(prefix, endpoint, port);
    }

    public void recordWitsmlQueryTime(int querySeconds){
        statsClient.recordExecutionTime("QueryResponseTime", querySeconds);
    }

    public void recordNumberOfPointsReceived(int pointsReceived){
        statsClient.recordExecutionTime("QueryPointsReceived", pointsReceived);
    }

    public void recordTimeSpanPerQuery(int pointRangeRecieved){
        statsClient.recordExecutionTime("QueryTimeSpan", pointRangeRecieved);
    }

    public void recordPercentToDone(int percentToDone){
        statsClient.recordExecutionTime("PercentComplete", percentToDone);
    }

    public void recordLastTimeProcessed(long lastTime){
        statsClient.recordGaugeValue("LastTimeProcessed", lastTime);
    }

    public void incrementQueryCounter(){
        statsClient.increment("NumOfQueries");
    }
}
