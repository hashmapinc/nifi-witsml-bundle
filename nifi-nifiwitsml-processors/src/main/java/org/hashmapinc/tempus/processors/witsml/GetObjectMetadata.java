/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hashmapinc.tempus.processors.witsml;

import com.eclipsesource.json.Json;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
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
import org.apache.nifi.processor.util.StandardValidators;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Tags({"WITSML", "WitsmlObjects", "log"})
@CapabilityDescription("Gets growing object metadata for a high frequency GetData processor")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetObjectMetadata extends AbstractProcessor {

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
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OBJECT_ID = new PropertyDescriptor
            .Builder().name("OBJECT ID")
            .displayName("Object ID")
            .description("The Object ID to get the metadata for")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OBJECT_TYPE = new PropertyDescriptor
            .Builder().name("OBJECT TYPE")
            .displayName("Object Type")
            .description("The Object type to get the query for")
            .required(true)
            .allowableValues("log", "trajectory")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_TYPE = new PropertyDescriptor
            .Builder().name("BATCH TYPE")
            .displayName("Batch Type")
            .description("The Object type to get the query for")
            .required(true)
            .allowableValues(BatchDuration.values())
            .defaultValue(BatchDuration.DAY.toString())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Data successfully received from the server")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Data not successfully received from the server")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(WITSML_SERVICE);
        descriptors.add(WELL_ID);
        descriptors.add(WELLBORE_ID);
        descriptors.add(OBJECT_ID);
        descriptors.add(OBJECT_TYPE);
        descriptors.add(BATCH_TYPE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
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
        try {
            final ComponentLog logger = getLogger();
            IWitsmlServiceApi witsmlServiceApi;

            try {
                witsmlServiceApi = context.getProperty(WITSML_SERVICE).asControllerService(IWitsmlServiceApi.class);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
                return;
            }

            FlowFile flowFile = session.get();

            String objectId;
            String wellId;
            String wellboreId;
            String objectType;

            if (flowFile != null) {
                objectId = context.getProperty(OBJECT_ID).getValue();
                wellId = context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue();
                wellboreId = context.getProperty(WELLBORE_ID).evaluateAttributeExpressions(flowFile).getValue();
                objectType = context.getProperty(OBJECT_TYPE).evaluateAttributeExpressions(flowFile).getValue();
            } else {
                objectId = context.getProperty(OBJECT_ID).getValue();
                wellId = context.getProperty(WELL_ID).getValue();
                wellboreId = context.getProperty(WELLBORE_ID).getValue();
                objectType = context.getProperty(OBJECT_TYPE).getValue();
            }

            if (objectType.equals("log")) {
                handleLog(wellId, wellboreId, objectId, flowFile, session, witsmlServiceApi,
                        BatchDuration.valueOf(BatchDuration.class, context.getProperty(BATCH_TYPE).getValue()));
            }
        }
        catch (Exception ex)
        {
            getLogger().error("bad");
        }
    }

    private void handleLog(String wellId, String wellboreId, String logId,
                           FlowFile flowFile, ProcessSession session, IWitsmlServiceApi witsmlService, BatchDuration duration){

        LogMetadataInfo results = witsmlService.getLogMetaData(wellId, wellboreId, logId);
        Object log = Configuration.defaultConfiguration().jsonProvider().parse(results.metadata);
        String indexType = JsonPath.read(log, "$.log[0].indexType");
        if (indexType.contains("TIME")) {
            String startTime = JsonPath.read(log, "$.log[0].startDateTimeIndex").toString();
            String endTime = JsonPath.read(log, "$.log[0].endDateTimeIndex").toString();
            String wellName = JsonPath.read(log, "$.log[0].nameWell").toString();
            List<BatchInfo> batches = computeTimeBatches(startTime, endTime,duration, results.timeZone);
            for (BatchInfo batch : batches){
                createLogBatchFlowFile(session, wellId, wellboreId, logId, results, indexType, batch, wellName,
                        endTime, startTime);
            }
        } else {
            session.putAttribute(flowFile, "endIndex", JsonPath.read(log, "$.log[0].endIndex"));
        }
    }

    private void createLogBatchFlowFile(ProcessSession session, String wellId, String wellboreID, String logId,
                                        LogMetadataInfo results, String indexType, BatchInfo info, String wellName,
                                        String endTime, String startTime){
        FlowFile flowFile = session.create();
        session.putAttribute(flowFile, "wellboreUid", wellboreID);
        session.putAttribute(flowFile, "wellUid", wellId);
        session.putAttribute(flowFile, "uid", logId);
        session.putAttribute(flowFile, "indexType", indexType);
        session.putAttribute(flowFile, "next.query.time", convertTimeString(info.startDate, results.timeZone));
        session.putAttribute(flowFile, "last.query.time", convertTimeString(info.endDate, results.timeZone));
        session.putAttribute(flowFile, "objectType", "log");
        session.putAttribute(flowFile, "mime.type", "application/json");
        session.putAttribute(flowFile, "well.name", wellName);
        session.putAttribute(flowFile, "log.max", endTime);
        session.putAttribute(flowFile, "log.min", startTime);
        session.putAttribute(flowFile,"timeZone", results.timeZone);
        session.putAttribute(flowFile, "batchId", info.batchId);
        flowFile = session.write(flowFile, out -> out.write(results.metadata.getBytes()));
        session.transfer(flowFile, SUCCESS);
    }

    private String convertTimeString(String timeStamp, String timeZone){
        ZonedDateTime zdt = ZonedDateTime.parse(timeStamp);
        ZoneId offset = ZoneId.of(timeZone);
        ZonedDateTime ldt = zdt.withZoneSameInstant(offset);
        return ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS[XXX]"));
    }

    private List<BatchInfo> computeTimeBatches(String startDate, String endDate, BatchDuration duration, String timeZone){
        List<BatchInfo> batches = new ArrayList<>();
        if (duration == BatchDuration.DAY) {
            ZoneId offset = ZoneId.of(timeZone);

            ZonedDateTime startTime = ZonedDateTime.parse(startDate);
            ZonedDateTime convertedStartTime = startTime.withZoneSameInstant(offset);

            ZonedDateTime endTime = ZonedDateTime.parse(endDate);
            ZonedDateTime convertedEndTime = endTime.withZoneSameInstant(offset);

            long dayBatchs = Duration.between(convertedStartTime, convertedEndTime).toDays();
            ZonedDateTime current = convertedStartTime;
            for (int i = 0; i <= dayBatchs; i++) {
                batches.add(getBatchInfo(current, i, timeZone));
                current = current.plus(1, ChronoUnit.DAYS);
            }
        } else {
            ZoneId offset = ZoneId.of(timeZone);

            ZonedDateTime startTime = ZonedDateTime.parse(startDate);
            ZonedDateTime convertedStartTime = startTime.withZoneSameInstant(offset);

            ZonedDateTime endTime = ZonedDateTime.parse(endDate);
            ZonedDateTime convertedEndTime = endTime.withZoneSameInstant(offset);
            BatchInfo info = new BatchInfo();
            info.startDate = convertedStartTime.format(DateTimeFormatter.ofPattern(WitsmlConstants.TIMEZONE_FORMAT));
            info.endDate = convertedEndTime.format(DateTimeFormatter.ofPattern(WitsmlConstants.TIMEZONE_FORMAT));
            info.batchId = "0";
            batches.add(info);
        }

        return batches;
    }

    private BatchInfo getBatchInfo(ZonedDateTime current, int id, String timeZone){
        int year = current.getYear();
        int month = current.getMonthValue();
        int day = current.getDayOfMonth();
        String date = String.format("%04d", year) + "-" + String.format("%02d", month) + "-" +
                String.format("%02d", day);
        String startTime = "00:00:01.000" + timeZone;
        String endTime = "23:59:59.000" + timeZone;
        BatchInfo info = new BatchInfo();
        info.startDate = date + "T" + startTime;
        info.endDate = date + "T" + endTime;
        info.batchId = date;
        return info;
    }

    private ZonedDateTime iso8601toInstant(String input){
        return ZonedDateTime.parse(input);
    }

    class BatchInfo{
        String startDate;
        String endDate;
        String batchId;
    }
}
