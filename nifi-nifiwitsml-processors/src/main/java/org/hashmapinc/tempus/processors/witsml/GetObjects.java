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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWellbores;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjWells;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;

@Tags({"WITSML", "WitsmlObjects"})
@CapabilityDescription("Get Objects from Witsml Server. Supported Objects : bharuns, cementjob, drillreport," +
        " fluidreport, formationMarker, log, mudLog, message, opsReport, rig, risk, sidewallCore, stimJob," +
        " target, trajectory, tubluar, wbGeometry, well, wellbore")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetObjects extends AbstractProcessor {

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
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OBJECT_LIST = new PropertyDescriptor
            .Builder().name("OBJECT LIST")
            .displayName("Objects")
            .description("Specify the Objects to get from WITSML Server. Can specify multiple comma seperated objects ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_FORMAT = new PropertyDescriptor
            .Builder().name("OUTPUT FORMAT")
            .displayName("Output Format")
            .description("This property will direct the processor to output the data in either nested JSON or flattened JSON.")
            .allowableValues("JSON", "Flattened")
            .defaultValue("Flattened")
            .required(true)
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
        descriptors.add(OBJECT_LIST);
        descriptors.add(OUTPUT_FORMAT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
        relationships.add(FAILURE);
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

        boolean flatten = false;

        if (context.getProperty(OUTPUT_FORMAT).getValue().equals("Flattened")){
            flatten = true;
        }

        try {
            witsmlServiceApi = context.getProperty(WITSML_SERVICE).asControllerService(IWitsmlServiceApi.class);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return;
        }

        String objectList = context.getProperty(OBJECT_LIST).getValue().replaceAll("[;\\s\t]", "").toUpperCase();
        String[] objectArray = objectList.split(",");

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }

        Boolean wellRequested = Arrays.asList(objectArray).contains("WELL");

        Boolean wellboreRequested = Arrays.asList(objectArray).contains("WELLBORE");

        String data = null;
        FlowFile dataFlowFile = session.create(flowFile);
        session.putAttribute(dataFlowFile, "mime.type", "application/json");

        if (wellRequested) {
            com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWells wells = null;
            wells = witsmlServiceApi.getWell1311(context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue(), "");
            try {
                data = mapper.writeValueAsString(wells);
                if (flatten){
                    data = JsonFlattener.flatten(data);
                }

            } catch (JsonProcessingException ex) {
                getLogger().error("Error in converting Wells Object to Json" + ex);
            }
            final String outData = data;
            dataFlowFile = session.write(dataFlowFile, out -> out.write(outData.getBytes()));
            session.transfer(dataFlowFile, SUCCESS);

        } else if (wellboreRequested) {
            com.hashmapinc.tempus.WitsmlObjects.v1311.ObjWellbores wellbores = null;
            wellbores = witsmlServiceApi.getWellbore1311(context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue(),
                                                     context.getProperty(WELLBORE_ID).evaluateAttributeExpressions(flowFile).getValue());
            try {
                data = mapper.writeValueAsString(wellbores);
                if (flatten){
                    data = JsonFlattener.flatten(data);
                }
            } catch (JsonProcessingException ex) {
                getLogger().error("Error in converting Wellbores Object to Json" + ex);
            }
            final String outData = data;

            dataFlowFile = session.write(dataFlowFile, out -> out.write(outData.getBytes()));
            session.transfer(dataFlowFile, SUCCESS);
        } else {
            for (String object : objectArray) {
                data = null;
                FlowFile dataFile = session.create(flowFile);
                Object objData = witsmlServiceApi.getObject(context.getProperty(WELL_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", ""),
                        context.getProperty(WELLBORE_ID).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", ""),
                        object.toUpperCase());
                if (objData == null){
                    session.remove(dataFile);
                    continue;
                }
                try {
                    data = mapper.writeValueAsString(objData);
                    if (flatten){
                        data = JsonFlattener.flatten(data);
                    }
                } catch (JsonProcessingException ex) {
                    getLogger().error("Error in converting Object to Json" + ex);
                }
                dataFile = session.putAttribute(dataFile, "objectType", object.toLowerCase());
                if (data == null) {
                    session.remove(dataFile);
                    continue;
                }
                final String outData = data;
                dataFile = session.write(dataFile, out -> out.write(outData.getBytes()));
                session.transfer(dataFile, SUCCESS);
            }
            session.remove(dataFlowFile);
        }
        session.remove(flowFile);
    }

    private void setMapper() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    }
}
