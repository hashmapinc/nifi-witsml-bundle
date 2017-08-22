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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"WITSML", "WitsmlObjects"})
@CapabilityDescription("Get Objects from Witsml Server. Supported Objects : bharuns, cementjob, drillreport," +
        " fluidreport, formationMarker, log, mudLog, message, opsReport, rig, risk, sidewallCore, stimJob," +
        " target, trajectory, tubluar, wbGeometry, well, wellbore")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetObjects extends AbstractProcessor {

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
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WELLBORE_ID = new PropertyDescriptor
            .Builder().name("WELLBORE ID")
            .displayName("Wellbore ID")
            .description("Specify the Wellbore Id")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OBJECT_LIST = new PropertyDescriptor
            .Builder().name("OBJECT LIST")
            .displayName("Objects")
            .description("Specify the Objects to get from WITSML Server. Can specify multiple comma seperated objects ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Data successfully received from the server")
            .build();

    public static final Relationship FAIULURE = new Relationship.Builder()
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
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
        relationships.add(FAIULURE);
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

        final ComponentLog logger = getLogger();
        IWitsmlServiceApi witsmlServiceApi;

        try {
            witsmlServiceApi = context.getProperty(WITSML_SERVICE).asControllerService(IWitsmlServiceApi.class);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return;
        }

        String objectList = context.getProperty(OBJECT_LIST).getValue().replaceAll("[;\\s\t]", "").toUpperCase();
        String[] objectArray = objectList.split(",");

        for (String object : objectArray) {
            String data = witsmlServiceApi.getObject(context.getProperty(WELL_ID).getValue().toString().replaceAll("[;\\s\t]", ""),
                    context.getProperty(WELLBORE_ID).getValue().toString().replaceAll("[;\\s\t]", ""),
                    object.toUpperCase());

            FlowFile flowFile = session.create();
            if (flowFile == null) {
                return;
            }
            try {
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(data.getBytes());
                    }
                });
                session.transfer(flowFile, SUCCESS);
            } catch (ProcessException ex) {
                logger.error("Unable to Process : " + ex);
                session.transfer(flowFile, FAIULURE);
            }
        }
    }
}
