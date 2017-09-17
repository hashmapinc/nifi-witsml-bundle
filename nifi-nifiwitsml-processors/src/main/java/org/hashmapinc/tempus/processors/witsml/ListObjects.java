package org.hashmapinc.tempus.processors.witsml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Chris on 6/2/17.
 */
@Tags({"WITSML", "LOG", "MUDLOG", "TRAJECTORY"})
@CapabilityDescription("Get Data from Witsml Server for Objects Log, Mudlog and Trajectory.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ListObjects extends AbstractProcessor {

    private static ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor WITSML_SERVICE = new PropertyDescriptor
            .Builder().name("WITSML SERVICE")
            .displayName("WITSML Service")
            .description("The service to be used to connect to the server.")
            .required(true)
            .identifiesControllerService(IWitsmlServiceApi.class)
            .build();

    public static final PropertyDescriptor PARENT_URI = new PropertyDescriptor
            .Builder().name("PARENT URI")
            .displayName("Parent URI")
            .description("Specify the parent node to look for object types. The top level is / and only wells are " +
                    "available. Only wellbores are available under wells. The format" +
                    "for this property is /wellName(wellId)/wellboreName(wellboreId). You can only subscribe " +
                    "to a server, a well, or a wellbore.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OBJECT_TYPES = new PropertyDescriptor
            .Builder().name("OBJECT TYPES")
            .displayName("Object Types")
            .description("Specify the types of objects to query for. Use * for all supported objects.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WELL_STATUS_FILTER = new PropertyDescriptor
            .Builder().name("WELL STATUS FILTER")
            .displayName("Well Status Filter")
            .description("Specifies the wellStatus filter to be used when querying for wells.")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful Query to Server")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed query to server")
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("Original")
            .description("The original flowfile that lead to the result.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(WITSML_SERVICE);
        descriptors.add(PARENT_URI);
        descriptors.add(OBJECT_TYPES);
        descriptors.add(WELL_STATUS_FILTER);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships.add(ORIGINAL);
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

        FlowFile inputFile = session.get();
        FlowFile outputFile;
        if (inputFile == null)
            outputFile = session.create();
        else {
            outputFile = session.create(inputFile);
        }

        try {
            final ComponentLog logger = getLogger();
            IWitsmlServiceApi witsmlServiceApi;

            try {
                witsmlServiceApi = context.getProperty(WITSML_SERVICE).asControllerService(IWitsmlServiceApi.class);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
                return;
            }

            session.putAttribute(outputFile, "mime.type", "application/json");

            String[] objectTypes = context.getProperty(OBJECT_TYPES).toString().replaceAll("[;\\s\t]", "").split(",");

            String uri;
            if (inputFile != null)
                uri = context.getProperty(PARENT_URI).evaluateAttributeExpressions(inputFile).getValue();
            else
                uri = context.getProperty(PARENT_URI).evaluateAttributeExpressions().getValue();

            List<WitsmlObjectId> objects = witsmlServiceApi.getAvailableObjects(uri, Arrays.asList(objectTypes), context.getProperty(WELL_STATUS_FILTER).getValue());

            String data;

            try {
                if (objects.size() == 0) {
                    session.remove(outputFile);
                    if (inputFile != null)
                        session.transfer(inputFile, ORIGINAL);
                    return;
                }
                data = mapper.writeValueAsString(objects);
            } catch (JsonProcessingException e) {
                getLogger().error("Error converting objects to JSON in ListObject: " + e.getMessage());
                if (inputFile != null)
                    session.transfer(inputFile, FAILURE);
                session.remove(outputFile);
                return;
            }

            if (data == null) {
                session.remove(outputFile);
                if (inputFile != null)
                    session.transfer(inputFile, ORIGINAL);
                return;
            }

            final String jsonData = data;

            if (jsonData.equals("")) {
                session.remove(outputFile);
                if (inputFile != null)
                    session.transfer(inputFile, ORIGINAL);
                return;
            }

            outputFile = session.write(outputFile, out -> out.write(jsonData.getBytes()));
            session.transfer(outputFile, SUCCESS);
            if (inputFile != null)
                session.transfer(inputFile, ORIGINAL);
        }catch (Exception ex){
            getLogger().error("Error getting objects in ListObjects" + ex.getMessage() + System.lineSeparator() + ex.getStackTrace());
            session.remove(outputFile);
            if (inputFile != null)
                session.transfer(inputFile, FAILURE);
        }
    }

    private void setMapper() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    }
}
