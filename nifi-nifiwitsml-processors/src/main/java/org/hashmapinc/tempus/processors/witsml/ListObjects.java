package org.hashmapinc.tempus.processors.witsml;

import com.fasterxml.jackson.annotation.JsonInclude;
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
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;

import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
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
@ReadsAttributes({@ReadsAttribute(attribute="uri", description="The URI of the object to list children of.")})
@WritesAttributes({@WritesAttribute(attribute="id", description=""),
        @WritesAttribute(attribute="uri", description="The URI of the object."),
        @WritesAttribute(attribute="wmlObjectType", description="The WITSML object type that was found")
})
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

    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor
            .Builder().name("DISTRIBUTED CACHE SERVICE")
            .displayName("Distributed Cache Service")
            .description("Specifies the service to use for the distributed map cache client for known/unknown status.")
            .identifiesControllerService(DistributedMapCacheClient.class)
            .required(false)
            .build();

    public static final PropertyDescriptor MAINTAIN_QUERY_STATE = new PropertyDescriptor
            .Builder().name("MAINTAIN QUERY STATE")
            .displayName("Maintain Query State")
            .description("Maintain a list of already known objects.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
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
        descriptors.add(DISTRIBUTED_CACHE_SERVICE);
        descriptors.add(MAINTAIN_QUERY_STATE);


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

    private DistributedMapCacheClient otherCacheClient = null;

    private DistributedMapCacheClient logCacheClient = null;


    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile inputFile = session.get();

        boolean trackObjects = context.getProperty(MAINTAIN_QUERY_STATE).asBoolean();

        try {
            final ComponentLog logger = getLogger();
            IWitsmlServiceApi witsmlServiceApi;

            try {
                witsmlServiceApi = context.getProperty(WITSML_SERVICE).asControllerService(IWitsmlServiceApi.class);
            } catch (Exception ex) {
                logger.error("Error getting WITSML Controller Service: " + ex.getMessage());
                session.remove(inputFile);
                return;
            }

            String[] objectTypes = context.getProperty(OBJECT_TYPES).toString().replaceAll("[;\\s\t]", "").split(",");
            String objectType = objectTypes[0];

            if("log".equalsIgnoreCase(objectType)){
                if (logCacheClient == null)
                    logCacheClient = (DistributedMapCacheClient)context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
            }else{
                if (otherCacheClient == null)
                    otherCacheClient = (DistributedMapCacheClient)context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
            }

            String uri;
            if (inputFile != null){
                uri = context.getProperty(PARENT_URI).evaluateAttributeExpressions(inputFile).getValue();
            }

            else{
                uri = context.getProperty(PARENT_URI).evaluateAttributeExpressions().getValue();
            }

            List<WitsmlObjectId> objects = witsmlServiceApi.getAvailableObjects(uri, Arrays.asList(objectTypes), context.getProperty(WELL_STATUS_FILTER).getValue());
            List<FlowFile> outputFiles = new ArrayList<>();

            if(!objects.isEmpty()){
                for (WitsmlObjectId wmlObj : objects) {
                    FlowFile outputFile = session.create();
                    try {
                        session.putAttribute(outputFile, "mime.type", "application/json");
                        session.putAttribute(outputFile, "id", wmlObj.getId());
                        session.putAttribute(outputFile, "uri", wmlObj.getUri());
                        session.putAttribute(outputFile, "wmlObjectType", wmlObj.getType());

                       if (trackObjects){
                            String cacheUri = wmlObj.getUri();
                            boolean known = checkIfObjectKnown(wmlObj.getUri(),wmlObj.getType());

                            if (!known){
                                setObjectKnown(wmlObj.getUri(), wmlObj.getName(),wmlObj.getType());
                            }

                            session.putAttribute(outputFile, "object.known", String.valueOf(known));
                        }

                        outputFile = session.write(outputFile, out -> out.write(wmlObj.getData().getBytes()));
                        outputFiles.add(outputFile);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        getLogger().error("Error processing data for witsml object: " + ex.getMessage());
                        session.remove(outputFile);
                    }
                }

                session.transfer(outputFiles, SUCCESS);

                if (inputFile != null)
                    session.transfer(inputFile, ORIGINAL);
            }else{
              if (inputFile != null)
                    session.transfer(inputFile, ORIGINAL);
           }
        } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("error processing listobject response: " + ex.getMessage());
            session.remove(inputFile);
        }
    }

    private final org.apache.nifi.distributed.cache.client.Serializer<String> keySerializer = new StringSerializer();

    private final org.apache.nifi.distributed.cache.client.Serializer<String> keyDeSerializer = new StringSerializer();
    private final Deserializer<byte[]> valueDeserializer = new ValueDeSerializer();

    private  void setObjectKnown(String uri, String name,String objectType) throws IOException {
        if("log".equalsIgnoreCase(objectType)){
            logCacheClient.put(uri, name, keySerializer, keySerializer);
        }else{
            otherCacheClient.put(uri, name, keySerializer, keySerializer);
        }
    }

    private  boolean checkIfObjectKnown(String uri,String objectType) throws IOException {
        boolean isObjectknown = false;
        if("log".equalsIgnoreCase(objectType)){
            byte[] cachedValue = logCacheClient.get(uri, keySerializer,valueDeserializer);
            if(cachedValue != null){
                isObjectknown = true;
            }

        }else{
            byte[] cachedValue = otherCacheClient.get(uri, keySerializer,valueDeserializer);
            if(cachedValue != null){
                isObjectknown = true;
            }
        }
        return isObjectknown;
    }

    private void setMapper() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    public static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class ValueDeSerializer implements Deserializer<byte[]> {

        @Override
        public byte[] deserialize(final byte[] input) throws DeserializationException, IOException {
            if (input == null || input.length == 0) {
                return null;
            }
            return input;
        }

    }

}
