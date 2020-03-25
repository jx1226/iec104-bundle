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
package ppcraft.processors.iec104;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.openmuc.j60870.*;
import org.openmuc.j60870.ie.IeQualifierOfSetPointCommand;
import org.openmuc.j60870.ie.IeShortFloat;
import org.openmuc.j60870.ie.IeSingleCommand;
import org.openmuc.j60870.ie.InformationObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SupportsBatching
@Tags({"PutIEC104", "IEC104", "Client", "IEC104Client"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@TriggerSerially
@CapabilityDescription("Create PutIEC104 Client")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})

public class PutIEC104 extends AbstractProcessor {

    public static final PropertyDescriptor IP_PROPERTY = new PropertyDescriptor
            .Builder().name("Server_IP")
            .displayName("Server IP")
            .description("IEC104 Server IP. \n Sample : 192.168.0.1")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT_PROPERTY = new PropertyDescriptor
            .Builder().name("Server_PORT")
            .displayName("Server port")
            .description("IEC104 Server port")
            .required(true)
            .defaultValue("2404")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TAG_PROPERTY = new PropertyDescriptor
            .Builder().name("TAG_PROPERTY")
            .displayName("Attribute tag name")
            .description("Attribute tag name")
            .required(true)
            .defaultValue("tag")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_PROPERTY = new PropertyDescriptor
            .Builder().name("DATA_PROPERTY")
            .displayName("Attribute data name")
            .description("Attribute data name")
            .required(true)
            .defaultValue("data")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("It's okay")
            .build();

    public static final Relationship ERROR = new Relationship.Builder()
            .name("error")
            .description("It's not good")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private volatile Connection connection;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(IP_PROPERTY);
        descriptors.add(PORT_PROPERTY);
        descriptors.add(TAG_PROPERTY);
        descriptors.add(DATA_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(ERROR);
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
    public void onScheduled(final ProcessContext context) throws ProcessException {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            ClientConnectionBuilder clientConnectionBuilder = new ClientConnectionBuilder(context.getProperty(IP_PROPERTY).getValue());
            clientConnectionBuilder.setPort(Integer.parseInt(context.getProperty(PORT_PROPERTY).getValue()));
            connection = clientConnectionBuilder.build();
            int tag = Integer.parseInt(flowFile.getAttribute(context.getProperty(TAG_PROPERTY).getValue()));
            String data = flowFile.getAttribute(context.getProperty(DATA_PROPERTY).getValue());
            if (data.equals("true") || data.equals("false")) {
                connection.singleCommand(65535, CauseOfTransmission.ACTIVATION, tag, new IeSingleCommand(Boolean.parseBoolean(data), 1, false));
            } else {
                connection.setShortFloatCommand(65535, CauseOfTransmission.ACTIVATION, tag, new IeShortFloat(Float.parseFloat(data)), new IeQualifierOfSetPointCommand(1, false));
            }
            session.transfer(flowFile, SUCCESS);
            session.commit();
        } catch (UnknownHostException e) {
            logException(session, context, flowFile, e);
        } catch (IOException e) {
            logException(session, context, flowFile, e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void logException(ProcessSession session, ProcessContext context, FlowFile flowFile, Exception e) {
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        getLogger().error(errors.toString());
        String errorMsg = "{\"tag\":\"" + flowFile.getAttribute(context.getProperty(TAG_PROPERTY).getValue()) + "\", \"data\":\"" + flowFile.getAttribute(context.getProperty(DATA_PROPERTY).getValue()) + "\", \"error\":\"" + errors.toString() + "\", \"timeStamp\":\"" + System.currentTimeMillis() + "\"}";
        session.remove(flowFile);
        session.commit();
        flowFile = session.create();
        flowFile = session.write(flowFile, outputStream -> outputStream.write(errorMsg.getBytes(StandardCharsets.UTF_8)));
        session.transfer(flowFile, ERROR);
        session.commit();
    }

    @OnStopped
    public void onStopped(final ProcessContext context) throws ProcessException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}