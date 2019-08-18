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
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONObject;
import org.openmuc.j60870.*;
import org.openmuc.j60870.ie.IeQualifierOfInterrogation;
import org.openmuc.j60870.ie.InformationElement;
import org.openmuc.j60870.ie.InformationObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@SupportsBatching
@Tags({"GetIEC104", "IEC104", "Client", "IEC104Client"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@CapabilityDescription("Create GetIEC104 Client")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class GetIEC104 extends AbstractProcessor {

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

    public static final PropertyDescriptor FULL_PROPERTY = new PropertyDescriptor
            .Builder().name("Full_survey")
            .displayName("Full survey")
            .description("Send interrogation command")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("It's okay")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private volatile Queue<String> result = new LinkedList<String>();
    private volatile Connection connection;
    private volatile ConnectionEventListener connectionEventListener;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(IP_PROPERTY);
        descriptors.add(PORT_PROPERTY);
        descriptors.add(FULL_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
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
        if (connection == null){
            readIEC(context);
        }
        if (result.isEmpty()){
            return;
        }
        if (connectionEventListener == null){
            stopIEC();
            return;
        }
        while (!result.isEmpty()){
            try {
                String toWrite = result.poll();
                JSONObject jsonObject = new JSONObject(toWrite);
                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, outputStream -> outputStream.write(toWrite.getBytes(StandardCharsets.UTF_8)));
                flowFile = session.putAttribute(flowFile, "tag", jsonObject.getString("tag"));
                flowFile = session.putAttribute(flowFile, "data", jsonObject.getString("data"));
                flowFile = session.putAttribute(flowFile, "controllerTime", jsonObject.getString("controllerTime"));
                session.transfer(flowFile, SUCCESS);
                session.commit();
            } catch (Exception ex){
                getLogger().error(ex.toString());
                stopIEC();
                return;
            }
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) throws ProcessException{
        stopIEC();
        if (!result.isEmpty()){
            result.clear();
        }
    }

    private void readIEC(ProcessContext context){
        String ip = context.getProperty(IP_PROPERTY).getValue();
        int port = Integer.parseInt(context.getProperty(PORT_PROPERTY).getValue());
        try {
            ClientConnectionBuilder clientConnectionBuilder = new ClientConnectionBuilder(ip);
            clientConnectionBuilder.setPort(port);
            connection = clientConnectionBuilder.build();
            connectionEventListener = new ConnectionEventListener() {
                @Override
                public void newASdu(ASdu aSdu) {
                    InformationObject[] informationObject = aSdu.getInformationObjects();
                    if (informationObject != null){
                        int var3 = informationObject.length;
                        int tag;
                        String data = "";
                        String controllerTime = "";
                        long millis;
                        for (int var4 = 0; var4 < var3; ++var4){
                            InformationObject informationObject2 = informationObject[var4];
                            tag = informationObject2.getInformationObjectAddress();
                            InformationElement[][] informationElements = informationObject2.getInformationElements();
                            for (int i = 0; i < informationElements.length; i++){
                                if (informationElements[i].length == 2){
                                    String string1 = informationObject2.getInformationElements()[i][0].toString();
                                    String string2 = informationObject2.getInformationElements()[i][1].toString();
                                    int indexString1 = string1.indexOf(":") + 2;
                                    int index2String1 = string1.indexOf(",");
                                    int indexString2 = string2.indexOf(":") + 2;
                                    data = string1.substring(indexString1, index2String1);
                                    controllerTime = string2.substring(indexString2);
                                } else if (informationElements[i].length == 3){
                                    String string1 = informationObject2.getInformationElements()[i][0].toString();
                                    String string2 = informationObject2.getInformationElements()[i][2].toString();
                                    int indexString1 = string1.indexOf(":") + 2;
                                    int indexString2 = string2.indexOf(":") + 2;
                                    data = string1.substring(indexString1);
                                    controllerTime = string2.substring(indexString2);
                                } else {
                                    if (informationObject2.getInformationObjectAddress() != 0){
                                        String string1 = informationObject2.getInformationElements()[i][0].toString();
                                        int indexString1 = string1.indexOf(":") + 2;
                                        int index2String1 = string1.indexOf(",");
                                        data = string1.substring(indexString1, index2String1);
                                    }
                                }
                            }
                            if (controllerTime.equals("")){
                                Date date = new Date();
                                millis = date.getTime();
                            } else {
                                SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yy hh:mm:ss:SSS");
                                Date date = new Date();
                                try {
                                    date = dateFormat.parse(controllerTime);
                                } catch (ParseException e) {
                                    getLogger().error(e.toString());
                                }
                                millis = date.getTime();
                            }
                            result.offer("{\"tag\":\"" + tag + "\", \"data\":\"" + data + "\", \"controllerTime\":\"" + millis + "\"}");
                            controllerTime = "";
                        }
                    } else {
                        getLogger().error("Private Information");
                    }
                }

                @Override
                public void connectionClosed(IOException e) {
                    getLogger().error(e.toString());
                    if (connection != null){
                        connection = null;
                    }
                }
            };
            if (context.getProperty(FULL_PROPERTY).getValue().equals("true")){
                connection.interrogation(65535, CauseOfTransmission.ACTIVATION, new IeQualifierOfInterrogation(20));
            }
            connection.startDataTransfer(connectionEventListener, 50);
        } catch (UnknownHostException ex){
            getLogger().error(ex.toString());
            stopIEC();
        } catch (IOException ex){
            getLogger().error(ex.toString());
            stopIEC();
        }
    }

    private void stopIEC(){
        if (connection != null){
            connection.close();
            connection = null;
        }
    }
}
