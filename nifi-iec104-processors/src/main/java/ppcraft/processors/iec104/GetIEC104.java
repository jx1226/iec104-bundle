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
import java.io.PrintWriter;
import java.io.StringWriter;
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
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})

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

    public static final PropertyDescriptor DATE_FORMAT_PROPERTY = new PropertyDescriptor
            .Builder().name("DATE_FORMAT")
            .displayName("Date format")
            .description("Date format on controller")
            .required(true)
            .defaultValue("dd-MM-yy hh:mm:ss:SSS")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMEZONE_PROPERTY = new PropertyDescriptor
            .Builder().name("TIMEZONE")
            .displayName("TIMEZONE")
            .description("TIMEZONE")
            .required(true)
            .defaultValue("UTC+3")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("It's okay")
            .build();

    public static final Relationship BADCONNECT = new Relationship.Builder()
            .name("bad connect")
            .description("It's not good(problem from connect)")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private volatile Connection connection;
    private volatile ConnectionEventListener connectionEventListener;
    private volatile JSONObject jsonObject;
    private volatile boolean fault;
    private FlowFile flowFile;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(IP_PROPERTY);
        descriptors.add(PORT_PROPERTY);
        descriptors.add(FULL_PROPERTY);
        descriptors.add(DATE_FORMAT_PROPERTY);
        descriptors.add(TIMEZONE_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(BADCONNECT);
        this.relationships = Collections.unmodifiableSet(relationships);
        fault = false;
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
        if (connection == null) {
            readIEC(context, session);
        }
        if (connectionEventListener == null) {
            stopIEC();
            return;
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) throws ProcessException {
        stopIEC();
    }

    private synchronized void writeNext(ProcessSession session, String msg) {
        try {
            flowFile = session.create();
            jsonObject = new JSONObject(msg);
            flowFile = session.write(flowFile, outputStream -> outputStream.write(msg.getBytes(StandardCharsets.UTF_8)));
            flowFile = session.putAttribute(flowFile, "tag", jsonObject.getString("tag"));
            flowFile = session.putAttribute(flowFile, "data", jsonObject.getString("data"));
            flowFile = session.putAttribute(flowFile, "controllerTime", jsonObject.getString("controllerTime"));
            session.transfer(flowFile, SUCCESS);
            session.commit();
        } catch (Exception e) {
            if (fault) {
                getLogger().error("message -> " + msg + "; " + e.toString());
                fault = false;
            }
            return;
        }
    }

    private void readIEC(ProcessContext context, ProcessSession session) {
        String ip = context.getProperty(IP_PROPERTY).getValue();
        int port = Integer.parseInt(context.getProperty(PORT_PROPERTY).getValue());
        try {
            ClientConnectionBuilder clientConnectionBuilder = new ClientConnectionBuilder(ip);
            clientConnectionBuilder.setPort(port);
            connection = clientConnectionBuilder.build();
            connectionEventListener = new ConnectionEventListener() {
                @Override
                public void newASdu(ASdu aSdu) {
                    if (!fault) {
                        fault = true;
                    }
                    InformationObject[] informationObject = aSdu.getInformationObjects();
                    if (informationObject != null) {
                        int var3 = informationObject.length;
                        //check "doubles". If iec-server sends the old value to the same ASdu, and it is not needed. Uncomment lines 208-217, 223, 276.
//                        Set<Integer> numberDouble = new HashSet<>();
//                        for (int var4 = 0; var4 < var3; ++var4){
//                            int testir = informationObject[var4].getInformationObjectAddress();
//                            for (int var5 = var4 + 1; var5 < var3; ++var5){
//                                int testirSec = informationObject[var5].getInformationObjectAddress();
//                                if (testir ==testirSec){
//                                    numberDouble.add(var4);
//                                }
//                            }
//                        }
                        int tag;
                        String data = "";
                        String controllerTime = "";
                        long millis;
                        for (int var4 = 0; var4 < var3; ++var4) {
//                            if (!numberDouble.contains(var4)){
                            InformationObject informationObject2 = informationObject[var4];
                            tag = informationObject2.getInformationObjectAddress();
                            InformationElement[][] informationElements = informationObject2.getInformationElements();
                            for (int i = 0; i < informationElements.length; ++i) {
                                if (informationElements[i].length == 2) {
                                    String string1 = informationObject2.getInformationElements()[i][0].toString();
                                    String string2 = informationObject2.getInformationElements()[i][1].toString();
                                    int indexString1 = string1.indexOf(":") + 2;
                                    int index2String1 = string1.indexOf(",", indexString1);
                                    int indexString2 = string2.indexOf(":") + 2;
                                    if (index2String1 > 0) {
                                        data = string1.substring(indexString1, index2String1);
                                        controllerTime = string2.substring(indexString2);
                                    } else {
                                        data = string1.substring(indexString1);
                                    }
                                } else if (informationElements[i].length == 3) {
                                    String string1 = informationObject2.getInformationElements()[i][0].toString();
                                    String string2 = informationObject2.getInformationElements()[i][2].toString();
                                    int indexString1 = string1.indexOf(":") + 2;
                                    int indexString2 = string2.indexOf(":") + 2;
                                    data = string1.substring(indexString1);
                                    controllerTime = string2.substring(indexString2);
                                } else {
                                    if (informationObject2.getInformationObjectAddress() != 0) {
                                        String string1 = informationObject2.getInformationElements()[i][0].toString();
                                        int indexString1 = string1.indexOf(":") + 2;
                                        int index2String1 = string1.indexOf(",", indexString1);
                                        data = string1.substring(indexString1, index2String1);
                                    }
                                }
                            }
                            //if controller time not found in the message, it use server time.
                            if (controllerTime.equals("")) {
                                Date date = new Date();
                                millis = date.getTime();
                            } else {
                                SimpleDateFormat dateFormat = new SimpleDateFormat(context.getProperty(DATE_FORMAT_PROPERTY).getValue());
                                dateFormat.setTimeZone(TimeZone.getTimeZone(context.getProperty(TIMEZONE_PROPERTY).getValue()));
                                Date date = new Date();
                                try {
                                    date = dateFormat.parse(controllerTime);
                                } catch (ParseException e) {
                                    getLogger().error(e.toString());
                                }
                                millis = date.getTime();
                            }
                            if (tag != 0) {
                                writeNext(session, "{\"tag\":\"" + tag + "\", \"data\":\"" + data + "\", \"controllerTime\":\"" + millis + "\"}");
                            }
                            tag = 0;
                            controllerTime = "";
//                            }
                        }
                    } else {
                        getLogger().error("Private Information");
                    }
                }

                @Override
                public void connectionClosed(IOException e) {
                    if (fault) {
                        logException(e);
                        flowFile = session.create();
                        flowFile = session.write(flowFile, outputStream -> outputStream.write(e.toString().getBytes(StandardCharsets.UTF_8)));
                        session.transfer(flowFile, BADCONNECT);
                        session.commit();
                        fault = false;
                    }
                    if (connection != null) {
                        connection = null;
                    }
                }
            };
            connection.startDataTransfer(connectionEventListener, 0);
            if (context.getProperty(FULL_PROPERTY).getValue().equals("true")) {
                connection.interrogation(65535, CauseOfTransmission.ACTIVATION, new IeQualifierOfInterrogation(20));
            }
        } catch (UnknownHostException e) {
            if (fault) {
                logException(e);
                flowFile = session.create();
                flowFile = session.write(flowFile, outputStream -> outputStream.write(e.toString().getBytes(StandardCharsets.UTF_8)));
                session.transfer(flowFile, BADCONNECT);
                session.commit();
                fault = false;
            }
            stopIEC();
        } catch (IOException e) {
            if (fault) {
                logException(e);
                fault = false;
            }
            stopIEC();
        }
    }

    private void logException(Exception e) {
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        getLogger().error(errors.toString());
    }

    private void stopIEC() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}