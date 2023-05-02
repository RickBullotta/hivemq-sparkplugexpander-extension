/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.industry40.common.sparkplug;

import org.eclipse.tahu.message.*;
import org.eclipse.tahu.message.model.*;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.ModifiablePublishPacket;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import com.hivemq.extension.sdk.api.services.publish.Publish;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a {@link PublishInboundInterceptor} that automatically expands
 * Sparkplug payloads
 *
 * @author Rick Bullotta
 * @since 4.2.0
 */
public class SparkplugExpanderInterceptor implements PublishInboundInterceptor {
    private static final @NotNull Logger logger = LoggerFactory.getLogger(SparkplugExpanderInterceptor.class);

    protected void publishMessage(String topic, String payload, boolean retain) throws Exception {
        final Publish publishTopic = Builders.retainedPublish().topic(topic).payload(ByteBuffer.wrap(payload.getBytes())).build();

        Services.extensionExecutorService().submit(() -> {
            Services.publishService().publish(publishTopic).whenComplete((unused, throwable) -> {
                if (throwable != null) {
                    logger.info("Unable To Publish Topic [" + topic + "]", throwable.getCause());
                }
            });
        });
    }

    protected byte[] toByteArray(ByteBuffer buf) {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    protected JsonNode toJSON(String jsonString) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(jsonString);
        return json;
    }

    protected String JSONToString(JsonNode json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(json);
    }

    protected String MetricToString(Metric metric) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.valueToTree(metric);
        ((ObjectNode)node).remove("timestamp");
        ((ObjectNode)node).remove("value");
        return mapper.writeValueAsString(node);
    }

    protected String spToString(SparkplugBPayload payload) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
        String payloadString = mapper.writeValueAsString(payload);
        return payloadString;
    }

    protected JsonNode spToJSON(SparkplugBPayload payload) throws Exception {
        return toJSON(spToString(payload));
    }

    @Override
    public void onInboundPublish(
            final @NotNull PublishInboundInput publishInboundInput,
            final @NotNull PublishInboundOutput publishInboundOutput) {

        final ModifiablePublishPacket publishPacket = publishInboundOutput.getPublishPacket();

        if(publishPacket.getTopic().startsWith("spBv1.0") && !publishPacket.getTopic().startsWith("spBv1.0/STATE")) {
            try {
                SparkplugBPayloadDecoder decoder = new SparkplugBPayloadDecoder();
                Optional<ByteBuffer> payload = publishPacket.getPayload();
                
                if(payload.isPresent()) {
                    ByteBuffer buffer = payload.get();
            
                    SparkplugBPayload spPayload = null;

                    byte[] bytes = toByteArray(buffer);

                    try {
                        spPayload = decoder.buildFromByteArray(bytes);
                    }
                    catch(Throwable e) {
                        logger.error("Could not convert payload");
                        logger.error(e.getMessage());
                    }
        
                    String[] parts = publishPacket.getTopic().split("/");
                
                    String groupid = parts[1];
                    String msgType = parts[2];
                    String edgenodeid = parts[3];
                    
                    String baseTopic = groupid + "/" + edgenodeid;

                    JsonNode jsonPayload = spToJSON(spPayload);

                    String stringPayload = JSONToString(jsonPayload);
    
                    if(msgType.equals("NDATA") || msgType.equals("NBIRTH")) {
                        if(msgType.equals("NBIRTH")) {
                            try {
                                publishMessage(groupid + "/NMETADATA/" + edgenodeid,stringPayload,true);
                            } catch (Exception e) {
                                logger.error("Unable to publish MQTT NMETADATA message");
                            }
                        }

                        int i;
                        Metric metric;
            
                        // Save the metadata in a new topic that is persisted/retained so that subscribers don"t need the *BIRTH stuff
                        
                        List<Metric> metrics = spPayload.getMetrics();
    
                        for(i=0;i<metrics.size();i++) {
                            metric = metrics.get(i);
                            String name = metric.getName();
    
                            try {
                                publishMessage(baseTopic + "/" + name,metric.getValue().toString(),true);
                            } catch (Exception e) {
                                logger.error("Unable to publish MQTT message");
                            }
                            
                            try {
                                publishMessage(baseTopic + "/" + name + "/_metadata",MetricToString(metric),true);
                            } catch (Exception e) {
                                logger.error("Unable to publish MQTT message");
                            }
                        }
                    }
                    if(msgType.equals("DDATA") || msgType.equals("DBIRTH")) {
                        String  deviceid = parts[4];
            
                        baseTopic = baseTopic + "/" + deviceid;
            
                        if(msgType.equals("DBIRTH")) {
                            try {
                                publishMessage(groupid + "/DMETADATA/" + edgenodeid + "/" + deviceid,stringPayload,true);
                            } catch (Exception e) {
                                logger.error("Unable to publish MQTT DMETADATA message");
                            }
                        }
            
                        int i;
                        Metric metric;
            
                        List<Metric> metrics = spPayload.getMetrics();
    
                        for(i=0;i<metrics.size();i++) {
                            metric = metrics.get(i);
                            String name = metric.getName();
                            try {
                                publishMessage(baseTopic + "/" + name,metric.getValue().toString(),true);
                            } catch (Exception e) {
                                logger.error("Unable to publish MQTT message");
                            }
            
                            try {
                                publishMessage(baseTopic + "/" + name + "/_metadata",MetricToString(metric),true);
                            } catch (Exception e) {
                                logger.error("Unable to publish MQTT message");
                            }
                        }
                    }
                }
            }
            catch(Exception e) {
                logger.error("Unable To Process []" + publishPacket.getTopic() + "] : " + e.getMessage());
            }
        }
   }
}