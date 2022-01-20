/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.events.process.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import context.EventWriterContext;
import events.EventType;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PubSubEventWriterTest extends TestCase {
    private static PubSubEventWriter eventWriter;

    private static final String PROJECT = "project";
    private static final String SA_PATH = "service_account_path";
    private static final String TOPIC = "topic";
    private static final String PROXY_HOST = "proxy_host";
    private static final String PROXY_PORT = "proxy_port";
    private static final String WRITER_NAME = "pub_sub_event_writer";

    private static final Gson gson = new Gson();

    @Mock
    private static Publisher mockedPublisher;

    @BeforeClass
    public static void initTest() {
        Map<String,String> mockedProperties = new HashMap<>();
        mockedProperties.put(PROJECT, "testproject");
        mockedProperties.put(SA_PATH, "test");
        mockedProperties.put(TOPIC, "test-topic");
        mockedProperties.put(PROXY_HOST, "0.0.0.0");
        mockedProperties.put(PROXY_PORT, "8080");
        mockedProperties.put(WRITER_NAME, "test-writer");

        EventWriterContext mockContext = new EventWriterContext() {
            Map<String,String> mockedProperties = new HashMap<>();

            @Override
            public Map<String, String> getProperties() {
                return mockedProperties;
            }
        };

        eventWriter = new PubSubEventWriter();
        eventWriter.initialize(mockContext);
    }

    @Test
    public void testPublishEvent() {
        Event mockedEvent = new Event() {
            @Override
            public EventType getType() {
                return EventType.PROGRAM_STATUS;
            }

            @Override
            public long getPublishTime() {
                return 1;
            }

            @Override
            public String getVersion() {
                return "1.0.0";
            }

            @Override
            public String getInstanceName() {
                return "POTATO";
            }

            @Override
            public Object getEventDetails() {
                return "this-is-a-test";
            }
        };
        String stringEvent = gson.toJson(mockedEvent);
        ByteString data = ByteString.copyFromUtf8(stringEvent);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(data)
                .build();
        System.out.println("pubsubMessage: " + pubsubMessage.getMessageId());
        System.out.println("mocked event :" + mockedEvent);
        System.out.println("data  :" + stringEvent);


        when(mockedPublisher.publish(pubsubMessage)).thenReturn(ApiFutures.immediateFuture("soy-el-message-id"));
        this.eventWriter.publishEvent(mockedEvent);
        System.out.println("Test executed");
    }

    public void testPublishEventKo() {
    }

    public void testClose() {
    }
}
