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

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.cdap.cdap.spi.events.Event;
import io.cdap.cdap.spi.events.EventType;
import io.cdap.cdap.spi.events.EventWriterContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PubSubEventWriterTest {
    private static final String PROJECT = "project";
    private static final String SA_PATH = "service_account_path";
    private static final String TOPIC = "topic";
    private static final String PROXY_HOST = "proxy_host";
    private static final String PROXY_PORT = "proxy_port";
    private static final String WRITER_NAME = "pub_sub_event_writer";
    private static Map<String, String> mockedProperties;

    private static final Gson gson = new Gson();

    @BeforeClass
    public static void initTest() {
        mockedProperties = new HashMap<>();
        mockedProperties.put(PROJECT, "testproject");
        mockedProperties.put(SA_PATH, "test");
        mockedProperties.put(TOPIC, "test-topic");
        mockedProperties.put(PROXY_HOST, "0.0.0.0");
        mockedProperties.put(PROXY_PORT, "8080");
        mockedProperties.put(WRITER_NAME, "test-writer");

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
                return "test-instance";
            }

            @Override
            public Object getEventDetails() {
                return "this-is-a-test";
            }
        };

        PubSubEventWriter original = new PubSubEventWriter();
        PubSubEventWriter eventWriter = Mockito.spy(original);
        Publisher mockedPublisher = Mockito.mock(Publisher.class);
        when(mockedPublisher.publish(any())).thenReturn(ApiFutures.immediateFuture("soy-el-message-id"));
        Mockito.doReturn(mockedPublisher).when(eventWriter).getPublisher();
        EventWriterContext mockContext = () -> mockedProperties;

        eventWriter.initialize(mockContext);

        String stringEvent = gson.toJson(mockedEvent);
        ByteString data = ByteString.copyFromUtf8(stringEvent);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

        Collection<Event> events = new ArrayList<>();
        events.add(mockedEvent);
        eventWriter.write(events);
        Mockito.verify(mockedPublisher).publish(pubsubMessage);
    }

    @Test
    public void testPublishEventKo() {
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
                return "test-instance";
            }

            @Override
            public Object getEventDetails() {
                return "this-is-a-test";
            }
        };

        PubSubEventWriter original = new PubSubEventWriter();
        PubSubEventWriter eventWriter = Mockito.spy(original);
        Publisher mockedPublisher = Mockito.mock(Publisher.class);
        when(mockedPublisher.publish(any()))
                .thenReturn(ApiFutures.immediateFailedFuture(new Exception("Error publishing")));
        Mockito.doReturn(mockedPublisher).when(eventWriter).getPublisher();
        EventWriterContext mockContext = () -> mockedProperties;

        eventWriter.initialize(mockContext);

        String stringEvent = gson.toJson(mockedEvent);
        ByteString data = ByteString.copyFromUtf8(stringEvent);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();


        Collection<Event> events = new ArrayList<>();
        events.add(mockedEvent);
        eventWriter.write(events);
        Mockito.verify(mockedPublisher).publish(pubsubMessage);
    }

    @Test
    public void testClose() throws InterruptedException {
        PubSubEventWriter original = new PubSubEventWriter();
        PubSubEventWriter eventWriter = Mockito.spy(original);
        Publisher mockedPublisher = Mockito.mock(Publisher.class);

        Mockito.doReturn(mockedPublisher).when(eventWriter).getPublisher();
        Mockito.doNothing().when(mockedPublisher).shutdown();
        try {
            Mockito.doReturn(true).when(mockedPublisher).awaitTermination(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        EventWriterContext mockContext = () -> mockedProperties;

        eventWriter.initialize(mockContext);

        eventWriter.close();
        Mockito.verify(mockedPublisher).awaitTermination(15, TimeUnit.SECONDS);
    }
}
