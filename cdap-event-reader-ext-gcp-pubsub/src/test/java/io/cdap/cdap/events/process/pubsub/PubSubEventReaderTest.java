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

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.gson.Gson;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.cdap.cdap.spi.events.EventReaderContext;
import io.cdap.cdap.spi.events.EventResult;
import io.cdap.cdap.spi.events.StartProgramEvent;
import io.cdap.cdap.spi.events.StartProgramEventDetails;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link GCPPubSubEventReader}.
 */
@RunWith(MockitoJUnitRunner.class)
public class PubSubEventReaderTest {
  private static final String SA_PATH = "serviceAccountPath";
  private static final String TOPIC = "topic";
  private static final String PROXY_HOST = "proxyHost";
  private static final String PROXY_PORT = "proxyPort";
  private static final String READER_NAME = "pub-sub-event-reader";
  private static final String SUBSCRIPTION = "subscription";
  private static final String ACK_DEADLINE = "ackDeadline";
  private static final Gson gson = new Gson();
  private static Map<String, String> mockedProperties;

  @BeforeClass
  public static void initTest() {
    mockedProperties = new HashMap<>();
    mockedProperties.put(SA_PATH, "test");
    mockedProperties.put(TOPIC, "test-topic");
    mockedProperties.put(PROXY_HOST, "0.0.0.0");
    mockedProperties.put(PROXY_PORT, "8080");
    mockedProperties.put(READER_NAME, "test-reader");
    mockedProperties.put(SUBSCRIPTION, "test-subscription");
    mockedProperties.put(ACK_DEADLINE, "60");
  }

  @Test
  public void testPull() {
    GCPPubSubEventReader original = new GCPPubSubEventReader();
    GCPPubSubEventReader eventReader = Mockito.spy(original);
    SubscriberStub mockedSubscriber = Mockito.mock(SubscriberStub.class);
    EventReaderContext mockContext = () -> mockedProperties;
    Mockito.doReturn("test-subscription").when(eventReader).getSubscriptionName();
    Mockito.doReturn(mockedSubscriber).when(eventReader).getSubscriber();
    eventReader.initialize(mockContext);

    StartProgramEventDetails details = Mockito.mock(StartProgramEventDetails.class);

    StartProgramEvent expected = new StartProgramEvent( 1, "1.0.0", details);

    when(mockedSubscriber.pullCallable()).thenReturn(Mockito.mock(UnaryCallable.class));
    when(mockedSubscriber.pullCallable().call(any())).thenReturn(PullResponse.newBuilder().addReceivedMessages(
            ReceivedMessage.newBuilder().setMessage(PubsubMessage.newBuilder()).setAckId("ack-id")).build());
    Mockito.doReturn(mockedSubscriber).when(eventReader).getSubscriber();
    EventResult<StartProgramEvent> actual = eventReader.pull(1);
    AtomicBoolean called = new AtomicBoolean(false);
    actual.consumeMessages((startProgramEvent -> {
      called.set(true);
    }));

    assert(called.get());
  }

  @Test
  public void testAck() {
    GCPPubSubEventReader original = new GCPPubSubEventReader();
    GCPPubSubEventReader eventReader = Mockito.spy(original);
    SubscriberStub mockedSubscriber = Mockito.mock(SubscriberStub.class);
    EventReaderContext mockContext = () -> mockedProperties;
    Mockito.doReturn("test-subscription").when(eventReader).getSubscriptionName();
    Mockito.doReturn(mockedSubscriber).when(eventReader).getSubscriber();
    eventReader.initialize(mockContext);

    StartProgramEventDetails details = Mockito.mock(StartProgramEventDetails.class);

    StartProgramEvent expected = new StartProgramEvent( 1, "1.0.0", details);

    when(mockedSubscriber.pullCallable()).thenReturn(Mockito.mock(UnaryCallable.class));
    when(mockedSubscriber.pullCallable().call(any())).thenReturn(PullResponse.newBuilder().addReceivedMessages(
            ReceivedMessage.newBuilder().setMessage(PubsubMessage.newBuilder()).setAckId("ack-id")).build());
    Mockito.doReturn(mockedSubscriber).when(eventReader).getSubscriber();
    EventResult<StartProgramEvent> actual = eventReader.pull(1);
    actual.consumeMessages((startProgramEvent -> {
    }));

    try {
     verify(eventReader).ack("ack-id");
    } catch (TimeoutException e) {
      assert false;
    }
  }

  @Test
  public void testNack() {
    GCPPubSubEventReader original = new GCPPubSubEventReader();
    GCPPubSubEventReader eventReader = Mockito.spy(original);
    SubscriberStub mockedSubscriber = Mockito.mock(SubscriberStub.class);
    EventReaderContext mockContext = () -> mockedProperties;
    Mockito.doReturn("test-subscription").when(eventReader).getSubscriptionName();
    Mockito.doReturn(mockedSubscriber).when(eventReader).getSubscriber();
    eventReader.initialize(mockContext);

    StartProgramEventDetails details = Mockito.mock(StartProgramEventDetails.class);

    StartProgramEvent expected = new StartProgramEvent( 1, "1.0.0", details);

    when(mockedSubscriber.pullCallable()).thenReturn(Mockito.mock(UnaryCallable.class));
    when(mockedSubscriber.pullCallable().call(any())).thenReturn(PullResponse.newBuilder().addReceivedMessages(
            ReceivedMessage.newBuilder().setMessage(PubsubMessage.newBuilder()).setAckId("ack-id")).build());
    Mockito.doReturn(mockedSubscriber).when(eventReader).getSubscriber();
    EventResult<StartProgramEvent> actual = eventReader.pull(1);
    actual.consumeMessages((startProgramEvent -> {
      throw new IllegalStateException("Too many requests, please retry");
    }));

    try {
      verify(eventReader).nack("ack-id");
    } catch (TimeoutException e) {
      assert false;
    }
  }
}
