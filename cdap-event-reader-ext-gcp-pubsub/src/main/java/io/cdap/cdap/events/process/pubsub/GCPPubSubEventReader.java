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

import autovalue.shaded.kotlin.Pair;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.UpdateSubscriptionRequest;
import io.cdap.cdap.spi.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * {@link io.cdap.cdap.spi.events.EventReader} implementation for sending events to Pub/Sub
 */
public class GCPPubSubEventReader implements EventReader<StartProgramEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(GCPPubSubEventReader.class);
    private static final Gson GSON = new Gson();

    //Constant for retrieves the variables from the context
    private static final String CONFIG_PROJECT = "project";
    private static final String CONFIG_SA_PATH = "serviceAccountPath";
    private static final String CONFIG_TOKEN_ENDPOINT = "token.endpoint";
    private static final String CONFIG_TOPIC = "topic";
    private static final String CONFIG_ACK_DEADLINE = "ackDeadline";
    private static final String CONFIG_PROXY_HOST = "proxyHost";
    private static final String CONFIG_PROXY_PORT = "proxyPort";

    private static final String CONFIG_READER_ID = "pub-sub-event-reader";
    private static final String CONFIG_SUBSCRIPTION = "subscription";
    @Nullable
    private SubscriberStub subscriber;

    private String subscriptionName;
    @Nullable
    private String serviceAccountPath;

    public GCPPubSubEventReader() {
    }


    @Override
    public void initialize(EventReaderContext pubSubEventReaderContext) {
        if (getSubscriber() != null) {
            LOG.warn("Subscriber already initialized");
            return;
        }
        String projectId = pubSubEventReaderContext.getProperties().get(CONFIG_PROJECT);
        String topicId = pubSubEventReaderContext.getProperties().get(CONFIG_TOPIC);
        String subscriptionId = pubSubEventReaderContext.getProperties().get(CONFIG_SUBSCRIPTION);
        String tokenEndPoint =  pubSubEventReaderContext.getProperties().get(CONFIG_TOKEN_ENDPOINT);
        serviceAccountPath =  pubSubEventReaderContext.getProperties().get(CONFIG_SA_PATH);
        int deadline = Integer.parseInt(pubSubEventReaderContext.getProperties().get(CONFIG_ACK_DEADLINE));
        start(projectId, topicId, subscriptionId, tokenEndPoint, deadline);
    }

    private void start(String projectId, String topicId, String subscriptionId,
        @Nullable String tokenEndPoint, int ackDeadline) {
        subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
        LOG.debug("Using token end point {}.", tokenEndPoint);

        TopicName topicName = TopicName.of(projectId, topicId);
        try {

            SubscriberStubSettings.Builder subscriberStubSettings =
                    SubscriberStubSettings.newBuilder()
                            .setTransportChannelProvider(
                                    SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                            .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                                            .build());
            subscriberStubSettings = setCredentials(subscriberStubSettings, tokenEndPoint);

            this.subscriber = GrpcSubscriberStub.create(subscriberStubSettings.build());
            SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriber);
            this.subscriber.updateSubscriptionCallable()
                    .call(setupSubscription(subscriptionAdminClient
                            .getSubscription(subscriptionName), ackDeadline).build());
            LOG.info("Subscriber created successfully");
        } catch (IOException e) {
            LOG.error("Error creating pubsub events.subscriber.", e);
        } finally {
            // TODO: Clean up subscriber
        }
    }

    private SubscriberStubSettings.Builder setCredentials(SubscriberStubSettings.Builder subscriberSettings,
                                                          String tokenEndPoint) throws IOException {
        // Use SA if present
        if (this.serviceAccountPath != null) {
            LOG.debug("Using provided service account path for fetching credentials.");
            return subscriberSettings
                    .setCredentialsProvider(
                            () -> GoogleCredentials.fromStream(getCredentials(serviceAccountPath)));
        }

        // Use token end point for token
        return subscriberSettings.setCredentialsProvider(() -> ComputeEngineCredentials.getOrCreate(tokenEndPoint));
    }

    /**
     *
     */
    @Override
    public EventResult<StartProgramEvent> pull(int maxMessages) {
        if (getSubscriber() == null) {
            LOG.warn("Subscriber is not initialized, cannot pull message");
            LOG.info("No message was pulled. Exiting.");
            return new PubSubEventResult(Collections.emptyList());
        }
        PullRequest pullRequest =
                PullRequest.newBuilder()
                        .setMaxMessages(maxMessages)
                        .setSubscription(getSubscriptionName())
                        .build();
        PullResponse pullResponse = getSubscriber().pullCallable().call(pullRequest);

        // Stop the program if the pull response is empty to avoid acknowledging
        // an empty list of ack IDs.
        if (pullResponse.getReceivedMessagesList().isEmpty()) {
            LOG.info("No message was pulled. Exiting.");
            return new PubSubEventResult(Collections.emptyList());
        }

        LOG.info("Pulling message");
        List<Pair<StartProgramEvent, String>> receivedEventList = new ArrayList<>();
        for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
            try {

                StartProgramEventDetails eventDetails = GSON.fromJson(message.getMessage().getData().toStringUtf8(),
                        StartProgramEventDetails.class);
                StartProgramEvent event = new StartProgramEvent(
                        message.getMessage().getPublishTime().getSeconds(),
                        message.getMessage().getMessageId(), eventDetails);
                receivedEventList.add(new Pair<>(event, message.getAckId()));
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
        }
        return new PubSubEventResult(receivedEventList);
    }

    protected void ack(String ackId) throws TimeoutException {
        if (getSubscriber() == null) {
            LOG.warn("Subscriber is not initialized, cannot handle message");
            return;
        }
        AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder().setSubscription(getSubscriptionName())
                .addAckIds(ackId).build();
        try {
            getSubscriber().acknowledgeCallable().call(acknowledgeRequest);
        } catch (Exception e) {
            throw new TimeoutException(e.getMessage());
        }
    }

    protected void nack(String ackId) throws TimeoutException {
        if (getSubscriber() == null) {
            LOG.warn("Subscriber is not initialized, cannot handle message");
            return;
        }
            ModifyAckDeadlineRequest nackRequest = ModifyAckDeadlineRequest.newBuilder()
                    .setSubscription(getSubscriptionName())
                    .addAckIds(ackId).setAckDeadlineSeconds(0).build();
            LOG.warn("Retrying message");
        try {
            getSubscriber().modifyAckDeadlineCallable().call(nackRequest);
        } catch (Exception e) {
            throw new TimeoutException(e.getMessage());
        }
    }


    public String getId() {
        return CONFIG_READER_ID;
    }

    public void close() {
        if (getSubscriber() == null) {
            LOG.warn("Subscriber is not initialized, cannot close subscriber");
            return;
        }
        subscriber.shutdown();
    }

    /**
     * @return Instance of Pub/Sub Subscriber
     */
    protected SubscriberStub getSubscriber() {
        return this.subscriber;
    }

    protected String getSubscriptionName() {
        return this.subscriptionName;
    }
    /**
     * Method to use the service account path as a File
     *
     * @param serviceAccountPath Service account where is storage
     * @return Service account as a file
     * @throws FileNotFoundException If the file does not exists, it will throws a FileNotFoundException
     */
    private InputStream getCredentials(String serviceAccountPath) throws FileNotFoundException {
        File credentialsFile = new File(serviceAccountPath);
        return new FileInputStream(credentialsFile);
    }

    /**
     * Create a subscription builder with a set ack deadline and exactly-once delivery
     * @param subscription Subscription to update
     * @param newDeadline New acknowledgement deadline
     * @return UpdateSubscriptionRequest.Builder to update the subscription
     */
    private static UpdateSubscriptionRequest.Builder setupSubscription(Subscription subscription,
                                                                       int newDeadline) {

        Descriptors.FieldDescriptor ackDeadlineFieldDescriptor = Subscription.getDescriptor()
                .findFieldByNumber(Subscription.ACK_DEADLINE_SECONDS_FIELD_NUMBER);
        return UpdateSubscriptionRequest
                .newBuilder()
                .setSubscription(subscription.toBuilder()
                        .setAckDeadlineSeconds(newDeadline)
                        .setEnableExactlyOnceDelivery(true))
                .setUpdateMask(FieldMask
                        .newBuilder()
                        .addPaths("ack_deadline_seconds")
                        .addPaths("enable_exactly_once_delivery")
                        .build());
    }

    class PubSubEventResult implements EventResult<StartProgramEvent> {
        private final List<Pair<StartProgramEvent, String>> messages;

        PubSubEventResult(List<Pair<StartProgramEvent, String>> messages) {
            this.messages = messages;
        }

        @Override
        public void consumeMessages(Consumer<StartProgramEvent> consumer) {
            for (Pair<StartProgramEvent, String> msg : messages) {
                try {
                    consumer.accept(msg.component1());
                    ack(msg.component2());
                } catch (IllegalStateException e) {
                    try {
                        nack(msg.component2());
                    } catch (TimeoutException e2) {
                        LOG.warn("ACK Timeout");
                    }
                } catch (TimeoutException e) {
                    LOG.warn("ACK Timeout");
                }


            }

        }

        @Override
        public void close() throws Exception {

        }
    }

}
