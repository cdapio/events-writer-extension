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
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.cdap.cdap.spi.events.EventWriter;
import io.cdap.cdap.spi.events.EventWriterContext;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxiedSocketAddress;
import io.grpc.ProxyDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * {@link EventWriter} implementation for sending events to Pub/Sub
 */
public class PubSubEventWriter implements EventWriter {

    private static final Gson GSON = new Gson();
    private static final String PROJECT = "project";
    private static final String SA_PATH = "service_account_path";
    private static final String TOPIC = "topic";
    private static final String PROXY_HOST = "proxy_host";
    private static final String PROXY_PORT = "proxy_port";
    private static final String WRITER_NAME = "pub_sub_event_writer";

    @Nullable
    private Publisher publisher;
    private static final Logger logger = LoggerFactory.getLogger(PubSubEventWriter.class);
    private String serviceAccountPath;

    public PubSubEventWriter() {}

    @Override
    public void initialize(EventWriterContext eventWriterContext) {
        if (getPublisher() != null) {
            logger.debug("Publisher is already initialized");
            publisher = getPublisher();
            return;
        }
        String projectId = eventWriterContext.getProperties().get(PROJECT);
        String topicId = eventWriterContext.getProperties().get(TOPIC);
        serviceAccountPath = eventWriterContext.getProperties().get(SA_PATH);

        Publisher.Builder publisherBuilder;
        TopicName topicName = TopicName.of(projectId, topicId);

        try {
            // This means to use the service account if it comes from the CDAP configuration
            if (this.serviceAccountPath != null) {
                publisherBuilder = Publisher.newBuilder(topicName)
                        .setCredentialsProvider(() -> GoogleCredentials.fromStream(getCredentials(serviceAccountPath)));
            } else {
                publisherBuilder = Publisher.newBuilder(topicName);
            }
            String proxyHost = eventWriterContext.getProperties().get(PROXY_HOST);
            String proxyPort = eventWriterContext.getProperties().get(PROXY_PORT);
            // This means to configure the proxy if it comes from the CDAP configuration
            if (proxyHost != null && proxyPort != null) {
                configureChannelProxy(publisherBuilder, proxyHost, proxyPort);
            }
            this.publisher = publisherBuilder.build();
            logger.info("Publisher created successfully");
        } catch (IOException e) {
            logger.error("Error creating pubsub events.publisher.", e);
        }
    }

    /**
     * Method to use the service account path as a File
     *
     * @param saPath Service account where is storage
     * @return Service account as a file
     * @throws FileNotFoundException If the file does not exists, it will throws a FileNotFoundException
     */
    private InputStream getCredentials(String saPath) throws FileNotFoundException {
        File credentialsFile = new File(saPath);
        return new FileInputStream(credentialsFile);
    }

    /**
     * Method to configure proxy to the Pub/Sub Builder
     *
     * @param builder   Pub/Sub builder where was already initialized with the topic name, and project
     * @param proxyHost Proxy host where configure the use of the proxy
     * @param proxyPort Proxy port where configure the use of the proxy
     * @throws NumberFormatException
     */
    private void configureChannelProxy(Publisher.Builder builder, String proxyHost, String proxyPort)
            throws NumberFormatException {
        SocketAddress proxySocketAddress = new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort));
        builder.setChannelProvider(
                InstantiatingGrpcChannelProvider.newBuilder()
                        .setChannelConfigurator(managedChannelBuilder -> managedChannelBuilder.proxyDetector(
                                new ProxyDetector() {
                                    @Nullable
                                    @Override
                                    public ProxiedSocketAddress proxyFor(SocketAddress socketAddress) {
                                        if (socketAddress == null) {
                                            return null;
                                        }
                                        return HttpConnectProxiedSocketAddress.newBuilder()
                                                .setTargetAddress((InetSocketAddress) socketAddress)
                                                .setProxyAddress(proxySocketAddress)
                                                .build();
                                    }
                                }))
                        .build()
        );
    }

    /**
     *
     * @return Instance Pub/sub publisher
     */
    protected Publisher getPublisher() {
        return this.publisher;
    }

    @Override
    public void write(Collection events) {
        if (publisher == null) {
            logger.debug("Publisher is not already initialized");
            return;
        }
        Iterator iterator = events.iterator();

        while (iterator.hasNext()) {
            String stringEvent = GSON.toJson(iterator.next());
            ByteString data = ByteString.copyFromUtf8(stringEvent);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

            ApiFuture<String> future = this.publisher.publish(pubsubMessage);
            ApiFutures.addCallback(
                    future,
                    new ApiFutureCallback<String>() {

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error("Error publishing message", e);
                        }

                        @Override
                        public void onSuccess(String messageId) {
                            logger.info("Published message ID: " + messageId);
                        }
                    },
                    MoreExecutors.directExecutor());
        }
    }

    @Override
    public String getID() {
        return WRITER_NAME;
    }

    @Override
    public void close() {
        if (publisher == null) {
            return;
        }
        publisher.shutdown();
        try {
            publisher.awaitTermination(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Error while attempting to shutdown publisher", e);
        }
    }
}
