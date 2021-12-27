/*
 * Copyright © 2021 Cask Data, Inc.
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

package events.writer;

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
import events.ProgramStatusEventDetails;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxiedSocketAddress;
import io.grpc.ProxyDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * PubSubEventWriter takes messages from CDAP Event Publisher and writes them
 * to Google Cloud PubSub
 */
public class PubSubEventWriter implements EventWriter {
    private Publisher publisher;
    private static final Logger logger = LoggerFactory.getLogger(PubSubEventWriter.class);

    /**
     *
     * @param projectId
     * @param serviceAccountPath
     * @param topicId
     */
    public PubSubEventWriter(String projectId, String serviceAccountPath, String topicId) {
        Publisher.Builder publisherBuilder = null;
        this.publisher = null;

        TopicName topicName = TopicName.of(projectId, topicId);
        try {
            publisherBuilder = Publisher.newBuilder(topicName)
                    .setCredentialsProvider(() -> GoogleCredentials.fromStream(getCredentials(serviceAccountPath)));
            // Configure channel proxy is only for this to work in Vodafone VPC
            configureChannelProxy(publisherBuilder);
            this.publisher = publisherBuilder.build();
            logger.info("Publisher created successfully");
        } catch (Exception e) {
            logger.error("Error creating pubsub events.publisher");
        }
    }

    /**
     *
     * @param saPath
     * @return
     * @throws FileNotFoundException
     */
    private InputStream getCredentials(String saPath) throws FileNotFoundException {
        File credentialsFile = new File(saPath);
        return new FileInputStream(credentialsFile);
    }

    /**
     *
     * @param builder
     * @throws NumberFormatException
     */
    private void configureChannelProxy(Publisher.Builder builder) throws NumberFormatException {
        SocketAddress proxySocketAddress = new InetSocketAddress("10.74.42.22", 8080);
        builder.setChannelProvider(
                InstantiatingGrpcChannelProvider.newBuilder()
                        .setChannelConfigurator(managedChannelBuilder -> managedChannelBuilder.proxyDetector(new ProxyDetector() {
                            @Nullable
                            @Override
                            public ProxiedSocketAddress proxyFor(SocketAddress socketAddress) {
                                if (socketAddress == null) return null;

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
     * @param event
     */
    public void publishEvent(ProgramStatusEventDetails event) {
        Gson gson = new Gson();

        logger.info("Publishing event");
        String stringEvent = gson.toJson(event);
        ByteString data = ByteString.copyFromUtf8(stringEvent);
        try {
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(data)
                    .build();
            ApiFuture<String> future = this.publisher.publish(pubsubMessage);
            ApiFutures.addCallback(
                    future,
                    new ApiFutureCallback<String>() {

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error("Error publishing message : " + e.getMessage());

                        }

                        @Override
                        public void onSuccess(String messageId) {
                            logger.info("Published message ID: " + messageId);
                        }
                    },
                    MoreExecutors.directExecutor());
            int retries = 0;
            while (!future.isDone() && retries <= 10) {
                logger.info("Future not done yet");
                retries++;
                Thread.sleep(300);
            }
        } catch (Exception e) {
            logger.error("Error publishing message: " + e.getMessage());
        }
    }
}
