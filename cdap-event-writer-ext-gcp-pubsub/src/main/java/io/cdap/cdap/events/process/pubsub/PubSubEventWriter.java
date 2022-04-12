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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * {@link EventWriter} implementation for sending events to Pub/Sub
 */
public class PubSubEventWriter implements EventWriter {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubEventWriter.class);
  private static final Gson GSON = new Gson();

  //Constant for retrieves the variables from the context
  private static final String CONFIG_PROJECT = "project";
  private static final String CONFIG_SA_PATH = "service-account-path";
  private static final String CONFIG_TOPIC = "topic";
  private static final String CONFIG_PROXY_HOST = "proxy-host";
  private static final String CONFIG_PROXY_PORT = "proxy-port";

  private static final String CONFIG_WRITER_ID = "pub-sub-event-writer";

  @Nullable
  private Publisher publisher;
  @Nullable
  private String serviceAccountPath;

  public PubSubEventWriter() {
  }

  @Override
  public void initialize(EventWriterContext eventWriterContext) {
    if (getPublisher() != null) {
      LOG.warn("Publisher is already initialized");
      return;
    }
    String projectId = eventWriterContext.getProperties().get(CONFIG_PROJECT);
    String topicId = eventWriterContext.getProperties().get(CONFIG_TOPIC);
    serviceAccountPath = eventWriterContext.getProperties().get(CONFIG_SA_PATH);

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
      String proxyHost = eventWriterContext.getProperties().get(CONFIG_PROXY_HOST);
      String proxyPort = eventWriterContext.getProperties().get(CONFIG_PROXY_PORT);
      // This means to configure the proxy if it comes from the CDAP configuration
      if (proxyHost != null && proxyPort != null) {
        configureChannelProxy(publisherBuilder, proxyHost, proxyPort);
      }
      this.publisher = publisherBuilder.build();
      LOG.info("Publisher created successfully");
    } catch (IOException e) {
      LOG.error("Error creating pubsub events.publisher.", e);
    }
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
   * @return Instance Pub/sub publisher
   */
  protected Publisher getPublisher() {
    return this.publisher;
  }

  @Override
  public void write(Collection events) {
    if (getPublisher() == null) {
      LOG.warn("Publisher is not initialized, events will not be published.");
      return;
    }
    Iterator iterator = events.iterator();

    while (iterator.hasNext()) {
      String stringEvent = GSON.toJson(iterator.next());
      ByteString data = ByteString.copyFromUtf8(stringEvent);
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
      ApiFuture<String> future = getPublisher().publish(pubsubMessage);
      ApiFutures.addCallback(
        future,
        new ApiFutureCallback<String>() {

          @Override
          public void onFailure(Throwable e) {
            LOG.error("Error publishing message", e);
          }

          @Override
          public void onSuccess(String messageId) {
            LOG.trace("Published message ID: " + messageId);
          }
        },
        MoreExecutors.directExecutor());
    }
  }

  @Override
  public String getID() {
    return CONFIG_WRITER_ID;
  }

  @Override
  public void close() {
    if (getPublisher() == null) {
      return;
    }
    getPublisher().shutdown();
    try {
      getPublisher().awaitTermination(15, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Error while attempting to shutdown publisher", e);
    }
  }
}
