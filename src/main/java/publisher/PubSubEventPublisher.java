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

package publisher;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import interfaces.Event;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import static proxy.ProxyPatcher.configureChannelProxy;

public class PubSubEventPublisher implements IEventPublisher {
    public Publisher publisher;

    public PubSubEventPublisher(String projectId, String serviceAccountPath, String topicId) {
        Publisher.Builder publisherBuilder = null;
        this.publisher = null;

        TopicName topicName = TopicName.of(projectId, topicId);
        try {
            publisherBuilder = Publisher.newBuilder(topicName)
                    .setCredentialsProvider(() -> GoogleCredentials.fromStream(getCredentials(serviceAccountPath)));
            // Configure channel proxy is only for this to work in Vodafone VPC
            configureChannelProxy(publisherBuilder);
            this.publisher = publisherBuilder.build();
            System.out.println("Publisher created successfully");
        } catch (Exception e) {
            System.out.println("Error creating pubsub publisher");
        }
    }

    static InputStream getCredentials(String saPath) throws FileNotFoundException {
        File credentialsFile = new File(saPath);
        return new FileInputStream(credentialsFile);
    }

    public void publishEvent(Event event) {
        Gson gson = new Gson();

        System.out.println("Publishing event");
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
                            System.out.println("Error publishing message : " + e.getMessage());

                        }

                        @Override
                        public void onSuccess(String messageId) {
                            System.out.println("Published message ID: " + messageId);
                        }
                    },
                    MoreExecutors.directExecutor());
            int retries = 0;
            while (!future.isDone() && retries <= 10) {
                System.out.println("Future not done yet");
                retries++;
                Thread.sleep(300);
            }
        } catch (Exception e) {
            System.out.println("Error publishing message: " + e.getMessage());
        }
    }
}
