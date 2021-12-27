package proxy;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxiedSocketAddress;
import io.grpc.ProxyDetector;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

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

public class ProxyPatcher {
    public static void configureChannelProxy(Publisher.Builder builder) throws NumberFormatException {
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
}
