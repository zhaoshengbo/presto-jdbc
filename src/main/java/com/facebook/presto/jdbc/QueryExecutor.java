/*
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
package com.facebook.presto.jdbc;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.client.StatementClient;
import com.google.common.net.HostAndPort;
import io.airlift.http.client.JsonResponseHandler;
import io.airlift.json.JsonCodec;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.client.HttpAsyncClient;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.*;

import static com.facebook.presto.utils.Objects.requireNonNull;
import static io.airlift.json.JsonCodec.jsonCodec;

class QueryExecutor
        implements Closeable {
    private final JsonCodec<QueryResults> queryInfoCodec;
    private final JsonCodec<ServerInfo> serverInfoCodec;
    private final HttpClient httpClient;
    private final HttpAsyncClient httpAsyncClient;

    private QueryExecutor(JsonCodec<QueryResults> queryResultsCodec, JsonCodec<ServerInfo> serverInfoCodec, HttpClient httpClient, HttpAsyncClient httpAsyncClient) {
        this.queryInfoCodec = requireNonNull(queryResultsCodec, "queryResultsCodec is null");
        this.serverInfoCodec = requireNonNull(serverInfoCodec, "serverInfoCodec is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.httpAsyncClient = requireNonNull(httpAsyncClient, "httpClient is null");
    }

    public StatementClient startQuery(ClientSession session, String query) {
        return new StatementClient(httpClient, httpAsyncClient, queryInfoCodec, session, query);
    }

    @Override
    public void close() {
    }

    public ServerInfo getServerInfo(URI server) {
        try {
            URIBuilder uriBuilder = new URIBuilder(server);
            URI uri = uriBuilder.setPath("/v1/info").build();
            HttpGet httpGet = new HttpGet(uri);
            return httpClient.execute(httpGet, JsonResponseHandler.createJsonResponseHandler(serverInfoCodec));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (ClientProtocolException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: replace this with a phantom reference
    @SuppressWarnings("FinalizeDeclaration")
    @Override
    protected void finalize() {
        close();
    }

    static QueryExecutor create(String userAgent) {
        HttpClient httpClient = HttpClientBuilder.create().setUserAgent(userAgent).build();
        HttpAsyncClient httpAsyncClient = HttpAsyncClientBuilder.create().setUserAgent(userAgent).build();

        return create(httpClient, httpAsyncClient);
    }

    static QueryExecutor create(HttpClient httpClient, HttpAsyncClient httpAsyncClient) {
        return new QueryExecutor(jsonCodec(QueryResults.class), jsonCodec(ServerInfo.class), httpClient, httpAsyncClient);
    }

    @Nullable
    private static HostAndPort getSystemSocksProxy() {
        URI uri = URI.create("socket://0.0.0.0:80");
        for (Proxy proxy : ProxySelector.getDefault().select(uri)) {
            if (proxy.type() == Proxy.Type.SOCKS) {
                if (proxy.address() instanceof InetSocketAddress) {
                    InetSocketAddress address = (InetSocketAddress) proxy.address();
                    return HostAndPort.fromParts(address.getHostString(), address.getPort());
                }
            }
        }
        return null;
    }
}
