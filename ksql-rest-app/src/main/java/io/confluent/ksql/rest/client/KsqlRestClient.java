/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.ssl.DefaultSslClientConfigurer;
import io.confluent.ksql.rest.ssl.SslClientConfigurer;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import org.apache.commons.compress.utils.IOUtils;

public class KsqlRestClient implements Closeable {
  private final RestClient client;

  private List<URI> serverAddresses;

  private final LocalProperties localProperties;

  public KsqlRestClient(final String serverAddress) {
    this(serverAddress, Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * @param serverAddress the address of the KSQL server to connect to.
   * @param localProps initial set of local properties.
   * @param clientProps properties used to build the client.
   */
  public KsqlRestClient(
      final String serverAddress,
      final Map<String, ?> localProps,
      final Map<String, String> clientProps
  ) {
    this(
        serverAddress,
        localProps,
        clientProps,
        ClientBuilder.newBuilder(),
        new DefaultSslClientConfigurer()
    );
  }

  @VisibleForTesting
  KsqlRestClient(
      final String serverAddress,
      final Map<String, ?> localProps,
      final Map<String, String> clientProps,
      final ClientBuilder clientBuilder,
      final SslClientConfigurer sslClientConfigurer
  ) {
    this.serverAddresses = parseServerAddresses(serverAddress);
    this.client = new RestClient(
        getServerAddress().toString(),
        clientBuilder,
        sslClientConfigurer,
        clientProps
    );
    this.localProperties = new LocalProperties(localProps);
  }

  @VisibleForTesting
  KsqlRestClient(
      final Client client,
      final String serverAddress,
      final Map<String, ?> localProps
  ) {
    this.serverAddresses = parseServerAddresses(serverAddress);
    this.client = new RestClient(getServerAddress().toString(), client);
    this.localProperties = new LocalProperties(localProps);
  }

  public void setupAuthenticationCredentials(final String userName, final String password) {
    client.setupAuthenticationCredentials(userName, password);
  }

  public URI getServerAddress() {
    return serverAddresses.get(0);
  }

  public void setServerAddress(final String serverAddress) {
    this.serverAddresses = parseServerAddresses(serverAddress);
  }

  public RestResponse<ServerInfo> makeRootRequest() {
    return getServerInfo();
  }

  public RestResponse<ServerInfo> getServerInfo() {
    return client.getRequest("/info", ServerInfo.class);
  }

  public RestResponse<KsqlEntityList> makeKsqlRequest(final String ksql) {
    return makeKsqlRequest(ksql, null);
  }

  public RestResponse<KsqlEntityList> makeKsqlRequest(final String ksql, final Long commandSeqNum) {
    final KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties.toMap(), commandSeqNum);
    return client.postRequest("ksql", jsonRequest, Optional.empty(), true,
        r -> r.readEntity(KsqlEntityList.class));
  }

  public RestResponse<CommandStatuses> makeStatusRequest() {
    return client.getRequest("status", CommandStatuses.class);
  }

  public RestResponse<CommandStatus> makeStatusRequest(final String commandId) {
    return client.getRequest(String.format("status/%s", commandId), CommandStatus.class);
  }

  public RestResponse<QueryStream> makeQueryRequest(final String ksql, final Long commandSeqNum) {
    final KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties.toMap(), commandSeqNum);
    final Optional<Integer> readTimeoutMs = Optional.of(QueryStream.READ_TIMEOUT_MS);
    return client.postRequest("query", jsonRequest, readTimeoutMs, false, QueryStream::new);
  }

  public RestResponse<InputStream> makePrintTopicRequest(
      final String ksql, final Long commandSeqNum) {
    final KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties.toMap(), commandSeqNum);
    return client.postRequest("query", jsonRequest, Optional.empty(), false,
        r -> (InputStream) r.getEntity());
  }

  @Override
  public void close() {
    client.close();
  }

  public static final class QueryStream implements Closeable, Iterator<StreamedRow> {

    private static final int READ_TIMEOUT_MS = (int)TimeUnit.SECONDS.toMillis(2);

    private final Response response;
    private final ObjectMapper objectMapper;
    private final Scanner responseScanner;
    private final InputStreamReader isr;

    private StreamedRow bufferedRow;
    private volatile boolean closed = false;

    private QueryStream(final Response response) {
      this.response = response;

      this.objectMapper = JsonMapper.INSTANCE.mapper;
      this.isr = new InputStreamReader(
          (InputStream) response.getEntity(),
          StandardCharsets.UTF_8
      );
      this.responseScanner = new Scanner((buf) -> {
        while (true) {
          try {
            return isr.read(buf);
          } catch (final SocketTimeoutException e) {
            // Read timeout:
            if (closed) {
              return -1;
            }
          } catch (final IOException e) {
            // Can occur if isr closed:
            if (closed) {
              return -1;
            }

            throw e;
          }
        }
      });

      this.bufferedRow = null;
    }

    @Override
    public boolean hasNext() {
      if (bufferedRow != null) {
        return true;
      }

      return bufferNextRow();
    }

    @Override
    public StreamedRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      final StreamedRow result = bufferedRow;
      bufferedRow = null;
      return result;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }

      synchronized (this) {
        closed = true;
      }
      responseScanner.close();
      response.close();
      IOUtils.closeQuietly(isr);
    }

    private boolean bufferNextRow() {
      try {
        while (responseScanner.hasNextLine()) {
          final String responseLine = responseScanner.nextLine().trim();
          if (!responseLine.isEmpty()) {
            try {
              bufferedRow = objectMapper.readValue(responseLine, StreamedRow.class);
            } catch (final IOException exception) {
              if (closed) {
                return false;
              }
              throw new RuntimeException(exception);
            }
            return true;
          }
        }

        return false;
      } catch (final IllegalStateException e) {
        // Can happen is scanner is closed:
        if (closed) {
          return false;
        }

        throw e;
      }
    }
  }

  public Object setProperty(final String property, final Object value) {
    return localProperties.set(property, value);
  }

  public Object unsetProperty(final String property) {
    return localProperties.unset(property);
  }

  private static List<URI> parseServerAddresses(final String serverAddresses) {
    Objects.requireNonNull(serverAddresses, "serverAddress");
    return ImmutableList.copyOf(
        Arrays.stream(serverAddresses.split(","))
            .map(String::trim)
            .map(KsqlRestClient::parseServerAddress)
            .collect(Collectors.toList()));
  }

  private static URI parseServerAddress(final String serverAddress) {
    Objects.requireNonNull(serverAddress, "serverAddress");
    try {
      return new URL(serverAddress).toURI();
    } catch (final Exception e) {
      throw new KsqlRestClientException(
          "The supplied serverAddress is invalid: " + serverAddress, e);
    }
  }
}