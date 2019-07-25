/*
 * Copyright 2019 Confluent Inc.
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.rest.ssl.DefaultSslClientConfigurer;
import io.confluent.ksql.rest.ssl.SslClientConfigurer;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.io.Closeable;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.naming.AuthenticationException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

public class RestClient implements Closeable {
  private static final int MAX_TIMEOUT = (int)TimeUnit.SECONDS.toMillis(32);

  private static final KsqlErrorMessage UNAUTHORIZED_ERROR_MESSAGE = new KsqlErrorMessage(
      Errors.ERROR_CODE_UNAUTHORIZED,
      new AuthenticationException(
          "Could not authenticate successfully with the supplied credentials.")
  );

  private static final KsqlErrorMessage FORBIDDEN_ERROR_MESSAGE = new KsqlErrorMessage(
      Errors.ERROR_CODE_FORBIDDEN,
      new AuthenticationException("You are forbidden from using this cluster.")
  );

  private final Client client;
  private URI serverAddress;

  public RestClient(final String serverAddress) {
    this(
        serverAddress,
        ClientBuilder.newBuilder(),
        new DefaultSslClientConfigurer(),
        Collections.emptyMap()
    );
  }

  public RestClient(
      final String serverAddress,
      final ClientBuilder clientBuilder,
      final SslClientConfigurer sslClientConfigurer,
      final Map<String, String> props) {
    this.serverAddress = parseServerAddress(serverAddress);
    this.client = buildClient(clientBuilder, sslClientConfigurer, props);
  }

  public RestClient(final String serverAddress, final Client client) {
    this.client = client;
    this.serverAddress = parseServerAddress(serverAddress);
  }

  public <T> RestResponse<T> getRequest(final String path, final Class<T> type) {
    try (Response response = client.target(serverAddress)
        .path(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .get()) {

      return response.getStatus() == Response.Status.OK.getStatusCode()
          ? RestResponse.successful(response.readEntity(type))
          : createErrorResponse(path, response);

    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing GET to KSQL server. path:" + path, e);
    }
  }

  public <T> RestResponse<T> deleteRequest(final String path, final Class<T> type) {
    try (Response response = client.target(serverAddress)
        .path(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .delete()) {

      return response.getStatus() == Response.Status.OK.getStatusCode()
          || response.getStatus() == Response.Status.NO_CONTENT.getStatusCode()
          || response.getStatus() == Response.Status.ACCEPTED.getStatusCode()
          ? RestResponse.successful(response.readEntity(type))
          : createErrorResponse(path, response);
    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing DELETE to KSQL server. path:" + path, e);
    }
  }

  public <T> RestResponse<T> postRequest(
      final String path,
      final Object jsonEntity,
      final Optional<Integer> readTimeoutMs,
      final boolean closeResponse,
      final Function<Response, T> mapper) {
    Response response = null;

    try {
      final WebTarget target = client.target(serverAddress)
          .path(path);

      readTimeoutMs.ifPresent(timeout -> target.property(ClientProperties.READ_TIMEOUT, timeout));

      response = target
          .request(MediaType.APPLICATION_JSON_TYPE)
          .post(Entity.json(jsonEntity));

      return response.getStatus() == Response.Status.OK.getStatusCode()
          || response.getStatus() == Response.Status.CREATED.getStatusCode()
          ? RestResponse.successful(mapper.apply(response))
          : createErrorResponse(path, response);

    } catch (final ProcessingException e) {
      if (shouldRetry(readTimeoutMs, e)) {
        return postRequest(path, jsonEntity, calcReadTimeout(readTimeoutMs), closeResponse, mapper);
      }
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    } finally {
      if (response != null && closeResponse) {
        response.close();
      }
    }
  }

  public <T> RestResponse<T> putRequest(final String path, final Class<T> type) {
    try (Response response = client.target(serverAddress)
        .path(path)
        .request()
        .put(Entity.json(""))) {

      return response.getStatus() == Response.Status.OK.getStatusCode()
          || response.getStatus() == Response.Status.ACCEPTED.getStatusCode()
          ? RestResponse.successful(response.readEntity(type))
          : createErrorResponse(path, response);
    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing PUT to KSQL server. path:" + path, e);
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private static Client buildClient(
      final ClientBuilder clientBuilder,
      final SslClientConfigurer sslClientConfigurer,
      final Map<String, String> props
  ) {
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    final JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(objectMapper);

    try {
      clientBuilder.register(jsonProvider);

      sslClientConfigurer.configureSsl(clientBuilder, props);

      return clientBuilder.build();
    } catch (final Exception e) {
      throw new KsqlRestClientException("Failed to configure rest client", e);
    }
  }

  private static <T> RestResponse<T> createErrorResponse(
      final String path,
      final Response response) {

    final KsqlErrorMessage errorMessage = response.readEntity(KsqlErrorMessage.class);
    if (errorMessage != null) {
      return RestResponse.erroneous(errorMessage);
    }

    if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
      return RestResponse.erroneous(404, "Path not found. Path='" + path + "'. "
          + "Check your ksql http url to make sure you are connecting to a ksql server.");
    }

    if (response.getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()) {
      return RestResponse.erroneous(UNAUTHORIZED_ERROR_MESSAGE);
    }

    if (response.getStatus() == Response.Status.FORBIDDEN.getStatusCode()) {
      return RestResponse.erroneous(FORBIDDEN_ERROR_MESSAGE);
    }

    return RestResponse.erroneous(
        Errors.toErrorCode(response.getStatus()),
        "The server returned an unexpected error: "
            + response.getStatusInfo().getReasonPhrase());
  }

  private static boolean shouldRetry(
      final Optional<Integer> readTimeoutMs,
      final ProcessingException e
  ) {
    return readTimeoutMs.map(timeout -> timeout < MAX_TIMEOUT).orElse(false)
        && e.getCause() instanceof SocketTimeoutException;
  }

  private static Optional<Integer> calcReadTimeout(final Optional<Integer> previousTimeoutMs) {
    return previousTimeoutMs.map(timeout -> Math.min(timeout * 2, MAX_TIMEOUT));
  }

  public void setupAuthenticationCredentials(final String userName, final String password) {
    final HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(
        Objects.requireNonNull(userName),
        Objects.requireNonNull(password)
    );
    client.register(feature);
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