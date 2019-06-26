package io.confluent.ksql.rest.client;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.confluent.ksql.rest.entity.ConnectRequest;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.ConnectorList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class KsqlConnectClientTest {
  @Mock
  private RestClient restClient = new RestClient("http://foo");

  private KsqlConnectClient client;

  @Before
  public void init() {
    when(restClient.getRequest(any(), any())).thenReturn(RestResponse.of(""));
    when(restClient.postRequest(any(), any(), any(), anyBoolean(), any())).thenReturn(RestResponse.of(mock(ConnectorInfo.class)));
    when(restClient.deleteRequest(any(), any())).thenReturn(RestResponse.of(""));
    //client = new KsqlConnectClient(restClient);
    client = new KsqlConnectClient("http://localhost:8083");
  }

  @Test
  public void blah() {
    RestResponse<ConnectorInfo> response = client.createNewConnector("ghi", "PAGEVIEWS_FEMALE");
    assertThat(response.getResponse().getName(), is("def"));
  }

  @Test
  public void shouldGetConnectors() {
    client.getConnectors().getResponse();
    verify(restClient).getRequest("/connectors", ConnectorList.class);
  }

  @Test
  public void shouldGetConnectorInfo() {
    client.getConnectorInfo("bar").getResponse();
    verify(restClient).getRequest("/connectors/bar", ConnectorInfo.class);
  }

  @Test
  public void shouldCreateNewConnector() {
    client.createNewConnector("bar", "topic").getResponse();
    final ConnectRequest jsonRequest = new ConnectRequest("bar", "topic");
    verify(restClient).postRequest(eq("/connectors"), eq(jsonRequest), eq(Optional.empty()), eq(true), any());
  }

  @Test
  public void shouldDeleteConnector() {
    client.deleteConnector("bar").getResponse();
    verify(restClient).deleteRequest("/connectors/bar", String.class);
  }
}