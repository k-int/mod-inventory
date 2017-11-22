package api;

import api.support.ApiRoot;
import api.support.InstanceApiClient;
import api.support.Preparation;
import com.github.jsonldjava.core.JsonLdError;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static api.support.InstanceSamples.*;
import static api.support.ValidationErrorMatchers.errorFor;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class InstancesApiExamples {
  private final OkapiHttpClient okapiClient;

  public InstancesApiExamples() throws MalformedURLException {
    okapiClient = ApiTestSuite.createOkapiHttpClient();
  }

  @Before
  public void setup()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    new Preparation(okapiClient).deleteInstances();
  }

  @Test
  public void canCreateAnInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException,
    JsonLdError {

    JsonObject newInstanceRequest = new JsonObject()
      .put("title", "Long Way to a Small Angry Planet")
      .put("identifiers", new JsonArray().add(new JsonObject()
        .put("identifierTypeId", ApiTestSuite.getIsbnIdentifierType())
        .put("value", "9781473619777")))
      .put("creators", new JsonArray().add(new JsonObject()
        .put("creatorTypeId", ApiTestSuite.getPersonalCreatorType())
        .put("name", "Chambers, Becky")))
      .put("source", "Local")
      .put("instanceTypeId", ApiTestSuite.getBooksInstanceType());

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    okapiClient.post(ApiRoot.instances(),
      newInstanceRequest, ResponseHandler.any(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    String location = postResponse.getLocation();

    assertThat(postResponse.getStatusCode(), is(201));
    assertThat(location, is(notNullValue()));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(location, ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));

    JsonObject createdInstance = getResponse.getJson();

    assertThat(createdInstance.containsKey("id"), is(true));
    assertThat(createdInstance.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdInstance.getString("source"), is("Local"));
    assertThat(createdInstance.getString("instanceTypeId"), is(ApiTestSuite.getBooksInstanceType()));

    JsonObject firstIdentifier = createdInstance.getJsonArray("identifiers")
      .getJsonObject(0);

    assertThat(firstIdentifier.getString("identifierTypeId"),
      is(ApiTestSuite.getIsbnIdentifierType()));

    assertThat(firstIdentifier.getString("value"), is("9781473619777"));

    JsonObject firstCreator = createdInstance.getJsonArray("creators")
      .getJsonObject(0);

    assertThat(firstCreator.getString("creatorTypeId"),
      is(ApiTestSuite.getPersonalCreatorType()));

    assertThat(firstCreator.getString("name"), is("Chambers, Becky"));
  }

  @Test
  public void canCreateAnInstanceWithAnID()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException,
    JsonLdError {

    String instanceId = UUID.randomUUID().toString();

    JsonObject newInstanceRequest = new JsonObject()
      .put("id", instanceId)
      .put("title", "Long Way to a Small Angry Planet")
      .put("identifiers", new JsonArray().add(new JsonObject()
      .put("identifierTypeId", ApiTestSuite.getIsbnIdentifierType())
      .put("value", "9781473619777")))
      .put("creators", new JsonArray().add(new JsonObject()
      .put("creatorTypeId", ApiTestSuite.getPersonalCreatorType())
      .put("name", "Chambers, Becky")))
      .put("source", "Local")
      .put("instanceTypeId", ApiTestSuite.getBooksInstanceType());

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    okapiClient.post(ApiRoot.instances(),
      newInstanceRequest, ResponseHandler.any(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    String location = postResponse.getLocation();

    assertThat(postResponse.getStatusCode(), is(201));
    assertThat(location, is(notNullValue()));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(location, ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));

    JsonObject createdInstance = getResponse.getJson();

    assertThat(createdInstance.containsKey("id"), is(true));
    assertThat(createdInstance.getString("title"), is("Long Way to a Small Angry Planet"));
    assertThat(createdInstance.getString("source"), is("Local"));
    assertThat(createdInstance.getString("instanceTypeId"), is(ApiTestSuite.getBooksInstanceType()));

    JsonObject firstIdentifier = createdInstance.getJsonArray("identifiers")
      .getJsonObject(0);

    assertThat(firstIdentifier.getString("identifierTypeId"),
      is(ApiTestSuite.getIsbnIdentifierType()));

    assertThat(firstIdentifier.getString("value"), is("9781473619777"));

    JsonObject firstCreator = createdInstance.getJsonArray("creators")
      .getJsonObject(0);

    assertThat(firstCreator.getString("creatorTypeId"),
      is(ApiTestSuite.getPersonalCreatorType()));

    assertThat(firstCreator.getString("name"), is("Chambers, Becky"));
  }

  @Test
  public void instanceTitleIsMandatory()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject newInstanceRequest = new JsonObject()
      .put("identifiers", new JsonArray().add(new JsonObject()
        .put("identifierTypeId", ApiTestSuite.getIsbnIdentifierType())
        .put("value", "9781473619777")))
      .put("creators", new JsonArray().add(new JsonObject()
        .put("creatorTypeId", ApiTestSuite.getPersonalCreatorType())
        .put("name", "Chambers, Becky")))
      .put("source", "Local")
      .put("instanceTypeId", ApiTestSuite.getBooksInstanceType());

    CompletableFuture<Response> postCompleted = new CompletableFuture<>();

    okapiClient.post(ApiRoot.instances(),
      newInstanceRequest, ResponseHandler.any(postCompleted));

    Response postResponse = postCompleted.get(5, TimeUnit.SECONDS);

    assertThat(postResponse.getStatusCode(), anyOf(is(400), is(422)));
    assertThat(postResponse.getLocation(), is(nullValue()));

    if(postResponse.getStatusCode() == 422) {
      assertThat(JsonArrayHelper.toList(postResponse.getJson().getJsonArray("errors")),
        contains(errorFor("title", "may not be null")));
    }
    else {
      assertThat(postResponse.getBody(), is("Title must be provided for an instance"));
    }
  }

  @Test
  public void canUpdateAnExistingInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    UUID id = UUID.randomUUID();

    JsonObject newInstance = createInstance(smallAngryPlanet(id));

    JsonObject updateInstanceRequest = smallAngryPlanet(id)
      .put("title", "The Long Way to a Small, Angry Planet");

    URL instanceLocation = new URL(String.format("%s/%s", ApiRoot.instances(),
      newInstance.getString("id")));

    CompletableFuture<Response> putCompleted = new CompletableFuture<>();

    okapiClient.put(instanceLocation, updateInstanceRequest,
      ResponseHandler.any(putCompleted));

    Response putResponse = putCompleted.get(5, TimeUnit.SECONDS);

    assertThat(putResponse.getStatusCode(), is(204));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(instanceLocation, ResponseHandler.json(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(200));

    JsonObject updatedInstance = getResponse.getJson();

    assertThat(updatedInstance.getString("id"), is(newInstance.getString("id")));
    assertThat(updatedInstance.getString("title"), is("The Long Way to a Small, Angry Planet"));
    assertThat(updatedInstance.getJsonArray("identifiers").size(), is(1));
  }

  @Test
  public void cannotUpdateAnInstanceThatDoesNotExist()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    JsonObject updateInstanceRequest = smallAngryPlanet(UUID.randomUUID());

    CompletableFuture<Response> putCompleted = new CompletableFuture<>();

    URL instanceLocation = new URL(String.format("%s/%s", ApiRoot.instances(),
      updateInstanceRequest.getString("id")));

    okapiClient.put(instanceLocation, updateInstanceRequest,
        ResponseHandler.any(putCompleted));

    Response putResponse = putCompleted.get(5, TimeUnit.SECONDS);

    assertThat(putResponse.getStatusCode(), is(404));
  }

  @Test
  public void canDeleteAllInstances()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(leviathanWakes(UUID.randomUUID()));

    CompletableFuture<Response> deleteCompleted = new CompletableFuture<Response>();

    okapiClient.delete(ApiRoot.instances(), ResponseHandler.any(deleteCompleted));

    Response deleteResponse = deleteCompleted.get(5, TimeUnit.SECONDS);

    assertThat(deleteResponse.getStatusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("instances").size(), is(0));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(0));
  }

  @Test
  public void canDeleteAnInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));

    JsonObject instanceToDelete = createInstance(leviathanWakes(UUID.randomUUID()));

    URL instanceToDeleteLocation = new URL(String.format("%s/%s",
      ApiRoot.instances(), instanceToDelete.getString("id")));

    CompletableFuture<Response> deleteCompleted = new CompletableFuture<>();

    okapiClient.delete(instanceToDeleteLocation,
      ResponseHandler.any(deleteCompleted));

    Response deleteResponse = deleteCompleted.get(5, TimeUnit.SECONDS);

    assertThat(deleteResponse.getStatusCode(), is(204));
    assertThat(deleteResponse.hasBody(), is(false));

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(instanceToDeleteLocation, ResponseHandler.any(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(404));

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getAllResponse.getJson().getJsonArray("instances").size(), is(2));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(2));
  }

  @Test
  public void canGetAllInstances()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(temeraire(UUID.randomUUID()));

    CompletableFuture<Response> getAllCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances(), ResponseHandler.json(getAllCompleted));

    Response getAllResponse = getAllCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getAllResponse.getStatusCode(), is(200));

    List<JsonObject> instances = JsonArrayHelper.toList(
      getAllResponse.getJson().getJsonArray("instances"));

    assertThat(instances.size(), is(3));
    assertThat(getAllResponse.getJson().getInteger("totalRecords"), is(3));

    hasCollectionProperties(instances);
  }

  @Test
  public void canPageAllInstances()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(temeraire(UUID.randomUUID()));
    createInstance(leviathanWakes(UUID.randomUUID()));
    createInstance(taoOfPooh(UUID.randomUUID()));

    CompletableFuture<Response> firstPageGetCompleted = new CompletableFuture<>();
    CompletableFuture<Response> secondPageGetCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances("limit=3"),
      ResponseHandler.json(firstPageGetCompleted));

    okapiClient.get(ApiRoot.instances("limit=3&offset=3"),
      ResponseHandler.json(secondPageGetCompleted));

    Response firstPageResponse = firstPageGetCompleted.get(5, TimeUnit.SECONDS);
    Response secondPageResponse = secondPageGetCompleted.get(5, TimeUnit.SECONDS);

    assertThat(firstPageResponse.getStatusCode(), is(200));
    assertThat(secondPageResponse.getStatusCode(), is(200));

    List<JsonObject> firstPageInstances = JsonArrayHelper.toList(
      firstPageResponse.getJson().getJsonArray("instances"));

    assertThat(firstPageInstances.size(), is(3));
    assertThat(firstPageResponse.getJson().getInteger("totalRecords"), is(5));

    hasCollectionProperties(firstPageInstances);

    List<JsonObject> secondPageInstances = JsonArrayHelper.toList(
      secondPageResponse.getJson().getJsonArray("instances"));

    assertThat(secondPageInstances.size(), is(2));
    assertThat(secondPageResponse.getJson().getInteger("totalRecords"), is(5));

    hasCollectionProperties(secondPageInstances);
  }

  @Test
  public void pageParametersMustBeNumeric()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    CompletableFuture<Response> getPagedCompleted = new CompletableFuture<>();

    //Response from RAML module builder does not supply a content type for error
    okapiClient.get(ApiRoot.instances("limit=&offset="),
      ResponseHandler.any(getPagedCompleted));

    Response getPagedResponse = getPagedCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getPagedResponse.getStatusCode(), is(400));
    assertThat(getPagedResponse.getBody(),
      anyOf(is("limit and offset must be numeric when supplied"),
        is("offset does not have a default value in the RAML and has been passed empty")));
  }

  @Test
  public void canSearchForInstancesByTitle()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    createInstance(smallAngryPlanet(UUID.randomUUID()));
    createInstance(nod(UUID.randomUUID()));
    createInstance(uprooted(UUID.randomUUID()));

    CompletableFuture<Response> searchGetCompleted = new CompletableFuture<>();

    okapiClient.get(ApiRoot.instances("query=title=*Small%20Angry*"),
      ResponseHandler.json(searchGetCompleted));

    Response searchGetResponse = searchGetCompleted.get(5, TimeUnit.SECONDS);

    assertThat(searchGetResponse.getStatusCode(), is(200));

    List<JsonObject> instances = JsonArrayHelper.toList(
      searchGetResponse.getJson().getJsonArray("instances"));

    assertThat(instances.size(), is(1));
    assertThat(searchGetResponse.getJson().getInteger("totalRecords"), is(1));
    assertThat(instances.get(0).getString("title"), is("Long Way to a Small Angry Planet"));

    hasCollectionProperties(instances);
  }

  @Test
  public void cannotFindAnUnknownInstance()
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    CompletableFuture<Response> getCompleted = new CompletableFuture<>();

    okapiClient.get(String.format("%s/%s", ApiRoot.instances(), UUID.randomUUID()),
      ResponseHandler.any(getCompleted));

    Response getResponse = getCompleted.get(5, TimeUnit.SECONDS);

    assertThat(getResponse.getStatusCode(), is(404));
  }

  private void hasCollectionProperties(List<JsonObject> instances) {
    instances.forEach(instance -> {
      assertThat(instance.containsKey("id"), is(true));
      assertThat(instance.containsKey("title"), is(true));
      assertThat(instance.containsKey("source"), is(true));
      assertThat(instance.containsKey("instanceTypeId"), is(true));

      assertThat(instance.containsKey("identifiers"), is(true));
      assertThat(instance.getJsonArray("identifiers").size(), is(greaterThan(0)));

      assertThat(instance.containsKey("creators"), is(true));
      assertThat(instance.getJsonArray("creators").size(), is(greaterThan(0)));
    });
  }

  private JsonObject createInstance(JsonObject newInstanceRequest)
    throws InterruptedException,
    MalformedURLException,
    TimeoutException,
    ExecutionException {

    return InstanceApiClient.createInstance(okapiClient, newInstanceRequest);
  }
}
