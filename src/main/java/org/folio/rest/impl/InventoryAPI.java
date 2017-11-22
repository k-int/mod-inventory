package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.InstanceCollection;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.ExternalStorageModuleInstanceCollection;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Instances;
import org.folio.rest.jaxrs.model.Item;
import org.folio.rest.jaxrs.resource.InventoryResource;
import org.folio.rest.jaxrs.resource.support.ResponseWrapper;
import org.folio.rest.tools.utils.OutStream;

import javax.mail.internet.MimeMultipart;
import javax.ws.rs.core.Response;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.function.Consumer;

public class InventoryAPI implements InventoryResource {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String INSTANCES_PATH = "/inventory/instances";
  private static final String ITEM_STORAGE_PATH = "/item-storage/items";
  private static final String INSTANCE_STORAGE_PATH = "/instance-storage/instances";

  private static final String OKAPI_URL = "X-Okapi-Url";
  private static final String TENANT_HEADER = "X-Okapi-Tenant";
  private static final String TOKEN_HEADER = "X-Okapi-Token";

  @Override
  public void deleteInventoryItems(
    String lang,
    RoutingContext routingContext,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    OkapiHttpClient client = createHttpClient(vertxContext, okapiHeaders);

    CollectionResourceClient itemsClient = new CollectionResourceClient(client,
      okapiBasedUrl(okapiHeaders.get(OKAPI_URL), ITEM_STORAGE_PATH));

    itemsClient.delete(response -> asyncResultHandler.handle(
      Future.succeededFuture(DeleteInventoryItemsResponse.withNoContent())));
  }

  @Override
  public void getInventoryItems(
    int offset,
    int limit,
    String query,
    String lang,
    RoutingContext routingContext,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    notImplemented(asyncResultHandler);
  }

  @Override
  public void postInventoryItems(
    String lang,
    Item entity,
    RoutingContext routingContext,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    notImplemented(asyncResultHandler);
  }

  @Override
  public void getInventoryItemsByItemId(
    String itemId,
    String lang,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    notImplemented(asyncResultHandler);
  }

  @Override
  public void deleteInventoryItemsByItemId(
    String itemId,
    String lang,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    notImplemented(asyncResultHandler);
  }

  @Override
  public void putInventoryItemsByItemId(
    String itemId,
    String lang,
    Item entity,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    notImplemented(asyncResultHandler);
  }

  @Override
  public void deleteInventoryInstances(
    String lang,
    RoutingContext routingContext,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    OkapiHttpClient client = createHttpClient(vertxContext, okapiHeaders);

    CollectionResourceClient instancesClient = new CollectionResourceClient(client,
      okapiBasedUrl(okapiHeaders.get(OKAPI_URL), INSTANCE_STORAGE_PATH));

    instancesClient.delete(response -> respond(asyncResultHandler,
      DeleteInventoryInstancesResponse.withNoContent()));
  }

  @Override
  public void getInventoryInstances(
    int offset,
    int limit,
    String lang,
    RoutingContext routingContext,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    WebContext context = new WebContext(routingContext);

    String search = context.getStringParameter("query", null);

    PagingParameters pagingParameters = PagingParameters.from(context);

    //TODO: Check if this can be reached, as RAML Module Builder might intercept
    if(pagingParameters == null) {
      respond(asyncResultHandler,
        GetInventoryInstancesResponse.withPlainBadRequest(
          "limit and offset must be numeric when supplied"));

      return;
    }

    InstanceCollection storage = getInstanceStorage(vertxContext, okapiHeaders);

    if(search == null) {
      storage.findAll(
        pagingParameters,
        success -> {
          Instances instances = new Instances()
            .withInstances(success.getResult().records)
            .withTotalRecords(success.getResult().totalRecords);

          respond(asyncResultHandler,
            GetInventoryInstancesResponse.withJsonOK(instances));
        },
        forwardFailureOn(asyncResultHandler));
    }
    else {
      storage.findByCql(search,
        pagingParameters, success -> {
          Instances instances = new Instances()
            .withInstances(success.getResult().records)
            .withTotalRecords(success.getResult().totalRecords);

          respond(asyncResultHandler,
            GetInventoryInstancesResponse.withJsonOK(instances));
        },
        forwardFailureOn(asyncResultHandler));
    }
  }

  @Override
  public void postInventoryInstances(
    String lang,
    Instance entity,
    RoutingContext routingContext,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    WebContext context = new WebContext(routingContext);

    //TODO: Check if this can be reached, as RAML Module Builder might intercept
    if(StringUtils.isBlank(entity.getTitle())) {
      respond(asyncResultHandler,
        PostInventoryInstancesResponse.withPlainBadRequest(
        "Title must be provided for an instance"));
      return;
    }

    InstanceCollection storage = getInstanceStorage(vertxContext, okapiHeaders);

    storage.add(entity,
      success -> {
        try {
          URL url = context.absoluteUrl(String.format("%s/%s",
            INSTANCES_PATH, success.getResult().getId()));

          OutStream stream = new OutStream();
          stream.setData(success.getResult());

          respond(asyncResultHandler,
              PostInventoryInstancesResponse.withJsonCreated(url.toString(),
                stream));
        } catch (MalformedURLException e) {
          String message = String.format(
            "Failed to create URL for location header: %s", e.toString());

          log.warn(message);
          respond(asyncResultHandler,
            PostInventoryInstancesResponse.withPlainInternalServerError(message));
        }
      }, forwardFailureOn(asyncResultHandler));
  }

  @Override
  public void getInventoryInstancesByInstanceId(
    String instanceId,
    String lang,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    InstanceCollection storage = getInstanceStorage(vertxContext, okapiHeaders);

    storage.findById(
      instanceId,
      it -> {
        Instance instance = it.getResult();

        if(instance != null) {
          respond(asyncResultHandler,
            GetInventoryInstancesByInstanceIdResponse.withJsonOK(instance));
        }
        else {
          respond(asyncResultHandler, GetInventoryInstancesByInstanceIdResponse
            .withPlainNotFound("Not Found"));
        }
      }, forwardFailureOn(asyncResultHandler));
  }

  @Override
  public void deleteInventoryInstancesByInstanceId(
    String instanceId,
    String lang,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    OkapiHttpClient client = createHttpClient(vertxContext, okapiHeaders);

    CollectionResourceClient instancesClient = new CollectionResourceClient(client,
      okapiBasedUrl(okapiHeaders.get(OKAPI_URL), INSTANCE_STORAGE_PATH));

    instancesClient.delete(instanceId, response -> respond(asyncResultHandler,
        DeleteInventoryInstancesByInstanceIdResponse.withNoContent()));
  }

  @Override
  public void putInventoryInstancesByInstanceId(
    String instanceId,
    String lang,
    Instance entity,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    InstanceCollection storage = getInstanceStorage(vertxContext, okapiHeaders);

    storage.findById(instanceId,
      it -> {
        if(it.getResult() != null) {
          storage.update(entity,
            v -> respond(asyncResultHandler,
              PutInventoryInstancesByInstanceIdResponse.withNoContent()),
            forwardFailureOn(asyncResultHandler));
        }
        else {
          respond(asyncResultHandler,
            PutInventoryInstancesByInstanceIdResponse.withPlainNotFound("Not Found"));
        }
      }, forwardFailureOn(asyncResultHandler));
  }

  @Override
  public void postInventoryIngestMods(
    MimeMultipart entity,
    RoutingContext routingContext,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    notImplemented(asyncResultHandler);
  }

  @Override
  public void getInventoryIngestModsStatusById(
    String id,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    notImplemented(asyncResultHandler);
  }

  private OkapiHttpClient createHttpClient(
    Context context,
    Map<String, String> okapiHeaders)
    throws MalformedURLException {

    return new OkapiHttpClient(context.owner().createHttpClient(),
      new URL(okapiHeaders.get(OKAPI_URL)), okapiHeaders.get(TENANT_HEADER),
      okapiHeaders.get(TOKEN_HEADER),
      exception ->
        log.error("Error occurred when making request to storage module", exception));
  }

  private URL okapiBasedUrl(String okapiUrl, String path)
    throws MalformedURLException {

    URL currentRequestUrl = new URL(okapiUrl);

    return new URL(currentRequestUrl.getProtocol(), currentRequestUrl.getHost(),
      currentRequestUrl.getPort(), path);
  }

  private Response convertResponseToJax(Failure failure) {
    return Response
      .status(failure.getStatusCode())
      .header("Content-Type", failure.getContentType())
      .entity(failure.getReason())
      .build();
  }

  private Consumer<Failure> forwardFailureOn(
    Handler<AsyncResult<Response>> asyncResultHandler) {

    return f -> asyncResultHandler.handle(Future.succeededFuture(
      convertResponseToJax(f)));
  }

  private void notImplemented(
    Handler<AsyncResult<Response>> asyncResultHandler) {

    Response.ResponseBuilder responseBuilder = Response.status(501)
      .header("Content-Type", "text/plain");

    responseBuilder.entity("Not Implemented");

    asyncResultHandler.handle(Future.succeededFuture(responseBuilder.build()));
  }

  private ExternalStorageModuleInstanceCollection getInstanceStorage(
    Context vertxContext,
    Map<String, String> okapiHeaders) {

    return new ExternalStorageModuleInstanceCollection(vertxContext.owner(),
      okapiHeaders.get(OKAPI_URL), okapiHeaders.get(TENANT_HEADER),
      okapiHeaders.get(TOKEN_HEADER));
  }

  private void respond(
    Handler<AsyncResult<Response>> asyncResultHandler,
    ResponseWrapper response) {

    asyncResultHandler.handle(Future.succeededFuture(response));
  }
}
