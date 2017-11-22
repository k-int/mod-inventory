package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.InstanceCollection;
import org.folio.inventory.domain.ItemCollection;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.ExternalStorageModuleInstanceCollection;
import org.folio.inventory.storage.external.ExternalStorageModuleItemCollection;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.rest.jaxrs.model.*;
import org.folio.rest.jaxrs.resource.InventoryResource;
import org.folio.rest.jaxrs.resource.support.ResponseWrapper;
import org.folio.rest.tools.utils.OutStream;

import javax.mail.internet.MimeMultipart;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.folio.inventory.common.FutureAssistance.allOf;

public class InventoryAPI implements InventoryResource {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String INSTANCES_PATH = "/inventory/instances";
  private static final String ITEMS_PATH = "/inventory/items";
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
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
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
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
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

    ItemCollection storage = getItemStorage(vertxContext, okapiHeaders);

    if(search == null) {
      storage.findAll(
        pagingParameters,
        success -> respondWithManyItems(asyncResultHandler, vertxContext,
          okapiHeaders, success.getResult()),
        forwardFailureOn(asyncResultHandler));
    }
    else {
      storage.findByCql(search,
        pagingParameters, success ->
          respondWithManyItems(asyncResultHandler, vertxContext, okapiHeaders,
            success.getResult()),
        forwardFailureOn(asyncResultHandler));
    }
  }

  @Override
  public void postInventoryItems(
    String lang,
    Item entity,
    RoutingContext routingContext,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    WebContext context = new WebContext(routingContext);

    ItemCollection itemCollection = getItemStorage(vertxContext, okapiHeaders);

    if(entity.getBarcode() != null) {
      try {
        itemCollection.findByCql(String.format("barcode=%s", entity.getBarcode()),
          PagingParameters.defaults(), findResult -> {

            if(findResult.getResult().records.isEmpty()) {
              addItem(context, entity, itemCollection,
                okapiHeaders, vertxContext, asyncResultHandler);
            }
            else {
              respond(asyncResultHandler,
                PostInventoryItemsResponse.withPlainBadRequest(
                String.format("Barcode must be unique, %s is already assigned to another item",
                  entity.getBarcode())));
            }
          }, forwardFailureOn(asyncResultHandler));
      } catch (UnsupportedEncodingException e) {
        respond(asyncResultHandler,
          PostInventoryItemsResponse.withPlainInternalServerError(e.toString()));
      }
    }
    else {
      addItem(context, entity, itemCollection, okapiHeaders,
        vertxContext, asyncResultHandler);
    }
  }

  @Override
  public void getInventoryItemsByItemId(
    String itemId,
    String lang,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;

    try {
      OkapiHttpClient client = createHttpClient(vertxContext, okapiHeaders);
      materialTypesClient = createMaterialTypesClient(client, okapiHeaders);
      loanTypesClient = createLoanTypesClient(client, okapiHeaders);
      locationsClient = createLocationsClient(client, okapiHeaders);
    }
    catch (MalformedURLException e) {
      respond(asyncResultHandler,
        GetInventoryItemsByItemIdResponse.withPlainInternalServerError(e.toString()));

      return;
    }

    getItemStorage(vertxContext, okapiHeaders).findById(
      itemId,
      (Success<Item> itemResponse) -> {
        Item item = itemResponse.getResult();

        if(item != null) {
          respondWithItem(itemResponse.getResult(), materialTypesClient
            , loanTypesClient, locationsClient, representation ->
              respond(asyncResultHandler,
                GetInventoryItemsByItemIdResponse.withJsonOK(representation)),
            f -> respond(asyncResultHandler,
              GetInventoryItemsByItemIdResponse.withPlainInternalServerError(f)));
        }
        else {
          respond(asyncResultHandler,
            GetInventoryItemsByItemIdResponse.withPlainNotFound("Not Found"));
        }
      }, forwardFailureOn(asyncResultHandler));

  }

  @Override
  public void deleteInventoryItemsByItemId(
    String itemId,
    String lang,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    OkapiHttpClient client = createHttpClient(vertxContext, okapiHeaders);

    CollectionResourceClient itemsClient = new CollectionResourceClient(client,
      okapiBasedUrl(okapiHeaders.get(OKAPI_URL), ITEM_STORAGE_PATH));

    itemsClient.delete(itemId, response -> respond(asyncResultHandler,
      DeleteInventoryItemsByItemIdResponse.withNoContent()));
  }

  @Override
  public void putInventoryItemsByItemId(
    String itemId,
    String lang,
    Item entity,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    ItemCollection storage = getItemStorage(vertxContext, okapiHeaders);

    storage.findById(itemId,
      it -> {
        if(it.getResult() != null) {
          if(hasSameBarcode(entity, it.getResult())) {
            updateItem(asyncResultHandler, entity, storage);
          } else {
            try {
              checkForNonUniqueBarcode(asyncResultHandler, entity, storage);
            } catch (UnsupportedEncodingException e) {
              respond(asyncResultHandler,
                PutInventoryItemsByItemIdResponse.withPlainInternalServerError(
                  e.toString()));
            }
          }
        }
        else {
          respond(asyncResultHandler,
            PutInventoryItemsByItemIdResponse.withPlainNotFound("Not Found"));
        }
      }, forwardFailureOn(asyncResultHandler));
  }

  @Override
  public void deleteInventoryInstances(
    String lang,
    RoutingContext routingContext,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
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
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
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
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
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
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
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
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
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
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
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
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
    Context vertxContext) throws Exception {

    notImplemented(asyncResultHandler);
  }

  @Override
  public void getInventoryIngestModsStatusById(
    String id,
    Map<String, String> okapiHeaders,
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
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

  private javax.ws.rs.core.Response convertResponseToJax(Failure failure) {
    return javax.ws.rs.core.Response
      .status(failure.getStatusCode())
      .header(HttpHeaders.CONTENT_TYPE.toString(), failure.getContentType())
      .entity(failure.getReason())
      .build();
  }

  private Consumer<Failure> forwardFailureOn(
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler) {

    return f -> asyncResultHandler.handle(Future.succeededFuture(
      convertResponseToJax(f)));
  }

  private void notImplemented(
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler) {

    asyncResultHandler.handle(Future.succeededFuture(
      javax.ws.rs.core.Response
        .status(501)
        .header(HttpHeaders.CONTENT_TYPE.toString(), "text/plain")
        .entity("Not Implemented")
        .build()));
  }

  private InstanceCollection getInstanceStorage(
    Context vertxContext,
    Map<String, String> okapiHeaders) {

    return new ExternalStorageModuleInstanceCollection(vertxContext.owner(),
      okapiHeaders.get(OKAPI_URL), okapiHeaders.get(TENANT_HEADER),
      okapiHeaders.get(TOKEN_HEADER));
  }

  private ItemCollection getItemStorage(
    Context vertxContext,
    Map<String, String> okapiHeaders) {

    return new ExternalStorageModuleItemCollection(vertxContext.owner(),
      okapiHeaders.get(OKAPI_URL), okapiHeaders.get(TENANT_HEADER),
      okapiHeaders.get(TOKEN_HEADER));
  }

  private void respond(
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
    ResponseWrapper response) {

    asyncResultHandler.handle(Future.succeededFuture(response));
  }

  private void addItem(
    WebContext webContext,
    Item newItem,
    ItemCollection itemCollection,
    Map<String, String> okapiHeaders,
    Context context,
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler) {

    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;

    try {
      OkapiHttpClient client = createHttpClient(context, okapiHeaders);
      materialTypesClient = createMaterialTypesClient(client, okapiHeaders);
      loanTypesClient = createLoanTypesClient(client, okapiHeaders);
      locationsClient = createLocationsClient(client, okapiHeaders);
    }
    catch (MalformedURLException e) {
      respond(asyncResultHandler,
        PostInventoryItemsResponse.withPlainInternalServerError(e.toString()));

      return;
    }

    itemCollection.add(newItem, success -> {
      try {
        URL location = webContext.absoluteUrl(String.format("%s/%s",
          ITEMS_PATH, success.getResult().getId()));

        OutStream stream = new OutStream();
        stream.setData(success.getResult());

        respondWithItem(success.getResult(), materialTypesClient
          , loanTypesClient, locationsClient, representation ->
            respond(asyncResultHandler,
              PostInventoryItemsResponse.withJsonCreated(location.toString(), stream)),
          f -> respond(asyncResultHandler,
            PostInventoryItemsResponse.withPlainInternalServerError(f)));
      } catch (MalformedURLException e) {
        log.warn(String.format("Failed to create self link for item: %s", e.toString()));
      }
    }, forwardFailureOn(asyncResultHandler));
  }

  private void respondWithItem(
    Item item,
    CollectionResourceClient materialTypesClient,
    CollectionResourceClient loanTypesClient,
    CollectionResourceClient locationsClient,
    Consumer<Item> responder,
    Consumer<String> failureResponder) {

    ArrayList<CompletableFuture<Response>> allFutures = new ArrayList<>();

    CompletableFuture<Response> materialTypeFuture = getReferenceRecord(
      getMaterialTypeId(item), materialTypesClient, allFutures);

    CompletableFuture<Response> permanentLoanTypeFuture = getReferenceRecord(
      getPermanentLoanTypeId(item), loanTypesClient, allFutures);

    CompletableFuture<Response> temporaryLoanTypeFuture = getReferenceRecord(
      getTemporaryLoanTypeId(item), loanTypesClient, allFutures);

    CompletableFuture<Response> permanentLocationFuture = getReferenceRecord(
      getPermanentLocationId(item), locationsClient, allFutures);

    CompletableFuture<Response> temporaryLocationFuture = getReferenceRecord(
      getTemporaryLocationId(item), locationsClient, allFutures);

    CompletableFuture<Void> allDoneFuture = allOf(allFutures);

    allDoneFuture.thenAccept(v -> {
      try {
        Item extendedItem = includeReferenceRecordInformationInItem(
          item, materialTypeFuture, permanentLoanTypeFuture,
          temporaryLoanTypeFuture, temporaryLocationFuture, permanentLocationFuture);

        responder.accept(extendedItem);
      } catch (Exception e) {
        failureResponder.accept(e.toString());
      }
    });
  }

  private Item includeReferenceRecordInformationInItem(
    Item item,
    CompletableFuture<Response> materialTypeFuture,
    CompletableFuture<Response> permanentLoanTypeFuture,
    CompletableFuture<Response> temporaryLoanTypeFuture,
    CompletableFuture<Response> temporaryLocationFuture,
    CompletableFuture<Response> permanentLocationFuture) {

    JsonObject foundMaterialType = referenceRecordFrom(
      getMaterialTypeId(item), materialTypeFuture);

    JsonObject foundPermanentLoanType = referenceRecordFrom(
      getPermanentLoanTypeId(item), permanentLoanTypeFuture);

    JsonObject foundTemporaryLoanType = referenceRecordFrom(
      getTemporaryLoanTypeId(item), temporaryLoanTypeFuture);

    JsonObject foundPermanentLocation = referenceRecordFrom(
      getPermanentLocationId(item), permanentLocationFuture);

    JsonObject foundTemporaryLocation = referenceRecordFrom(
      getTemporaryLocationId(item), temporaryLocationFuture);

    return addReferenceInformationToItem(item,
      foundMaterialType,
      foundPermanentLoanType,
      foundTemporaryLoanType,
      foundPermanentLocation,
      foundTemporaryLocation);
  }

  private Item addReferenceInformationToItem(
    Item item,
    JsonObject foundMaterialType,
    JsonObject foundPermanentLoanType,
    JsonObject foundTemporaryLoanType,
    JsonObject foundPermanentLocation,
    JsonObject foundTemporaryLocation) {

    if(foundMaterialType != null) {
      item.getMaterialType().setName(foundMaterialType.getString("name"));
    }

    if(foundPermanentLoanType != null) {
      item.getPermanentLoanType().setName(foundPermanentLoanType.getString("name"));
    }

    if(foundTemporaryLoanType != null) {
      item.getTemporaryLoanType().setName(foundTemporaryLoanType.getString("name"));
    }

    if(foundPermanentLocation != null) {
      item.getPermanentLocation().setName(foundPermanentLocation.getString("name"));
    }

    if(foundTemporaryLocation != null) {
      item.getTemporaryLocation().setName(foundTemporaryLocation.getString("name"));
    }

    return item;
  }

  private JsonObject referenceRecordFrom(
    String id,
    CompletableFuture<Response> requestFuture) {

    return id != null &&
      requestFuture.join().getStatusCode() == 200 ?
      requestFuture.join().getJson() : null;
  }

  private CollectionResourceClient createMaterialTypesClient(
    OkapiHttpClient client,
    Map<String, String> okapiHeaders)
    throws MalformedURLException {

    return createCollectionResourceClient(client, okapiHeaders,
      "/material-types");
  }

  private CollectionResourceClient createLoanTypesClient(
    OkapiHttpClient client,
    Map<String, String> okapiHeaders)
    throws MalformedURLException {

    return createCollectionResourceClient(client, okapiHeaders,
      "/loan-types");
  }

  private CollectionResourceClient createLocationsClient(
    OkapiHttpClient client,
    Map<String, String> okapiHeaders)
    throws MalformedURLException {

    return createCollectionResourceClient(client, okapiHeaders,
      "/shelf-locations");
  }

  private CollectionResourceClient createCollectionResourceClient(
    OkapiHttpClient client,
    Map<String, String> okapiHeaders,
    String rootPath)
    throws MalformedURLException {

    return new CollectionResourceClient(client,
      new URL(okapiHeaders.get(OKAPI_URL) + rootPath));
  }

  private CompletableFuture<Response> getReferenceRecord(
    String id, CollectionResourceClient client,
    ArrayList<CompletableFuture<Response>> allFutures) {

    CompletableFuture<Response> newFuture = new CompletableFuture<>();

    if(id != null) {
      allFutures.add(newFuture);

      client.get(id, newFuture::complete);

      return newFuture;
    }
    else {
      return null;
    }
  }

  private String getMaterialTypeId(Item item) {
    MaterialType materialType = item.getMaterialType();

    return materialType == null
      ? null
      : materialType.getId();
  }

  private String getPermanentLocationId(Item item) {
    PermanentLocation permanentLocation = item.getPermanentLocation();

    return permanentLocation == null
      ? null
      : permanentLocation.getId();
  }

  private String getTemporaryLocationId(Item item) {
    TemporaryLocation temporaryLocation = item.getTemporaryLocation();

    return temporaryLocation == null
      ? null
      : temporaryLocation.getId();
  }

  private String getTemporaryLoanTypeId(Item item) {
    TemporaryLoanType temporaryLoanType = item.getTemporaryLoanType();

    return temporaryLoanType == null
      ? null
      : temporaryLoanType.getId();
  }

  private String getPermanentLoanTypeId(Item item) {
    PermanentLoanType permanentLoanType = item.getPermanentLoanType();

    return permanentLoanType == null
      ? null
      : permanentLoanType.getId();
  }

  private boolean hasSameBarcode(Item updatedItem, Item foundItem) {
    return updatedItem.getBarcode() == null
      || StringUtils.equals(foundItem.getBarcode(), updatedItem.getBarcode());
  }

  private void updateItem(
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
    Item updatedItem,
    ItemCollection itemCollection) {

    itemCollection.update(updatedItem,
      v -> respond(asyncResultHandler,
        PutInventoryItemsByItemIdResponse.withNoContent()),
      forwardFailureOn(asyncResultHandler));
  }

  private void checkForNonUniqueBarcode(
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
    Item updatedItem,
    ItemCollection itemCollection)
    throws UnsupportedEncodingException {

    itemCollection.findByCql(
      String.format("barcode=%s and id<>%s", updatedItem.getBarcode(), updatedItem.getId()),
      PagingParameters.defaults(), it -> {

        List<Item> items = it.getResult().records;

        if(items.isEmpty()) {
          updateItem(asyncResultHandler, updatedItem, itemCollection);
        }
        else {
          respond(asyncResultHandler,
            PutInventoryItemsByItemIdResponse.withPlainBadRequest(
              String.format("Barcode must be unique, %s is already assigned to another item",
                updatedItem.getBarcode())));
        }
      }, forwardFailureOn(asyncResultHandler));
  }

  private void respondWithManyItems(
    Handler<AsyncResult<javax.ws.rs.core.Response>> asyncResultHandler,
    Context vertxContext,
    Map<String, String> okapiHeaders,
    MultipleRecords<Item> wrappedItems) {

    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;

    try {
      OkapiHttpClient client = createHttpClient(vertxContext, okapiHeaders);
      materialTypesClient = createMaterialTypesClient(client, okapiHeaders);
      loanTypesClient = createLoanTypesClient(client, okapiHeaders);
      locationsClient = createLocationsClient(client, okapiHeaders);
    }
    catch (MalformedURLException e) {
      respond(asyncResultHandler,
        GetInventoryItemsResponse.withPlainInternalServerError(e.toString()));

      return;
    }

    ArrayList<CompletableFuture<Response>> allMaterialTypeFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allLoanTypeFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allLocationsFutures = new ArrayList<>();
    ArrayList<CompletableFuture<Response>> allFutures = new ArrayList<>();

    List<String> materialTypeIds = wrappedItems.records.stream()
      .map(Item::getMaterialType)
      .filter(Objects::nonNull)
      .map(MaterialType::getId)
      .filter(Objects::nonNull)
      .distinct()
      .collect(Collectors.toList());

    materialTypeIds.stream().forEach(id -> {
      CompletableFuture<Response> newFuture = new CompletableFuture<>();

      allFutures.add(newFuture);
      allMaterialTypeFutures.add(newFuture);

      materialTypesClient.get(id, newFuture::complete);
    });

    List<String> permanentLoanTypeIds = wrappedItems.records.stream()
      .map(Item::getPermanentLoanType)
      .filter(Objects::nonNull)
      .map(PermanentLoanType::getId)
      .filter(Objects::nonNull)
      .distinct()
      .collect(Collectors.toList());

    List<String> temporaryLoanTypeIds = wrappedItems.records.stream()
      .map(Item::getTemporaryLoanType)
      .filter(Objects::nonNull)
      .map(TemporaryLoanType::getId)
      .filter(Objects::nonNull)
      .distinct()
      .collect(Collectors.toList());

    Stream.concat(permanentLoanTypeIds.stream(), temporaryLoanTypeIds.stream())
      .distinct()
      .forEach(id -> {

        CompletableFuture<Response> newFuture = new CompletableFuture<>();

        allFutures.add(newFuture);
        allLoanTypeFutures.add(newFuture);

        loanTypesClient.get(id, newFuture::complete);
      });

    List<String> permanentLocationIds = wrappedItems.records.stream()
      .map(Item::getPermanentLocation)
      .filter(Objects::nonNull)
      .map(PermanentLocation::getId)
      .distinct()
      .collect(Collectors.toList());

    List<String> temporaryLocationIds = wrappedItems.records.stream()
      .map(Item::getTemporaryLocation)
      .filter(Objects::nonNull)
      .map(TemporaryLocation::getId)
      .distinct()
      .collect(Collectors.toList());

    Stream.concat(permanentLocationIds.stream(), temporaryLocationIds.stream())
      .distinct()
      .forEach(id -> {
        CompletableFuture<Response> newFuture = new CompletableFuture<>();

        allFutures.add(newFuture);
        allLocationsFutures.add(newFuture);

        locationsClient.get(id, newFuture::complete);
      });

    CompletableFuture<Void> allDoneFuture = allOf(allFutures);

    allDoneFuture.thenAccept(v -> {
      log.info("GET all items: all futures completed");

      try {
        Map<String, JsonObject> foundMaterialTypes
          = allMaterialTypeFutures.stream()
          .map(CompletableFuture::join)
          .filter(response -> response.getStatusCode() == 200)
          .map(Response::getJson)
          .collect(Collectors.toMap(r -> r.getString("id"), r -> r));

        Map<String, JsonObject> foundLoanTypes
          = allLoanTypeFutures.stream()
          .map(CompletableFuture::join)
          .filter(response -> response.getStatusCode() == 200)
          .map(Response::getJson)
          .collect(Collectors.toMap(r -> r.getString("id"), r -> r));

        Map<String, JsonObject> foundLocations
          = allLocationsFutures.stream()
          .map(CompletableFuture::join)
          .filter(response -> response.getStatusCode() == 200)
          .map(Response::getJson)
          .collect(Collectors.toMap(r -> r.getString("id"), r -> r));

        wrappedItems.records.stream().forEach(item -> {
          JsonObject materialType = foundMaterialTypes.get(getMaterialTypeId(item));
          JsonObject permanentLoanType = foundLoanTypes.get(getPermanentLoanTypeId(item));
          JsonObject temporaryLoanType = foundLoanTypes.get(getTemporaryLoanTypeId(item));
          JsonObject permanentLocation = foundLocations.get(getPermanentLocationId(item));
          JsonObject temporaryLocation = foundLocations.get(getTemporaryLocationId(item));

          addReferenceInformationToItem(item, materialType, permanentLoanType,
            temporaryLoanType, permanentLocation, temporaryLocation);
        });

        Items items = new Items()
          .withItems(wrappedItems.records)
          .withTotalRecords(wrappedItems.totalRecords);

        respond(asyncResultHandler,
          GetInventoryItemsResponse.withJsonOK(items));
      }
      catch(Exception e) {
        respond(asyncResultHandler,
          GetInventoryItemsResponse.withPlainInternalServerError(e.toString()));
      }
    });
  }
}
