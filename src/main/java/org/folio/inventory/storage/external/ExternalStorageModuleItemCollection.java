package org.folio.inventory.storage.external;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.ItemCollection;
import org.folio.rest.jaxrs.model.*;

import java.util.UUID;

public class ExternalStorageModuleItemCollection
  extends ExternalStorageModuleCollection<Item>
  implements ItemCollection {

  public ExternalStorageModuleItemCollection(Vertx vertx,
                                      String baseAddress,
                                      String tenant,
                                      String token) {

    super(vertx, String.format("%s/%s", baseAddress, "item-storage/items"),
      tenant, token, "items");
  }

  @Override
  protected Item mapFromJson(JsonObject itemFromServer) {
    Item item = new Item();

    item
      .withId(itemFromServer.getString("id"))
      .withTitle(itemFromServer.getString("title"))
      .withBarcode(itemFromServer.getString("barcode"))
      .withInstanceId(itemFromServer.getString("instanceId"));

    if(itemFromServer.containsKey("status")) {
      item.withStatus(new Status().withName(
        itemFromServer.getJsonObject("status").getString("name")));
    }

    if(itemFromServer.containsKey("materialTypeId")) {
      item.withMaterialType(new MaterialType()
        .withId(itemFromServer.getString("materialTypeId")));
    }

    if(itemFromServer.containsKey("permanentLoanTypeId")) {
      item.withPermanentLoanType(new PermanentLoanType()
        .withId(itemFromServer.getString("permanentLoanTypeId")));
    }

    if(itemFromServer.containsKey("temporaryLoanTypeId")) {
      item.withTemporaryLoanType(new TemporaryLoanType()
        .withId(itemFromServer.getString("temporaryLoanTypeId")));
    }

    if(itemFromServer.containsKey("permanentLocationId")) {
      item.withPermanentLocation(new PermanentLocation()
        .withId(itemFromServer.getString("permanentLocationId")));
    }

    if(itemFromServer.containsKey("temporaryLocationId")) {
      item.withTemporaryLocation(new TemporaryLocation()
        .withId(itemFromServer.getString("temporaryLocationId")));
    }

    return item;
  }

  @Override
  protected String getId(Item record) {
    return record.getId();
  }

  @Override
  protected JsonObject mapToRequest(Item item) {
    JsonObject itemToSend = new JsonObject();

    //TODO: Review if this shouldn't be defaulting here
    itemToSend.put("id", item.getId() != null
      ? item.getId()
      : UUID.randomUUID().toString());

    itemToSend.put("title", item.getTitle());

    if(getStatusName(item) != null) {
      itemToSend.put("status", new JsonObject()
        .put("name", getStatusName(item)));
    }

    includeIfPresent(itemToSend, "barcode", item.getBarcode());
    includeIfPresent(itemToSend, "instanceId", item.getInstanceId());
    includeIfPresent(itemToSend, "materialTypeId", getMaterialTypeId(item));
    includeIfPresent(itemToSend, "permanentLoanTypeId", getPermanentLoanTypeId(item));
    includeIfPresent(itemToSend, "temporaryLoanTypeId", getTemporaryLoanTypeId(item));
		includeIfPresent(itemToSend, "permanentLocationId", getPermanentLocationId(item));
		includeIfPresent(itemToSend, "temporaryLocationId", getTemporaryLocationId(item));

    return itemToSend;
  }

  private String getStatusName(Item item) {
    Status status = item.getStatus();

    return status == null
      ? null
      : status.getName();
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
}
