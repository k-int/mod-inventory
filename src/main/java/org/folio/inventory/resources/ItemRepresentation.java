package org.folio.inventory.resources;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.rest.jaxrs.model.*;

import java.util.List;
import java.util.Map;

public class ItemRepresentation {
  public JsonObject toJson(
    Item item,
    JsonObject materialType,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType,
    JsonObject permanentLocation,
    JsonObject temporaryLocation) {

    JsonObject representation = toJson(item);

    if(materialType != null) {
      representation.getJsonObject("materialType")
        .put("name", materialType.getString("name"));
    }

    if(permanentLoanType != null) {
      representation.getJsonObject("permanentLoanType")
        .put("name", permanentLoanType.getString("name"));
    }

    if(temporaryLoanType != null) {
      representation.getJsonObject("temporaryLoanType")
        .put("name", temporaryLoanType.getString("name"));
    }

    if(permanentLocation != null) {
      representation.getJsonObject("permanentLocation")
        .put("name", permanentLocation.getString("name"));
    }

    if(temporaryLocation != null) {
      representation.getJsonObject("temporaryLocation")
        .put("name", temporaryLocation.getString("name"));
    }

    return representation;
  }

  JsonObject toJson(
    MultipleRecords<Item> wrappedItems,
    Map<String, JsonObject> materialTypes,
    Map<String, JsonObject> loanTypes,
    Map<String, JsonObject> locations) {

    JsonObject representation = new JsonObject();

    JsonArray results = new JsonArray();

    List<Item> items = wrappedItems.records;

    items.stream().forEach(item -> {
      JsonObject materialType = materialTypes.get(getMaterialTypeId(item));
      JsonObject permanentLoanType = loanTypes.get(getPermanentLoanTypeId(item));
      JsonObject temporaryLoanType = loanTypes.get(getTemporaryLoanTypeId(item));
      JsonObject permanentLocation = locations.get(getPermanentLocationId(item));
      JsonObject temporaryLocation = locations.get(getTemporaryLocationId(item));

      results.add(toJson(item, materialType, permanentLoanType, temporaryLoanType,
        permanentLocation, temporaryLocation));
    });

    representation
      .put("items", results)
      .put("totalRecords", wrappedItems.totalRecords);

    return representation;
  }

  private JsonObject toJson(Item item) {

    JsonObject representation = new JsonObject();
    representation.put("id", item.getId());
    representation.put("title", item.getTitle());

    if(item.getStatus() != null) {
      representation.put("status", new JsonObject()
        .put("name", item.getStatus().getName()));
    }

    includeIfPresent(representation, "instanceId", item.getInstanceId());
    includeIfPresent(representation, "barcode", item.getBarcode());

    includeReferenceIfPresent(representation, "materialType",
      getMaterialTypeId(item));

    includeReferenceIfPresent(representation, "permanentLoanType",
      getPermanentLoanTypeId(item));

    includeReferenceIfPresent(representation, "temporaryLoanType",
      getTemporaryLoanTypeId(item));

    includeReferenceIfPresent(representation, "permanentLocation",
      getPermanentLocationId(item));

    includeReferenceIfPresent(representation, "temporaryLocation",
      getTemporaryLocationId(item));

    return representation;
  }

  private void includeReferenceIfPresent(
    JsonObject representation,
    String referencePropertyName,
    String id) {

    if (id != null) {
      representation.put(referencePropertyName,
        new JsonObject().put("id", id));
    }
  }

  private void includeIfPresent(
    JsonObject representation,
    String propertyName,
    String propertyValue) {

    if (propertyValue != null) {
      representation.put(propertyName, propertyValue);
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
}
