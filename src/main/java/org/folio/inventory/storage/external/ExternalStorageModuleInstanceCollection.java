package org.folio.inventory.storage.external;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.InstanceCollection;
import org.folio.rest.jaxrs.model.Creator;
import org.folio.rest.jaxrs.model.Identifier;
import org.folio.rest.jaxrs.model.Instance;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.folio.inventory.support.JsonArrayHelper.toList;

public class ExternalStorageModuleInstanceCollection
  extends ExternalStorageModuleCollection<Instance>
  implements InstanceCollection {

  public ExternalStorageModuleInstanceCollection(
    Vertx vertx,
    String baseAddress,
    String tenant,
    String token) {

    super(vertx, String.format("%s/%s", baseAddress, "instance-storage/instances"),
      tenant, token, "instances");
  }

  @Override
  protected JsonObject mapToRequest(Instance instance) {
    JsonObject instanceToSend = new JsonObject();

    //TODO: Review if this shouldn't be defaulting here
    instanceToSend.put("id", instance.getId() != null
      ? instance.getId()
      : UUID.randomUUID().toString());

    instanceToSend.put("title", instance.getTitle());
    includeIfPresent(instanceToSend, "instanceTypeId", instance.getInstanceTypeId());
    includeIfPresent(instanceToSend, "source", instance.getSource());
    instanceToSend.put("identifiers", instance.getIdentifiers());
    instanceToSend.put("creators", instance.getCreators());

    return instanceToSend;
  }

  @Override
  protected Instance mapFromJson(JsonObject instanceFromServer) {
    List<JsonObject> identifiers = toList(
      instanceFromServer.getJsonArray("identifiers", new JsonArray()));

    List<Identifier> mappedIdentifiers = identifiers.stream()
      .map(it -> new Identifier()
        .withIdentifierTypeId(it.getString("identifierTypeId"))
        .withValue(it.getString("value")))
      .collect(Collectors.toList());

    List<JsonObject> creators = toList(
      instanceFromServer.getJsonArray("creators", new JsonArray()));

    List<Creator> mappedCreators = creators.stream()
      .map(it -> new Creator()
        .withCreatorTypeId(it.getString("creatorTypeId"))
        .withName(it.getString("name")))
      .collect(Collectors.toList());

    return new Instance()
      .withId(instanceFromServer.getString("id"))
      .withTitle(instanceFromServer.getString("title"))
      .withIdentifiers(mappedIdentifiers)
      .withSource(instanceFromServer.getString("source"))
      .withInstanceTypeId(instanceFromServer.getString("instanceTypeId"))
      .withCreators(mappedCreators);
  }

  @Override
  protected String getId(Instance record) {
    return record.getId();
  }
}
