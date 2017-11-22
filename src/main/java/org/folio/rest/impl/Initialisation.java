package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.folio.inventory.domain.ingest.IngestMessageProcessor;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.ExternalStorageCollections;
import org.folio.rest.resource.interfaces.InitAPI;

public class Initialisation implements InitAPI {
  @Override
  public void init(
    Vertx vertx,
    Context context,
    Handler<AsyncResult<Boolean>> handler) {

    try {
      Storage storage = new Storage(
        c -> new ExternalStorageCollections(vertx, c.getOkapiLocation()));

      new IngestMessageProcessor(storage)
        .register(vertx.eventBus());

      handler.handle(io.vertx.core.Future.succeededFuture(true));
    }
    catch (Exception e) {
      handler.handle(io.vertx.core.Future.failedFuture(e));
    }
  }
}
