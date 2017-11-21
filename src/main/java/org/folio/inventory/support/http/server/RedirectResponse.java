package org.folio.inventory.support.http.server;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;

public class RedirectResponse {
  private RedirectResponse() { }

  public static void accepted(HttpServerResponse response, String url) {
    response.headers().add(HttpHeaders.LOCATION, url);
    response.setStatusCode(202);
    response.end();
  }
}
