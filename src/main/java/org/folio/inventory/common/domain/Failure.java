package org.folio.inventory.common.domain;

public class Failure {
  private final String reason;
  private final Integer statusCode;
  private final String contentType;

  public Failure(
    String reason,
    Integer statusCode,
    String contentType) {

    this.reason = reason;
    this.statusCode = statusCode;
    this.contentType = contentType;
  }

  public final String getReason() {
    return reason;
  }

  public final Integer getStatusCode() {
    return statusCode;
  }

  public String getContentType() {
    return contentType;
  }
}
