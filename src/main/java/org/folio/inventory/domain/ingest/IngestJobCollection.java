package org.folio.inventory.domain.ingest;

import org.folio.inventory.domain.AsynchronousCollection;
import org.folio.rest.jaxrs.model.IngestStatus;

public interface IngestJobCollection
  extends AsynchronousCollection<IngestStatus> {

}
