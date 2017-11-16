package org.folio.inventory.domain;

import org.folio.rest.jaxrs.model.Instance;

public interface InstanceCollection
  extends AsynchronousCollection<Instance>, SearchableCollection<Instance> {
}
