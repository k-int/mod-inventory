package org.folio.inventory.domain;

import org.folio.rest.jaxrs.model.Item;

public interface ItemCollection extends AsynchronousCollection<Item>, SearchableCollection<Item> {
}
