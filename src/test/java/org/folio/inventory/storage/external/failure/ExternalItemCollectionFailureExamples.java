package org.folio.inventory.storage.external.failure;

import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.domain.CollectionProvider;
import org.folio.inventory.domain.ItemCollection;
import org.folio.rest.jaxrs.model.Item;
import org.folio.rest.jaxrs.model.MaterialType;
import org.folio.rest.jaxrs.model.PermanentLoanType;
import org.folio.rest.jaxrs.model.PermanentLocation;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

public abstract class ExternalItemCollectionFailureExamples {

  private final CollectionProvider collectionProvider;

  ExternalItemCollectionFailureExamples(CollectionProvider collectionProvider) {
    this.collectionProvider = collectionProvider;
  }

  @Test
  public void serverErrorWhenCreatingAnItemTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.add(createItem(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenUpdatingAnItemTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.update(createItem(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenGettingAllItemsTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenGettingAnItemByIdTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findById(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenDeletingAnItemByIdTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.delete(UUID.randomUUID().toString(),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenDeletingAllItemsTriggersFailureCallback()
    throws InterruptedException, ExecutionException, TimeoutException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.empty(
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  @Test
  public void serverErrorWhenFindingItemsTriggersFailureCallback()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    UnsupportedEncodingException {

    ItemCollection collection = createCollection();

    CompletableFuture<Failure> failureCalled = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"",
      new PagingParameters(10, 0),
      success -> fail("Completion callback should not be called"),
      failureCalled::complete);

    Failure failure = failureCalled.get(1000, TimeUnit.MILLISECONDS);

    check(failure);
  }

  protected abstract void check(Failure failure);

  private static Item createItem() {
    return new Item()
      .withId(UUID.randomUUID().toString())
      .withTitle("Nod")
      .withBarcode("6575467847")
      .withMaterialType(new MaterialType().withId(UUID.randomUUID().toString()))
      .withPermanentLoanType(new PermanentLoanType().withId(UUID.randomUUID().toString()))
      .withPermanentLocation(new PermanentLocation().withId(UUID.randomUUID().toString()));
  }

  private ItemCollection createCollection() {
    return collectionProvider.getItemCollection("test_tenant", "");
  }
}
