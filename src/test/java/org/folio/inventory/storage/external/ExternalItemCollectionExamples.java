package org.folio.inventory.storage.external;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WaitForAllFutures;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.ItemCollection;
import org.folio.rest.jaxrs.model.*;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.folio.inventory.common.FutureAssistance.*;
import static org.folio.inventory.storage.external.ExternalStorageSuite.getStorageAddress;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class ExternalItemCollectionExamples {
  private final String bookMaterialTypeId = UUID.randomUUID().toString();
  private final String canCirculateLoanTypeId = UUID.randomUUID().toString();

  private final String mainLibraryLocationId = UUID.randomUUID().toString();
  private final String annexLibraryLocationId = UUID.randomUUID().toString();

  private final ItemCollection collection =
    ExternalStorageSuite.useVertx(
      it -> new ExternalStorageModuleItemCollection(it, getStorageAddress(),
        ExternalStorageSuite.TENANT_ID, ExternalStorageSuite.TENANT_TOKEN));

  private final Item smallAngryPlanet = smallAngryPlanet();
  private final Item nod = nod();
  private final Item uprooted = uprooted();
  private final Item temeraire = temeraire();
  private final Item interestingTimes = interestingTimes();

  @Before
  public void before()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Void> emptied = new CompletableFuture<>();

    collection.empty(succeed(emptied), fail(emptied));

    waitForCompletion(emptied);
  }

  @Test
  public void canBeEmptied()
    throws InterruptedException, ExecutionException, TimeoutException {

    addSomeExamples(collection);

    CompletableFuture<Void> emptied = new CompletableFuture<>();

    collection.empty(succeed(emptied), fail(emptied));

    waitForCompletion(emptied);

    CompletableFuture<MultipleRecords<Item>> findFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(),
      succeed(findFuture), fail(findFuture));

    MultipleRecords<Item> wrappedItems = getOnCompletion(findFuture);

    List<Item> allItems = wrappedItems.records;

    assertThat(allItems.size(), is(0));
    assertThat(wrappedItems.totalRecords, is(0));
  }

  @Test
  public void anItemCanBeAdded()
    throws InterruptedException, ExecutionException, TimeoutException {

    addSomeExamples(collection);

    CompletableFuture<MultipleRecords<Item>> findFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(), succeed(findFuture),
      fail(findFuture));

    MultipleRecords<Item> wrappedItems = getOnCompletion(findFuture);

    List<Item> allItems = wrappedItems.records;

    assertThat(allItems.size(), is(3));
    assertThat(wrappedItems.totalRecords, is(3));

    Item smallAngry = getItem(allItems, "Long Way to a Small Angry Planet");

    assertThat(smallAngry, notNullValue());
    assertThat(smallAngry.getBarcode(), is("036000291452"));
    assertThat(smallAngry.getStatus().getName(), is("Available"));
    assertThat(smallAngry.getInstanceId(), is(notNullValue()));
    assertThat(smallAngry.getMaterialType().getId(), is(bookMaterialTypeId));
    assertThat(smallAngry.getPermanentLoanType().getId(), is(canCirculateLoanTypeId));
    assertThat(smallAngry.getTemporaryLoanType(), is(nullValue()));
    assertThat(smallAngry.getPermanentLocation().getId(), is(mainLibraryLocationId));
    assertThat(smallAngry.getTemporaryLocation().getId(), is(annexLibraryLocationId));

    Item nod = getItem(allItems, "Nod");

    assertThat(nod, notNullValue());
    assertThat(nod.getBarcode(), is("565578437802"));
    assertThat(nod.getStatus().getName(), is("Available"));
    assertThat(nod.getInstanceId(), is(notNullValue()));
    assertThat(nod.getMaterialType().getId(), is(bookMaterialTypeId));
    assertThat(nod.getPermanentLoanType().getId(), is(canCirculateLoanTypeId));
    assertThat(smallAngry.getTemporaryLoanType(), is(nullValue()));
    assertThat(nod.getPermanentLocation().getId(), is(mainLibraryLocationId));
    assertThat(nod.getTemporaryLocation().getId(), is(annexLibraryLocationId));

    Item uprooted = getItem(allItems, "Uprooted");

    assertThat(uprooted, notNullValue());
    assertThat(uprooted.getBarcode(), is("657670342075"));
    assertThat(uprooted.getStatus().getName(), is("Available"));
    assertThat(uprooted.getInstanceId(), is(notNullValue()));
    assertThat(uprooted.getMaterialType().getId(), is(bookMaterialTypeId));
    assertThat(uprooted.getPermanentLoanType().getId(), is(canCirculateLoanTypeId));
    assertThat(smallAngry.getTemporaryLoanType(), is(nullValue()));
    assertThat(uprooted.getPermanentLocation().getId(), is(mainLibraryLocationId));
    assertThat(uprooted.getTemporaryLocation().getId(), is(annexLibraryLocationId));
  }

  @Test
  public void anItemCanBeAddedWithAnId()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Item> addFinished = new CompletableFuture<>();

    String itemId = UUID.randomUUID().toString();

    //TODO: Should copy rather than set
    Item itemWithId = smallAngryPlanet.withId(itemId);

    collection.add(itemWithId, succeed(addFinished), fail(addFinished));

    Item added = getOnCompletion(addFinished);

    assertThat(added.getId(), is(itemId));
  }

  @Test
  public void anItemCanBeUpdated()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Item> addFinished = new CompletableFuture<>();

    collection.add(smallAngryPlanet, succeed(addFinished), fail(addFinished));

    Item added = getOnCompletion(addFinished);

    CompletableFuture<Void> updateFinished = new CompletableFuture<>();

    //TODO: Should copy rather than set
    Item changed = added.withStatus(new Status().withName("Checked Out"));

    collection.update(changed, succeed(updateFinished),
      fail(updateFinished));

    waitForCompletion(updateFinished);

    CompletableFuture<Item> gotUpdated = new CompletableFuture<>();

    collection.findById(added.getId(), succeed(gotUpdated),
      fail(gotUpdated));

    Item updated = getOnCompletion(gotUpdated);

    assertThat(updated.getId(), is(added.getId()));
    assertThat(updated.getTitle(), is(added.getTitle()));
    assertThat(updated.getBarcode(), is(added.getBarcode()));
    assertThat(updated.getPermanentLocation().getId(), is(added.getPermanentLocation().getId()));
    assertThat(updated.getTemporaryLocation().getId(), is(added.getTemporaryLocation().getId()));
    assertThat(updated.getMaterialType().getId(), is(added.getMaterialType().getId()));
    assertThat(updated.getPermanentLoanType().getId(), is(added.getPermanentLoanType().getId()));
    assertThat(updated.getStatus().getName(), is("Checked Out"));
  }

  @Test
  public void anItemCanBeDeleted()
    throws InterruptedException, ExecutionException, TimeoutException {

    addSomeExamples(collection);

    CompletableFuture<Item> itemToBeDeletedFuture = new CompletableFuture<>();

    collection.add(temeraire(), succeed(itemToBeDeletedFuture),
      fail(itemToBeDeletedFuture));

    Item itemToBeDeleted = itemToBeDeletedFuture.get();

    CompletableFuture<Void> deleted = new CompletableFuture<>();

    collection.delete(itemToBeDeleted.getId(),
      succeed(deleted), fail(deleted));

    waitForCompletion(deleted);

    CompletableFuture<Item> findFuture = new CompletableFuture<>();

    collection.findById(itemToBeDeleted.getId(), succeed(findFuture),
      fail(findFuture));

    assertThat(findFuture.get(), is(nullValue()));

    CompletableFuture<MultipleRecords<Item>> findAllFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(), succeed(findAllFuture),
      fail(findAllFuture));

    MultipleRecords<Item> wrappedItems = getOnCompletion(findAllFuture);

    List<Item> allItems = wrappedItems.records;

    assertThat(allItems.size(), is(3));
    assertThat(wrappedItems.totalRecords, is(3));
  }

  @Test
  public void allItemsCanBePaged()
    throws InterruptedException, ExecutionException, TimeoutException {

    WaitForAllFutures<Item> allAdded = new WaitForAllFutures<>();

    collection.add(smallAngryPlanet, allAdded.notifySuccess(), v -> {});
    collection.add(nod, allAdded.notifySuccess(), v -> {});
    collection.add(uprooted, allAdded.notifySuccess(), v -> {});
    collection.add(temeraire, allAdded.notifySuccess(), v -> {});
    collection.add(interestingTimes, allAdded.notifySuccess(), v -> {});

    allAdded.waitForCompletion();

    CompletableFuture<MultipleRecords<Item>> firstPageFuture = new CompletableFuture<>();
    CompletableFuture<MultipleRecords<Item>> secondPageFuture = new CompletableFuture<>();

    collection.findAll(new PagingParameters(3, 0), succeed(firstPageFuture),
      fail(secondPageFuture));

    collection.findAll(new PagingParameters(3, 3), succeed(secondPageFuture),
      fail(secondPageFuture));

    MultipleRecords<Item> firstPage = getOnCompletion(firstPageFuture);
    MultipleRecords<Item> secondPage = getOnCompletion(secondPageFuture);

    assertThat(firstPage.records.size(), is(3));
    assertThat(secondPage.records.size(), is(2));

    assertThat(firstPage.totalRecords, is(5));
    assertThat(secondPage.totalRecords, is(5));
  }

  @Test
  public void itemsCanBeFoundByByPartialName()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    UnsupportedEncodingException {

    CompletableFuture<Item> firstAddFuture = new CompletableFuture<>();
    CompletableFuture<Item> secondAddFuture = new CompletableFuture<>();
    CompletableFuture<Item> thirdAddFuture = new CompletableFuture<>();

    collection.add(smallAngryPlanet, succeed(firstAddFuture),
      fail(firstAddFuture));
    collection.add(nod, succeed(secondAddFuture),
      fail(secondAddFuture));
    collection.add(uprooted, succeed(thirdAddFuture),
      fail(thirdAddFuture));

    CompletableFuture<Void> allAddsFuture = CompletableFuture.allOf(firstAddFuture,
      secondAddFuture, thirdAddFuture);

    getOnCompletion(allAddsFuture);

    Item addedSmallAngryPlanet = getOnCompletion(firstAddFuture);

    CompletableFuture<MultipleRecords<Item>> findFuture = new CompletableFuture<>();

    collection.findByCql("title=\"*Small Angry*\"", new PagingParameters(10, 0),
      succeed(findFuture), fail(findFuture));

    MultipleRecords<Item> wrappedItems = getOnCompletion(findFuture);

    assertThat(wrappedItems.records.size(), is(1));
    assertThat(wrappedItems.totalRecords, is(1));

    assertThat(wrappedItems.records.stream().findFirst().get().getId(),
      is(addedSmallAngryPlanet.getId()));
  }

  @Test
  public void itemsCanBeFoundByBarcode()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    UnsupportedEncodingException {

    CompletableFuture<Item> firstAddFuture = new CompletableFuture<>();
    CompletableFuture<Item> secondAddFuture = new CompletableFuture<>();
    CompletableFuture<Item> thirdAddFuture = new CompletableFuture<>();

    collection.add(smallAngryPlanet, succeed(firstAddFuture),
      fail(firstAddFuture));
    collection.add(nod, succeed(secondAddFuture),
      fail(secondAddFuture));
    collection.add(uprooted, succeed(thirdAddFuture),
      fail(thirdAddFuture));

    CompletableFuture<Void> allAddsFuture = CompletableFuture.allOf(
      firstAddFuture, secondAddFuture, thirdAddFuture);

    getOnCompletion(allAddsFuture);

    Item addedSmallAngryPlanet = getOnCompletion(firstAddFuture);

    CompletableFuture<MultipleRecords<Item>> findFuture = new CompletableFuture<>();

    collection.findByCql("barcode=036000291452", new PagingParameters(10, 0),
      succeed(findFuture), fail(findFuture));

    MultipleRecords<Item> wrappedItems = getOnCompletion(findFuture);

    assertThat(wrappedItems.records.size(), is(1));
    assertThat(wrappedItems.totalRecords, is(1));

    assertThat(wrappedItems.records.stream().findFirst().get().getId(),
      is(addedSmallAngryPlanet.getId()));
  }

  @Test
  public void anItemCanBeFoundById()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Item> firstAddFuture = new CompletableFuture<>();
    CompletableFuture<Item> secondAddFuture = new CompletableFuture<>();

    collection.add(smallAngryPlanet, succeed(firstAddFuture),
      fail(firstAddFuture));

    collection.add(nod, succeed(secondAddFuture),
      fail(secondAddFuture));

    Item addedItem = getOnCompletion(firstAddFuture);
    Item otherAddedItem = getOnCompletion(secondAddFuture);

    CompletableFuture<Item> findFuture = new CompletableFuture<>();
    CompletableFuture<Item> otherFindFuture = new CompletableFuture<>();

    collection.findById(addedItem.getId(), succeed(findFuture),
      fail(findFuture));

    collection.findById(otherAddedItem.getId(), succeed(otherFindFuture),
      fail(otherFindFuture));

    Item foundItem = getOnCompletion(findFuture);
    Item otherFoundItem = getOnCompletion(otherFindFuture);

    assertThat(foundItem, notNullValue());
    assertThat(foundItem.getTitle(), is("Long Way to a Small Angry Planet"));
    assertThat(foundItem.getInstanceId(), is(smallAngryPlanet.getInstanceId()));
    assertThat(foundItem.getBarcode(), is("036000291452"));
    assertThat(foundItem.getStatus().getName(), is("Available"));
    assertThat(foundItem.getMaterialType().getId(), is(bookMaterialTypeId));
    assertThat(foundItem.getPermanentLoanType().getId(), is(canCirculateLoanTypeId));
    assertThat(foundItem.getPermanentLocation().getId(), is(mainLibraryLocationId));
    assertThat(foundItem.getTemporaryLocation().getId(), is(annexLibraryLocationId));

    assertThat(otherFoundItem, notNullValue());
    assertThat(otherFoundItem.getTitle(), is("Nod"));
    assertThat(otherFoundItem.getInstanceId(), is(nod.getInstanceId()));
    assertThat(otherFoundItem.getBarcode(), is("565578437802"));
    assertThat(otherFoundItem.getStatus().getName(), is("Available"));
    assertThat(otherFoundItem.getMaterialType().getId(), is(bookMaterialTypeId));
    assertThat(otherFoundItem.getPermanentLoanType().getId(), is(canCirculateLoanTypeId));
    assertThat(otherFoundItem.getPermanentLocation().getId(), is(mainLibraryLocationId));
    assertThat(otherFoundItem.getTemporaryLocation().getId(), is(annexLibraryLocationId));
  }

  private void addSomeExamples(ItemCollection itemCollection)
    throws InterruptedException, ExecutionException, TimeoutException {

    WaitForAllFutures<Item> allAdded = new WaitForAllFutures<>();

    itemCollection.add(smallAngryPlanet, allAdded.notifySuccess(), v -> { });
    itemCollection.add(nod, allAdded.notifySuccess(), v -> { });
    itemCollection.add(uprooted, allAdded.notifySuccess(), v -> { });

    allAdded.waitForCompletion();
  }

  private Item smallAngryPlanet() {
    return createItem(
      "Long Way to a Small Angry Planet",
      "036000291452",
      UUID.randomUUID(),
      "Available",
      bookMaterialTypeId,
      mainLibraryLocationId,
      annexLibraryLocationId,
      canCirculateLoanTypeId,
      null);
  }

  private Item nod() {
    return createItem(
      "Nod",
      "565578437802",
      UUID.randomUUID(),
      "Available",
      bookMaterialTypeId,
      mainLibraryLocationId,
      annexLibraryLocationId,
      canCirculateLoanTypeId,
      null);
  }

  private Item uprooted() {
    return createItem(
      "Uprooted",
      "657670342075",
      UUID.randomUUID(),
      "Available",
      bookMaterialTypeId,
      mainLibraryLocationId,
      annexLibraryLocationId,
      canCirculateLoanTypeId,
      null);
  }

  private Item temeraire() {
    return createItem(
      "Temeraire",
      "232142443432",
      UUID.randomUUID(),
      "Available",
      bookMaterialTypeId,
      mainLibraryLocationId,
      annexLibraryLocationId,
      canCirculateLoanTypeId,
      null);
  }

  private Item interestingTimes() {
    return createItem(
      "Interesting Times",
      "56454543534",
      UUID.randomUUID(),
      "Available",
      bookMaterialTypeId,
      mainLibraryLocationId,
      annexLibraryLocationId,
      canCirculateLoanTypeId,
      null);
  }

  private Item createItem(
    String title,
    String barcode,
    UUID instanceId,
    String status,
    String materialTypeId,
    String permanentLocationId,
    String temporaryLocationId,
    String permanentLoanTypeId,
    String temporaryLoanTypeId) {

    Item item = new Item();

    item.withId(UUID.randomUUID().toString())
      .withTitle(title)
      .withBarcode(barcode)
      .withInstanceId(instanceId.toString())
      .withStatus(new Status().withName(status));

    if (materialTypeId != null) {
      item.withMaterialType(new MaterialType().withId(materialTypeId));
    }

    if (permanentLocationId != null) {
      item.withPermanentLocation(new PermanentLocation().withId(permanentLocationId));
    }

    if (temporaryLocationId != null) {
      item.withTemporaryLocation(new TemporaryLocation().withId(temporaryLocationId));
    }

    if (permanentLoanTypeId != null) {
      item.withPermanentLoanType(new PermanentLoanType().withId(permanentLoanTypeId));
    }

    if (temporaryLoanTypeId != null) {
      item.withTemporaryLoanType(new TemporaryLoanType().withId(temporaryLoanTypeId));
    }

    return item;
  }

  private Item getItem(List<Item> allItems, String title) {
    return allItems.stream()
      .filter(it -> StringUtils.equals(it.getTitle(), title))
      .findFirst().orElse(null);
  }
}
