package org.folio.inventory.storage.memory;

import org.apache.commons.lang.StringUtils;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.ingest.IngestJobCollection;
import org.folio.rest.jaxrs.model.IngestStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InMemoryIngestJobCollection implements IngestJobCollection {
  private static final InMemoryIngestJobCollection instance = new InMemoryIngestJobCollection();

  private final List<IngestStatus> items = new ArrayList<>();

  public static InMemoryIngestJobCollection getInstance() {
    return instance;
  }

  @Override
  public void empty(
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    items.clear();
    completionCallback.accept(new Success<>(null));
  }

  @Override
  public void add(
    IngestStatus item,
    Consumer<Success<IngestStatus>> resultCallback,
    Consumer<Failure> failureCallback) {

    if(item.getId() == null) {
      item.setId(UUID.randomUUID().toString());
    }

    items.add(item);
    resultCallback.accept(new Success<>(item));
  }

  @Override
  public void findById(
    final String id,
    Consumer<Success<IngestStatus>> resultCallback,
    Consumer<Failure> failureCallback) {

    Optional<IngestStatus> foundJob = items.stream()
      .filter(sameId(id))
      .findFirst();

    if(foundJob.isPresent()) {
      resultCallback.accept(new Success<>(foundJob.get()));
    }
    else {
      resultCallback.accept(new Success<>(null));
    }
  }

  @Override
  public void findAll(
    PagingParameters pagingParameters,
    Consumer<Success<MultipleRecords<IngestStatus>>> resultCallback,
    Consumer<Failure> failureCallback) {

    int totalRecords = items.size();

    List<IngestStatus> paged = items.stream()
      .skip(pagingParameters.offset)
      .limit(pagingParameters.limit)
      .collect(Collectors.toList());

    resultCallback.accept(new Success<>(new MultipleRecords<>(paged, totalRecords)));
  }

  @Override
  public void update(
    final IngestStatus ingestJob,
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    items.removeIf(sameId(ingestJob.getId()));
    items.add(ingestJob);

    completionCallback.accept(new Success<>(null));
  }

  @Override
  public void delete(
    final String id,
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    items.removeIf(sameId(id));
    completionCallback.accept(new Success<>(null));
  }

  private Predicate<IngestStatus> sameId(String id) {
    return it -> StringUtils.equals(it.getId(), id);
  }
}
