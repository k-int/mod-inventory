package org.folio.inventory.storage.memory;

import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.rest.jaxrs.model.IngestStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.folio.inventory.common.FutureAssistance.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class InMemoryIngestJobCollectionExamples {

  private final InMemoryIngestJobCollection collection = new InMemoryIngestJobCollection();

  @Before
  public void before()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Success<Void>> emptied = new CompletableFuture<>();

    collection.empty(complete(emptied), fail(emptied));

    waitForCompletion(emptied);
  }

  @Test
  public void jobsCanBeAdded()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<IngestStatus> addFuture = new CompletableFuture<>();

    collection.add(new IngestStatus().withStatus(IngestStatus.Status.REQUESTED),
      succeed(addFuture), fail(addFuture));

    waitForCompletion(addFuture);

    CompletableFuture<MultipleRecords<IngestStatus>> findFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(), succeed(findFuture),
      fail(findFuture));

    MultipleRecords<IngestStatus> allJobsWrapped = getOnCompletion(findFuture);

    List<IngestStatus> allJobs = allJobsWrapped.records;

    assertThat(allJobs.size(), is(1));

    allJobs.stream().forEach(job -> assertThat(job.getId(), is(notNullValue())));
    allJobs.stream().forEach(job -> assertThat(job.getStatus(), is(IngestStatus.Status.REQUESTED)));
  }

  @Test
  public void jobsCanBeFoundById()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<IngestStatus> addFuture = new CompletableFuture<>();

    collection.add(new IngestStatus().withStatus(IngestStatus.Status.REQUESTED),
      succeed(addFuture), fail(addFuture));

    IngestStatus added = getOnCompletion(addFuture);

    CompletableFuture<IngestStatus> findFuture = new CompletableFuture<>();

    collection.findById(added.getId(), succeed(findFuture), fail(findFuture));

    IngestStatus found = getOnCompletion(findFuture);

    assertThat(found.getId(), is(added.getId()));
    assertThat(found.getStatus(), is(IngestStatus.Status.REQUESTED));
  }

  @Test
  public void jobStateCanBeUpdated()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<IngestStatus> addFuture = new CompletableFuture<>();

    collection.add(new IngestStatus().withStatus(IngestStatus.Status.REQUESTED),
      succeed(addFuture), fail(addFuture));

    IngestStatus added = getOnCompletion(addFuture);

    IngestStatus completed = new IngestStatus()
      .withId(added.getId())
      .withStatus(IngestStatus.Status.COMPLETED);

    CompletableFuture<Void> updateFuture = new CompletableFuture<>();

    collection.update(completed, succeed(updateFuture),
      fail(updateFuture));

    waitForCompletion(updateFuture);

    CompletableFuture<IngestStatus> findFuture = new CompletableFuture<>();

    collection.findById(added.getId(), succeed(findFuture), fail(findFuture));

    IngestStatus found = getOnCompletion(findFuture);

    assertThat(found.getStatus(), is(IngestStatus.Status.COMPLETED));
  }

  @Test
  public void singleJobWithSameIdFollowingUpdate()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<IngestStatus> addFuture = new CompletableFuture<>();

    collection.add(new IngestStatus().withStatus(IngestStatus.Status.REQUESTED),
      succeed(addFuture), fail(addFuture));

    IngestStatus added = getOnCompletion(addFuture);

    IngestStatus completed = new IngestStatus()
      .withId(added.getId())
      .withStatus(IngestStatus.Status.COMPLETED);

    CompletableFuture<Void> updateFuture = new CompletableFuture<>();

    collection.update(completed, succeed(updateFuture), fail(updateFuture));

    waitForCompletion(updateFuture);

    CompletableFuture<MultipleRecords<IngestStatus>> findAllFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(), succeed(findAllFuture),
      fail(findAllFuture));

    MultipleRecords<IngestStatus> allJobsWrapped = getOnCompletion(findAllFuture);

    List<IngestStatus> allJobs = allJobsWrapped.records;

    assertThat(allJobs.size(), is(1));
    assertThat(allJobs.stream().findFirst().get().getId(), is(added.getId()));
  }
}
