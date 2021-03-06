package org.folio.inventory.common.storage.memory

import org.folio.inventory.common.api.request.PagingParameters
import org.folio.inventory.common.domain.Success
import org.folio.inventory.common.cql.CqlFilter
import org.folio.inventory.common.cql.CqlParser

import java.util.function.Consumer

//TODO: truly asynchronous implementation
class InMemoryCollection<T> {

  public final List<T> items = new ArrayList<T>()

  List<T> find(Closure matcher) {
    items.findAll(matcher)
  }

  List<T> all() {
    items.collect()
  }

  void some(PagingParameters pagingParameters,
            String collectionName,
            Consumer<Success<Map>> resultCallback) {

    def totalRecords = all().size()

    def paged = all().stream()
      .skip(pagingParameters.offset)
      .limit(pagingParameters.limit)
      .collect()

    resultCallback.accept(new Success(
      wrapFindResult(collectionName, paged, totalRecords)))
  }

  void findOne(Closure matcher, Consumer<Success<T>> successCallback) {
    successCallback.accept(new Success(items.find(matcher)))
  }

  void find(String cqlQuery,
            PagingParameters pagingParameters,
            String collectionName,
            Consumer<Success<Map>> resultCallback) {

    def (field, searchTerm) = new CqlParser().parseCql(cqlQuery)

    def filtered = all().stream()
      .filter(new CqlFilter().filterBy(field, searchTerm))
      .collect()

    def paged = filtered.stream()
      .skip(pagingParameters.offset)
      .limit(pagingParameters.limit)
      .collect()

    resultCallback.accept(new Success(
      wrapFindResult(collectionName, paged, filtered.size())))
  }

  void add(T item, Consumer<Success<T>> resultCallback) {
    items.add(item)
    resultCallback.accept(new Success<T>(item))
  }

  void replace(T item, Consumer<Success> completionCallback) {
    items.removeIf({ it.id == item.id })
    items.add(item)
    completionCallback.accept(new Success(null))
  }

  void empty(Consumer<Success> completionCallback) {
    items.clear()
    completionCallback.accept(new Success())
  }

  void remove(String id, Consumer<Success> completionCallback) {
    items.removeIf({ it.id == id })
    completionCallback.accept(new Success())
  }

  private Map wrapFindResult(
    String collectionName,
    Collection pagedRecords,
    int totalRecords) {

    [
      (collectionName): pagedRecords,
      "totalRecords"  : totalRecords
    ]
  }
}
