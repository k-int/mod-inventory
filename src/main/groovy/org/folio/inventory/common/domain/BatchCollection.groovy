package org.folio.inventory.common.domain

interface BatchCollection<T> {
  List<T> add(List<T> items)
//    void add(List<T> collection, Closure resultCallback)
}
