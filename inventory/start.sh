#!/usr/bin/env bash

tenant_id=${1:-demo_tenant}
storage_type=${2:-internal}
storage_location=${3:-}

gradle fatJar

rm output.log

java -Dorg.folio.metadata.inventory.storage.type="${storage_type}" \
  -Dorg.folio.metadata.inventory.storage.location="${storage_location}" \
 -jar build/libs/inventory.jar 1>output.log 2>output.log &

printf 'Waiting for inventory module to start\n'

until $(curl --output /dev/null --silent --get --fail -H "X-Okapi-Tenant: ${tenant_id}" http://localhost:9403/inventory/items); do
    printf '.'
    sleep 1
done

printf '\n'

#tail -F output.log


