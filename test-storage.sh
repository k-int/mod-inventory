#!/usr/bin/env bash

tenant_id="test_tenant"
okapi_proxy_address="http://localhost:9130"
#Needs to be the specific version of Inventory Storage you want to use for testing
inventory_storage_module_id="mod-inventory-storage-5.1.0-SNAPSHOT"

echo "Check if Okapi is contactable"
curl -w '\n' -X GET -D -   \
     "${okapi_proxy_address}/_/env" || exit 1

echo "Create ${tenant_id} tenant"
./create-tenant.sh ${tenant_id}

echo "Activate inventory storage for ${tenant_id}"
activate_json=$(cat ./registration/activate.json)
activate_json="${activate_json/moduleidhere/$inventory_storage_module_id}"

curl -w '\n' -X POST -D - \
     -H "Content-type: application/json" \
     -d "${activate_json}"  \
     "${okapi_proxy_address}/_/proxy/tenants/${tenant_id}/modules"

echo "Run API tests"

gradle -Dokapi.address="${okapi_address}" clean cleanTest testStorageViaOkapi

test_results=$?

echo "Deactivate inventory storage for ${tenant_id}"
curl -X DELETE -D - -w '\n' "${okapi_proxy_address}/_/proxy/tenants/${tenant_id}/modules/${inventory_storage_module_id}"

echo "Deleting ${tenant_id}"
./delete-tenant.sh ${tenant_id}

echo "Need to manually remove test_tenant storage as Tenant API no longer invoked on deactivation"

if [ $test_results != 0 ]; then
    echo '--------------------------------------'
    echo 'BUILD FAILED'
    echo '--------------------------------------'
    exit 1;
else
    echo '--------------------------------------'
    echo 'BUILD SUCCEEDED'
    echo '--------------------------------------'
    exit 1;
fi
