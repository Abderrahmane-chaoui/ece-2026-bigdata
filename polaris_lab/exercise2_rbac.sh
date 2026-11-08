#!/usr/bin/env bash
# Exercise 2: catalog role silver_reader with privileges on the silver namespace only,
# a principal role silver_reader_role and a principal silver_analyst to test it.
set -euo pipefail
API="$POLARIS_URI/api/management/v1"
CAT="$POLARIS_CATALOG"

TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
H=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")

echo "== catalog role silver_reader"
curl -s -o /dev/null -w '%{http_code}\n' "${H[@]}" -X POST "$API/catalogs/$CAT/catalog-roles" \
  -d '{"catalogRole":{"name":"silver_reader"}}'
for p in NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  curl -s -o /dev/null -w "$p %{http_code}\n" "${H[@]}" -X PUT \
    "$API/catalogs/$CAT/catalog-roles/silver_reader/grants" \
    -d "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$p\"}}"
done
curl -s "${H[@]}" "$API/catalogs/$CAT/catalog-roles/silver_reader/grants" | jq -c '.grants[]'

echo "== principal role silver_reader_role -> silver_reader"
curl -s -o /dev/null -w '%{http_code}\n' "${H[@]}" -X POST "$API/principal-roles" \
  -d '{"principalRole":{"name":"silver_reader_role"}}'
curl -s -o /dev/null -w '%{http_code}\n' "${H[@]}" -X PUT \
  "$API/principal-roles/silver_reader_role/catalog-roles/$CAT" -d '{"catalogRole":{"name":"silver_reader"}}'

echo "== principal silver_analyst -> silver_reader_role"
if ! grep -q POLARIS_SILVER_ANALYST_CREDENTIAL /home/onyxia/.polaris-credentials.env; then
  curl -s "${H[@]}" -X POST "$API/principals" \
    -d '{"principal":{"name":"silver_analyst","properties":{}},"credentialRotationRequired":false}' \
    | jq -r '"POLARIS_SILVER_ANALYST_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
    >> /home/onyxia/.polaris-credentials.env
fi
curl -s -o /dev/null -w '%{http_code}\n' "${H[@]}" -X PUT "$API/principals/silver_analyst/principal-roles" \
  -d '{"principalRole":{"name":"silver_reader_role"}}'
curl -s "${H[@]}" "$API/principals/silver_analyst/principal-roles" | jq -c '[.roles[].name]'
