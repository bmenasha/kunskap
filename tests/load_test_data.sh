bq rm -f cud_sud_test_data.test_1_export
cat test_1/export.json | jq -rc . > test_1/export_load.json
bq load    --source_format NEWLINE_DELIMITED_JSON   cud_sud_test_data.test_1_export  test_1/export_load.json  billing_export_schema.json

bq rm -f cud_sud_test_data.test_1_commitments
cat test_1/commitments.json | jq -rc . > test_1/commitments_load.json
bq load   --source_format NEWLINE_DELIMITED_JSON  cud_sud_test_data.test_1_commitments  test_1/commitments_load.json  commitments_schema.json
