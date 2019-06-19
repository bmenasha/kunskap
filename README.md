To run the query:

python main.py bmenasha-1.cud_sud_test_data.test_1_export bmenasha-1.cud_sud_test_data.test_1_commitments pg_query.sql  | bq query --nouse_legacy_sql --format json | jq
