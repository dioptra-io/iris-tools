This directory contains schema and query files used by various tools
to interact with the databases.

`schema_tmp_metadata` defines the schema for the temporary metadata table used as a staging area.
This table includes measurement metadata, which can be obtained using irisctl, while other metadata are sourced from the configuration file and added during the final insertion.
Using this staging table helps improve efficiency by avoiding multiple calls to BigQuery, streamlining the overall process.
