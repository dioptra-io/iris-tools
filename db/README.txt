This directory contains schema and query files used by various tools
to interact with the databases.

`schema_tmp_metadata` defines the structure of the temporary metadata
table, which serves as a staging area for converting schema formats.
This table stores measurement metadata, retrievable via `irisctl`,
while additional metadata is sourced from the configuration file
and incorporated during the final insertion.  Using this staging
table enhances efficiency by minimizing the need for multiple
BigQuery calls, thereby streamlining the entire process.
