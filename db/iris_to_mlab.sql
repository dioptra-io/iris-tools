  -- This query converts the "one reply per row" format from Iris's `results__` tables
  -- to the "tracelb" format output by Scamper.
  -- It also includes additional M-Lab metadata (NULL-filled for now).
  -- CH Export: SELECT * EXCEPT (reply_mpls_labels) FROM iris.results__... INTO OUTFILE 'results.json.gz'
  -- BQ Import: bq load --autodetect --replace --source_format NEWLINE_DELIMITED_JSON iris.results results.json.gz
  -- NOTE: `reply_mpls_labels` would have to be transformed from an array of tuples to an array of structs to be imported in BQ.
  -- Declare and set variables for the table
DECLARE
  scamper1_table STRING DEFAULT @scamper1_table_name_param;
DECLARE
  measurement_uuid STRING DEFAULT @measurement_uuid_param;
DECLARE
  table_name STRING DEFAULT @table_name_param;
DECLARE
  hostname STRING DEFAULT @host_param;
DECLARE
  start_time STRING DEFAULT @start_time_param;
DECLARE
  agent_uuid STRING DEFAULT @agent_uuid_param;
DECLARE
  min_ttl STRING DEFAULT @min_ttl_param;
DECLARE
  failure_probability STRING DEFAULT @failure_probability_param;
-- Creating a new table from the query results using EXECUTE IMMEDIATE
DECLARE convert_iris_to_scamper1 STRING;

-- Iris always uses IPv6 addresses internally and store IPv4 addresses
-- as IPv4-mapped IPv6 addresses. This converts IPv4-mapped addresses
-- back to regular IPv4s in dot-decimal notation.
CREATE TEMP FUNCTION format_addr(addr STRING) AS (
  REPLACE(addr, '::ffff:', '')
);

-- To be used when the timestamps will be in microseconds and not in seconds
/*
CREATE TEMP FUNCTION make_timestamp(ts TIMESTAMP) AS (
  STRUCT(
    DIV(UNIX_MICROS(ts), 1000000) AS Sec,
    MOD(UNIX_MICROS(ts), 1000000) AS Usec
  )
);
*/

-- Create the timestamp structure output by Scamper.
CREATE TEMP FUNCTION make_timestamp(ts TIMESTAMP) AS (
  STRUCT(
    CAST(UNIX_SECONDS(ts) AS INT64) AS Sec,
    NULL AS Usec
  )
);

SET convert_iris_to_scamper1 = FORMAT("""
INSERT INTO `%s` -- scamper1 table
WITH
  -- This CTE aggregates and summarizes network probe results, selecting the first reply for each unique probe
  -- and calculating the total number of replies
  results_with_replies_count AS (
    SELECT
      probe_protocol,
      format_addr(probe_src_addr) AS probe_src_addr,
      format_addr(probe_dst_addr) AS probe_dst_addr,
      probe_src_port,
      probe_ttl,
      format_addr(reply_src_addr) AS reply_src_addr,
      -- First reply values based on capture timestamp
      (ARRAY_AGG(reply_ttl ORDER BY capture_timestamp LIMIT 1))[OFFSET(0)] AS reply_ttl,
      (ARRAY_AGG(reply_icmp_type ORDER BY capture_timestamp LIMIT 1))[OFFSET(0)] AS reply_icmp_type,
      (ARRAY_AGG(reply_icmp_code ORDER BY capture_timestamp LIMIT 1))[OFFSET(0)] AS reply_icmp_code,
      (ARRAY_AGG(quoted_ttl ORDER BY capture_timestamp LIMIT 1))[OFFSET(0)] AS quoted_ttl,
      (ARRAY_AGG(capture_timestamp ORDER BY capture_timestamp LIMIT 1))[OFFSET(0)] AS capture_timestamp,
      (ARRAY_AGG(rtt ORDER BY capture_timestamp LIMIT 1))[OFFSET(0)] AS rtt,
      COUNT(*) AS reply_count,
      format_addr(probe_dst_prefix) AS probe_dst_prefix
    FROM
      `%s`
    -- Filter out rows to avoid duplicates
    WHERE NOT EXISTS (
      SELECT 1
      FROM `%s` scamper1
      WHERE
        -- Rows belonging to the same measurement and agent
        scamper1.id = '%s' AND
        scamper1.raw.CycleStart.Hostname = '%s' AND
        -- Check for duplicate rows
        scamper1.raw.Tracelb.dst = format_addr(probe_dst_prefix)
    )
    GROUP BY
      probe_protocol,
      probe_src_addr,
      probe_dst_addr,
      probe_src_port,
      probe_ttl,
      reply_src_addr,
      probe_dst_prefix
  ),

  -- This CTE establishes the links between probes and their corresponding replies,
  -- aggregating results as needed to manage probes with amplification.
  links AS (
   SELECT
     near.probe_protocol,
     near.probe_src_addr,
     near.probe_dst_addr AS probe_dst_addr,
     near.probe_src_port,
     near.reply_src_addr AS near_addr,
     far.reply_src_addr AS far_addr,
     far.reply_ttl AS reply_ttl,
     far.reply_icmp_type AS reply_icmp_type,
     far.reply_icmp_code AS reply_icmp_code,
     far.probe_ttl AS probe_ttl,
     far.quoted_ttl AS quoted_ttl,
     far.capture_timestamp AS capture_timestamp,
     far.rtt AS rtt,
     far.reply_count AS reply_count,
     near.probe_dst_prefix AS probe_dst_prefix
   FROM
     results_with_replies_count near
   INNER JOIN
     results_with_replies_count far ON
       near.probe_protocol = far.probe_protocol AND
       near.probe_src_addr = far.probe_src_addr AND
       near.probe_dst_addr = far.probe_dst_addr AND
       near.probe_src_port = far.probe_src_port AND
       near.probe_ttl = far.probe_ttl - 1
  ),

  -- This CTE groups probes and their corresponding replies for each far_addr,
  -- aggregating relevant details such as timestamps, TTL, RTT, and ICMP information.
  aggregated_probes AS (
   SELECT
     probe_protocol,
     probe_src_addr,
     probe_dst_prefix,
     probe_ttl,
     near_addr,
     far_addr,
     MIN(capture_timestamp) AS first_timestamp,
     MAX(capture_timestamp) AS last_timestamp,
     ARRAY_AGG(
       STRUCT(
         STRUCT(
           NULL AS Sec,
           NULL AS Usec
         ) AS Tx,
         reply_count AS Replyc,
         probe_ttl AS TTL,
         1 AS Attempt,  -- Our current tools only do a single attempt.
         CASE
           WHEN probe_src_port > 24000 THEN probe_src_port
           ELSE CAST(SPLIT(probe_dst_addr, '.')[OFFSET(3)] AS INT64)
         END AS Flowid,
         [
           STRUCT(
             STRUCT(
               NULL AS Sec,
               NULL AS Usec
             ) AS Rx,
             reply_ttl AS TTL,
             rtt/10.0 AS RTT,
             reply_icmp_type AS icmp_type,
             reply_icmp_code AS icmp_code,
             NULL AS icmp_q_tos,  -- Not stored in Iris
             quoted_ttl AS icmp_q_ttl
           )
         ] AS Replies
       )
     ) AS Probes
   FROM links
   GROUP BY
     probe_protocol,
     probe_src_addr,
     probe_dst_prefix,
     probe_ttl,
     near_addr,
     far_addr
  ),

  -- This CTE groups and aggregates probe data by probe_ttl,
  -- organizing responses from different far_addr values into structured Links.
  ttl_links AS (
    SELECT
     probe_protocol,
     probe_src_addr,
     probe_dst_prefix,
     probe_ttl,
     near_addr,
     MIN(first_timestamp) AS first_timestamp,
     MAX(last_timestamp) AS last_timestamp,
     ARRAY_AGG(
       STRUCT(
         COALESCE(far_addr, '*') AS Addr,
         Probes
       )
     ) AS Links
   FROM aggregated_probes
   GROUP BY
     probe_protocol,
     probe_src_addr,
     probe_dst_prefix,
     probe_ttl,
     near_addr
  ),

  -- This CTE aggregates probe-response link data at the node level (near_addr),
  -- consolidating TTL-based link structures into a summarized view of network paths.
  node_links AS (
   SELECT
     probe_protocol,
     probe_src_addr,
     probe_dst_prefix,
     near_addr,
     MIN(first_timestamp) AS first_timestamp,
     MAX(last_timestamp) AS last_timestamp,
     ARRAY_AGG(
       STRUCT(
         Links
       )
       ORDER BY probe_ttl
     ) AS links_by_node
   FROM ttl_links
   GROUP BY
     probe_protocol,
     probe_src_addr,
     probe_dst_prefix,
     near_addr
  )

SELECT
 '%s' AS id, -- measurement_uuid AS id
 STRUCT(
   CAST(NULL AS STRING) AS Version,
   CAST(NULL AS TIMESTAMP) AS Time,
   CAST(NULL AS STRING) AS ArchiveURL,
   CAST(NULL AS STRING) AS Filename,
   CAST(NULL AS INT64) AS Priority,
   CAST(NULL AS STRING) AS GitCommit
 ) AS parser,
 DATE(TIMESTAMP('%s')) AS date,
 STRUCT(
   STRUCT(
     '%s' AS UUID, -- agent UUID
     CAST(NULL AS STRING) AS TracerouteCallerVersion,
     CAST(NULL AS BOOLEAN) AS CachedResult,
     CAST(NULL AS STRING) AS CachedUUID
   ) AS Metadata,
   STRUCT(
     'cycle-start' AS Type,
     'default' AS list_name,
     CAST(NULL AS INT64) AS ID,
     '%s' AS Hostname,
     UNIX_SECONDS(MIN(first_timestamp)) AS start_time
   ) AS CycleStart,
   STRUCT(
     'tracelb' AS type,
     CAST(NULL AS STRING) AS version,
     CAST(NULL AS INT64) AS userid,
     'icmp-echo' AS method,
     probe_src_addr AS src,
     probe_dst_prefix AS dst,
     make_timestamp(MIN(first_timestamp)) AS start,
     CAST(NULL AS INT64) AS probe_size,  -- Not stored in Iris
     CAST('%s' AS INT64) AS firsthop,
     1 AS attempts,  -- Our current tools always send a single probe.
     100 - CAST(CAST('%s' AS FLOAT64)*100 AS INT64) AS confidence,
     CAST(NULL AS INT64) AS tos,  -- Not stored in Iris
     CAST(NULL AS INT64) AS gaplimit,  -- Not applicable
     CAST(NULL AS INT64) AS wait_timeout,  -- Not applicable
     CAST(NULL AS INT64) AS wait_probe,  -- Not applicable
     CAST(NULL AS INT64) AS probec,  -- Not applicable
     CAST(NULL AS INT64) AS probec_max,  -- Not applicable
     COUNT(*) AS nodec,
     (
       SELECT COUNT(DISTINCT CONCAT(near_addr, '|', far_addr))
       FROM links
       WHERE probe_protocol = nl.probe_protocol
         AND probe_src_addr = nl.probe_src_addr
         AND probe_dst_prefix = nl.probe_dst_prefix
     ) AS linkc,
     ARRAY_AGG(STRUCT(
       GENERATE_UUID() AS hop_id,
       COALESCE(near_addr, '*') AS addr,
       CAST(NULL AS STRING) AS name,  -- Not applicable
       CAST(NULL AS INT64) AS q_ttl,  -- Not applicable
       (
         SELECT COUNT(DISTINCT CONCAT(near_addr, '|', far_addr))
         FROM links
         WHERE probe_protocol = nl.probe_protocol
           AND probe_src_addr = nl.probe_src_addr
           AND probe_dst_prefix = nl.probe_dst_prefix
           AND near_addr = nl.near_addr
       ) AS linkc,
       links_by_node as links
     )) AS nodes
   ) AS Tracelb,
   STRUCT(
     'cycle-stop' AS Type,
     'default' AS list_name,
     CAST(NULL AS INT64) AS ID,
     '%s' AS Hostname,
     UNIX_SECONDS(MAX(last_timestamp)) AS stop_time
   ) AS CycleStop
 ) AS raw
FROM node_links nl
GROUP BY
 probe_protocol,
 probe_src_addr,
 probe_dst_prefix
""", scamper1_table, table_name, scamper1_table, measurement_uuid,
hostname, measurement_uuid, start_time, agent_uuid, hostname, min_ttl,
failure_probability, hostname);

EXECUTE IMMEDIATE
  convert_iris_to_scamper1;
