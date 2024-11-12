  -- This query converts the "one reply per row" format from Iris's `results__` tables
  -- to the "tracelb" format output by Scamper.
  -- It also includes additional M-Lab metadata (NULL-filled for now).
  -- CH Export: SELECT * EXCEPT (reply_mpls_labels) FROM iris.results__... INTO OUTFILE 'results.json.gz'
  -- BQ Import: bq load --autodetect --replace --source_format NEWLINE_DELIMITED_JSON iris.results results.json.gz
  -- NOTE: `reply_mpls_labels` would have to be transformed from an array of tuples to an array of structs to be imported in BQ.
  -- Declare and set variables for the table
DECLARE
  scamper_table_name STRING DEFAULT @scamper_table_name_param;
DECLARE
  table_name STRING DEFAULT @table_name_param;
DECLARE
  hostname STRING DEFAULT @host_param;
DECLARE
  version STRING DEFAULT @version_param;
DECLARE
  user_id STRING DEFAULT @user_id_param;
DECLARE
  tool STRING DEFAULT @tool_param;
DECLARE
  min_ttl STRING DEFAULT @min_ttl_param;
DECLARE
  failure_probability STRING DEFAULT @failure_probability_param;
  -- Creating a new table from the query results using EXECUTE IMMEDIATE
DECLARE
  create_table_sql STRING;
  -- Iris always uses IPv6 addresses internally and store IPv4 addresses
  -- as IPv4-mapped IPv6 addresses. This converts IPv4-mapped addresses
  -- back to regular IPv4s in dot-decimal notation.
CREATE TEMP FUNCTION
  format_addr(addr STRING) AS ( REPLACE(addr, '::ffff:', '') );
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
CREATE TEMP FUNCTION
  make_timestamp(ts TIMESTAMP) AS ( STRUCT( UNIX_SECONDS(ts) AS Sec,
      NULL AS Usec ) );
SET
  create_table_sql = FORMAT("""
CREATE TABLE `%s` AS  -- Iris table in format scamper1
WITH
-- This CTE counts the number of replies for each unique combination of probe parameters.
replies_count AS (
  SELECT
    probe_protocol,
    probe_src_addr,
    probe_dst_addr,
    probe_src_port,
    probe_dst_port,
    probe_ttl,
    reply_src_addr,
    COUNT(*) AS reply_count  -- Count the number of replies for the specified group
  FROM `%s` -- Input Table containing all original results
  GROUP BY
    probe_protocol,
    probe_src_addr,
    probe_dst_addr,
    probe_src_port,
    probe_dst_port,
    probe_ttl,
    reply_src_addr  -- Grouping by all relevant fields to get distinct reply counts
),

-- This CTE adds the reply count to the original results while formatting addresses for easier reading.
results_with_replies_count AS (
  SELECT
    results.*,
    reply_count,  -- Include the reply count calculated in the previous CTE
    format_addr(results.probe_src_addr) AS formatted_probe_src_addr,
    format_addr(results.probe_dst_addr) AS formatted_probe_dst_addr,
    format_addr(results.reply_src_addr) AS formatted_reply_src_addr
  FROM `%s` AS results -- Input table containing all original results
  LEFT JOIN replies_count replies
    ON results.probe_protocol  = replies.probe_protocol
    AND results.probe_src_addr = replies.probe_src_addr
    AND results.probe_dst_addr = replies.probe_dst_addr
    AND results.probe_src_port = replies.probe_src_port
    AND results.probe_dst_port = replies.probe_dst_port
    AND results.probe_ttl      = replies.probe_ttl
    AND results.reply_src_addr = replies.reply_src_addr
),

-- This CTE establishes the links between probes and their corresponding replies,
-- aggregating results as needed to manage probes with amplification.
-- For probes that result in amplification, only the first reply is displayed,
-- while the reply count includes all received replies.
links AS (
  SELECT
    near.probe_protocol,
    near.formatted_probe_src_addr AS probe_src_addr,
    near.formatted_probe_dst_addr AS probe_dst_addr,
    near.probe_src_port,
    near.probe_dst_port,
    near.formatted_reply_src_addr AS near_addr,
    far.formatted_reply_src_addr  AS far_addr,
    far.probe_ttl                 AS probe_ttl,
    -- Use ARRAY_AGG with ORDER BY and LIMIT 1 to get the first reply value based on the capture timestamp
    (ARRAY_AGG(far.reply_ttl ORDER BY far.capture_timestamp LIMIT 1))[OFFSET(0)]         AS reply_ttl,
    (ARRAY_AGG(far.reply_icmp_type ORDER BY far.capture_timestamp LIMIT 1))[OFFSET(0)]   AS reply_icmp_type,
    (ARRAY_AGG(far.reply_icmp_code ORDER BY far.capture_timestamp LIMIT 1))[OFFSET(0)]   AS reply_icmp_code,
    (ARRAY_AGG(far.quoted_ttl ORDER BY far.capture_timestamp LIMIT 1))[OFFSET(0)]        AS quoted_ttl,
    (ARRAY_AGG(far.capture_timestamp ORDER BY far.capture_timestamp LIMIT 1))[OFFSET(0)] AS capture_timestamp,
    (ARRAY_AGG(far.rtt ORDER BY far.capture_timestamp LIMIT 1))[OFFSET(0)]               AS rtt,
    MIN(far.reply_count) AS reply_count
  FROM results_with_replies_count near
  INNER JOIN results_with_replies_count far
    ON near.probe_protocol             = far.probe_protocol
    AND near.formatted_probe_src_addr = far.formatted_probe_src_addr
    AND near.formatted_probe_dst_addr = far.formatted_probe_dst_addr
    AND near.probe_src_port           = far.probe_src_port
    AND near.probe_dst_port           = far.probe_dst_port
    AND near.probe_ttl                = far.probe_ttl - 1
  GROUP BY
    probe_protocol,
    probe_src_addr,
    probe_dst_addr,
    probe_src_port,
    probe_dst_port,
    near_addr,
    far_addr,
    probe_ttl
),
links_by_node AS (
  SELECT
    probe_protocol,
    probe_src_addr,
    probe_dst_addr,
    near_addr,
    MIN(capture_timestamp) AS first_timestamp,
    MAX(capture_timestamp) AS last_timestamp,
    ARRAY_AGG(STRUCT(
      COALESCE(far_addr, '*') AS Addr,
      [
        STRUCT(
          -- Iris stores the RTT in 1/10th of milliseconds (e.g. 123 = 12.3ms).
          -- Use it to infer the tx time from the rx time.
          -- make_timestamp(TIMESTAMP_SUB(capture_timestamp, INTERVAL rtt * 100 MICROSECOND)) AS Tx,
          NULL           AS Tx,
          reply_count    AS Replyc,
          probe_ttl      AS TTL,
          1              AS Attempt, -- Our current tools only do a single attempt.
          CASE
            WHEN probe_src_port > 24000 THEN probe_src_port  -- Use probe_src_port if greater than 24000
            ELSE CAST(SPLIT(probe_dst_addr, '.')[OFFSET(3)] AS INT64)  -- Else, use the last octet of probe_dst_addr
          END AS Flowid,
          [
            STRUCT(
              -- make_timestamp(capture_timestamp) AS Rx,
              NULL            AS Rx,
              reply_ttl       AS TTL,
              rtt/10          AS RTT,
              reply_icmp_type AS icmp_type,
              reply_icmp_code AS icmp_code,
              NULL            AS icmp_q_tos, -- Not stored in Iris
              quoted_ttl      AS icmp_q_ttl
            )
          ] AS Replies
        )
      ] AS Probes
    )) AS Links
  FROM links
  GROUP BY 1, 2, 3, 4
)
SELECT
  NULL AS id,
  -- TODO: @SaiedKazemi I'll let you fill the right values for the `parser` structure.
  STRUCT(
    NULL AS Version,
    NULL AS Time,
    NULL AS ArchiveURL,
    NULL AS Filename,
    NULL AS Priority,
    NULL AS GitCommit
  ) AS parser,
  CURRENT_DATE() AS date,
  STRUCT(
    -- TODO: @SaiedKazemi I'll let you fill the right values for the `Metadata` structure.
    STRUCT(
      GENERATE_UUID() AS UUID,
      NULL AS TracerouteCallerVersion,
      NULL AS CachedResult,
      NULL AS CachedUUID
    ) AS Metadata,
    STRUCT(
      'cycle-start'                           AS Type,
      'default'                               AS list_name,
      0                                       AS ID,
      '%s' AS Hostname,
      UNIX_SECONDS(MIN(last_timestamp))       AS start_time
    ) AS CycleStart,
    STRUCT(
      'cycle-stop'                            AS Type,
      'default'                               AS list_name,
      0                                       AS ID,
      '%s' AS Hostname,
      UNIX_SECONDS(MAX(last_timestamp))       AS stop_time
    ) AS CycleStop,
    STRUCT(
      'tracelb'                                  AS type,
      '%s'     AS version,
      '%s'     AS userid,
      '%s'     AS method,
      probe_src_addr                             AS src,
      probe_dst_addr                             AS dst,
      make_timestamp(MIN(first_timestamp))       AS start,
      NULL AS probe_size,   -- Not stored in Iris
      CAST('%s' AS INT)     AS firsthop,
      1                                          AS attempts,     -- Our current tools always send a single probe.
      1.0 - CAST('%s' AS FLOAT64)  AS confidence,
      NULL                                       AS tos,          -- Not stored in Iris
      NULL                                       AS gaplimit,     -- Not applicable
      NULL                                       AS wait_timeout, -- Not applicable
      NULL                                       AS wait_probe,   -- Not applicable
      NULL                                       AS probec,       -- TODO: Retrieve actual probe count from the measurement metadata.
      NULL                                       AS probec_max,   -- Not applicable
      COUNT(*)                                   AS nodec,
      (SELECT
        COUNT(DISTINCT CONCAT(near_addr, '|', far_addr))
        FROM links
        WHERE probe_protocol = lbn.probe_protocol
        AND probe_src_addr = lbn.probe_src_addr
        AND probe_dst_addr = lbn.probe_dst_addr) AS linkc,
      ARRAY_AGG(STRUCT(
        GENERATE_UUID()                  AS hop_id,
        COALESCE(near_addr, '*')         AS addr,
        NULL                             AS name,  -- Not applicable
        NULL                             AS q_ttl, -- Not applicable
        (SELECT
          COUNT(DISTINCT CONCAT(near_addr, '|', far_addr))
          FROM links
          WHERE probe_protocol = lbn.probe_protocol
          AND probe_src_addr = lbn.probe_src_addr
          AND probe_dst_addr = lbn.probe_dst_addr
          AND near_addr = lbn.near_addr) AS linkc,
        ARRAY(SELECT AS STRUCT Links)    AS links
      ))                                         AS nodes
    ) AS Tracelb
  ) AS raw
FROM links_by_node lbn
GROUP BY probe_protocol, probe_src_addr, probe_dst_addr
""", scamper_table_name, table_name, table_name, hostname, hostname, version, user_id, tool, min_ttl, failure_probability);
EXECUTE IMMEDIATE
  create_table_sql;
