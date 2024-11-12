-- This query converts the "one reply per row" format from Iris's `results__` tables
-- to the "tracelb" format output by Scamper.
-- It also includes additional M-Lab metadata (NULL-filled for now).

-- CH Export: SELECT * EXCEPT (reply_mpls_labels) FROM iris.results__... INTO OUTFILE 'results.json.gz'
-- BQ Import: bq load --autodetect --replace --source_format NEWLINE_DELIMITED_JSON iris.results results.json.gz
-- NOTE: `reply_mpls_labels` would have to be transformed from an array of tuples to an array of structs to be imported in BQ.

-- Iris always uses IPv6 addresses internally and store IPv4 addresses
-- as IPv4-mapped IPv6 addresses. This converts IPv4-mapped addresses
-- back to regular IPv4s in dot-decimal notation.
CREATE TEMP FUNCTION format_addr(addr STRING) AS (
  REPLACE(addr, '::ffff:', '')
);

-- Create the timestamp structure output by Scamper.
CREATE TEMP FUNCTION make_timestamp(ts TIMESTAMP) AS (
  STRUCT(
    DIV(UNIX_MICROS(ts), 1000000) AS Sec,
    MOD(UNIX_MICROS(ts), 1000000) AS Usec
  ) 
);

WITH results AS (
    SELECT * REPLACE (
      format_addr(probe_src_addr) AS probe_src_addr,
      format_addr(probe_dst_addr) AS probe_dst_addr,
      format_addr(reply_src_addr) AS reply_src_addr
    ) FROM iris.results -- Input table
),
links AS (
  SELECT
    near.probe_protocol,
    near.probe_src_addr,
    near.probe_dst_addr,
    near.probe_src_port,
    near.probe_dst_port,
    near.reply_src_addr   AS near_addr,
    far.reply_src_addr    AS far_addr,
    far.reply_ttl         AS reply_ttl,
    far.reply_icmp_type   AS reply_icmp_type,
    far.reply_icmp_code   AS reply_icmp_code,
    far.probe_ttl         AS probe_ttl,
    far.quoted_ttl        AS quoted_ttl,
    far.capture_timestamp AS capture_timestamp,
    far.rtt               AS rtt
  FROM results near
  LEFT JOIN results far
    ON near.probe_protocol  = far.probe_protocol
    AND near.probe_src_addr = far.probe_src_addr
    AND near.probe_dst_addr = far.probe_dst_addr
    AND near.probe_src_port = far.probe_src_port
    AND near.probe_dst_port = far.probe_dst_port
    AND near.probe_ttl      = far.probe_ttl - 1
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
          make_timestamp(TIMESTAMP_SUB(capture_timestamp, INTERVAL rtt * 100 MICROSECOND)) AS Tx,
          1              AS Replyc,
          probe_ttl      AS TTL,
          1              AS Attempt, -- Our current tools only do a single attempt.
          probe_src_port AS Flowid,
          [
            STRUCT(
              make_timestamp(capture_timestamp) AS Rx,
              reply_ttl       AS TTL,
              rtt             AS RTT,
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
      'cycle-start' AS Type,
      'default'     AS list_name,
      0             AS ID,
      'iris'        AS Hostname, -- TODO: Retrieve agent hostname from measurement metadata.
      UNIX_SECONDS(first_timestamp) AS start_time
    ) AS CycleStart,
    STRUCT(
      'cycle-stop' AS Type,
      'default'    AS list_name,
      0            AS ID,
      'iris'       AS Hostname, -- TODO: Retrieve agent hostname from measurement metadata.
      UNIX_SECONDS(last_timestamp) AS stop_time
    ) AS CycleStop,
    STRUCT(
      'tracelb'       AS type,
      '1.x.x'         AS version,-- TODO: Set Iris version from measurement metadata.
      0               AS userid, -- TODO: Set user UUID from measurement metadata.
      'diamond-miner' AS method, -- TODO: Use actual tool name from measurement metadata.
      probe_src_addr  AS src,
      probe_dst_addr  AS dst,
      make_timestamp(first_timestamp) AS start,
      NULL AS probe_size,   -- Not stored in Iris
      1    AS firsthop,     -- TODO: Retrieve the actual min_ttl from the measurement metadata.
      1    AS attempts,     -- Our current tools always send a single probe.
      95   AS confidence,   -- TODO: Retrieve the actual value from the measurement metadata.
      NULL AS tos,          -- Not stored in Iris
      NULL AS gaplimit,     -- Not applicable
      NULL AS wait_timeout, -- Not applicable
      NULL AS wait_probe,   -- Not applicable
      NULL AS probec,       -- TODO: Retrieve actual probe count from the measurement metadata.
      NULL AS probec_max,   -- Not applicable
      COUNT(*)                 AS nodec,
      SUM(ARRAY_LENGTH(links)) AS linkc,
      ARRAY_AGG(STRUCT(
        GENERATE_UUID()          AS hop_id,
        COALESCE(near_addr, '*') AS addr,
        NULL                     AS name,  -- Not applicable
        NULL                     AS q_ttl, -- Not applicable
        ARRAY_LENGTH(links)      AS linkc,
        ARRAY(SELECT AS STRUCT Links) AS links
      )) AS nodes
    ) AS Tracelb
  ) AS raw
FROM links_by_node
GROUP BY probe_protocol, probe_src_addr, probe_dst_addr, first_timestamp, last_timestamp
