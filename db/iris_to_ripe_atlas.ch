WITH
-- Step 1: Prepare per-hop data per flow
hop_results AS (
    SELECT
        probe_src_addr,  
        probe_dst_prefix,
        probe_dst_addr,
        probe_src_port,
        far_ttl - 1 AS hop,
        groupArray(
            CASE 
                WHEN toString(far_addr) = '::' THEN '*'  -- Treat '::' as '*'
                ELSE toString(far_addr)
            END
        ) AS replies
    FROM <links__ table>
    GROUP BY
        probe_src_addr,
        probe_dst_prefix,
        probe_dst_addr,
        probe_src_port,
        far_ttl
)

-- Step 2: Assemble traceroute-like structure
SELECT
    4 AS af,
    probe_dst_prefix AS dst_addr,
    probe_dst_prefix AS dst_name,
    NULL AS endtime,
    probe_src_addr AS from,
    NULL AS msm_id,
    NULL AS paris_id,
    CASE
        WHEN probe_src_port > 24000 THEN probe_src_port
        ELSE toInt64(splitByChar('.', toString(probe_dst_addr))[4])
    END AS prb_id,
    'ICMP' AS proto,
    arrayMap(
        x -> tuple(
            x.1,
            x.2
        ),
        arraySort(
            x -> x.1,
            groupArray(tuple(hop, replies))
        )
    ) AS result
FROM hop_results
GROUP BY
    probe_src_addr,
    probe_dst_prefix,
    probe_dst_addr,
    probe_src_port
ORDER BY
    probe_dst_prefix


