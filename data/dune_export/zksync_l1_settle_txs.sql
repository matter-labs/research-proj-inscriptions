---- Dune query ID: 3702601
WITH l1_settle_txs as (
    SELECT hash,
        CASE
            -- BlockCommit event. Same for pre- and post-Boojum
            WHEN l.topic0 = 0x8f2916b2f2d78cc5890ead36c06c0f6d5d112c7e103589947e8e2f0d6eddb763 THEN varbinary_to_uint256 (l.topic1) -- block_num info stored in topic1
            -- BlocksVerification event. Same for pre- and post-Boojum
            WHEN l.topic0 = 0x22c9005dd88c18b552a1cd7e8b3b937fcde9ca69213c1f658f54d572e4877a81 THEN varbinary_to_uint256 (l.topic2) -- block_num info stored in topic2
            -- BlockExecution event. Same for pre- and post-Boojum
            WHEN l.topic0 = 0x2402307311a4d6604e4e7b4c8a15a7e1213edb39c16a31efa70afb06030d3165 THEN varbinary_to_uint256 (l.topic1) -- block_num info stored in topic1
        END as l1_batch_number,
        CASE
            -- BlockCommit event. Same for pre- and post-Boojum
            WHEN l.topic0 = 0x8f2916b2f2d78cc5890ead36c06c0f6d5d112c7e103589947e8e2f0d6eddb763 THEN 'commit' -- BlocksVerification event. Same for pre- and post-Boojum
            WHEN l.topic0 = 0x22c9005dd88c18b552a1cd7e8b3b937fcde9ca69213c1f658f54d572e4877a81 THEN 'prove' -- BlockExecution event. Same for pre- and post-Boojum
            WHEN l.topic0 = 0x2402307311a4d6604e4e7b4c8a15a7e1213edb39c16a31efa70afb06030d3165 THEN 'execute'
        END AS l1_tx_type,
        t.gas_price / 1e9 AS l1_gas_price_gwei,
        t.gas_used as l1_gas_used,
        CAST(t.gas_price AS DOUBLE) / 1e18 * CAST(t.gas_used AS DOUBLE) AS l1_tx_fee_eth,
        length(t.data) as l1_tx_data_length,
        16 * (
            bytearray_length (t.data) - (
                length(from_utf8(t.data)) - length(replace(from_utf8(t.data), chr(0), ''))
            )
        ) --nonzero by
        + 4 * (
            (
                length(from_utf8(t.data)) - length(replace(from_utf8(t.data), chr(0), ''))
            )
        ) AS l1_calldata_gas_used
    FROM ethereum.logs l
        LEFT JOIN ethereum.transactions t on l.tx_hash = t.hash
    WHERE l.tx_to IN (
            0x3dB52cE065f728011Ac6732222270b3F2360d919 -- L1 transactions settle here pre-Boojum
,
            0xa0425d71cB1D6fb80E65a5361a04096E0672De03 -- L1 transactions settle here post-Boojum
,
            0xa8CB082A5a689E0d594d7da1E2d72A3D63aDc1bD -- L1 transactions settle here post-EIP4844
        )
        AND l.topic0 IN (
            0x8f2916b2f2d78cc5890ead36c06c0f6d5d112c7e103589947e8e2f0d6eddb763 -- BlockCommit event. Same for pre- and post-Boojum
,
            0x22c9005dd88c18b552a1cd7e8b3b937fcde9ca69213c1f658f54d572e4877a81 -- BlocksVerification event. Same for pre- and post-Boojum
,
            0x2402307311a4d6604e4e7b4c8a15a7e1213edb39c16a31efa70afb06030d3165 -- BlockExecution event. Same for pre- and post-Boojum
        )
        AND bytearray_substring (t.data, 1, 4) IN (
            0x0c4dd810 -- Commit Block, pre-Boojum
,
            0x701f58c5 -- Commit Batches, post-Boojum
,
            0x7739cbe7 -- Prove Block, pre-Boojum
,
            0x7f61885c -- Prove Batches, post-Boojum
,
            0xce9dcf16 -- Execute Block, pre-Boojum
,
            0xc3d93e7c -- Execute Batches, post-Boojum
        )
),
batch_data AS (
    SELECT l1_batch_number,
        SUM(gas_used) AS batch_l2_gas_used,
        SUM(length(data)) AS batch_l2_data_length,
        SUM(
            16 * (
                bytearray_length (data) - (
                    length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))
                )
            ) --nonzero by
            + 4 * (
                (
                    length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))
                )
            )
        ) AS batch_l2_calldata_gas_used
    FROM zksync.transactions
    GROUP BY l1_batch_number
),
raw_join_data as (
    SELECT hash,
        l1_settle_txs.l1_batch_number as l1_batch_number,
        l1_tx_type,
        l1_gas_price_gwei,
        l1_gas_used,
        l1_tx_fee_eth,
        l1_tx_data_length,
        l1_calldata_gas_used,
        batch_l2_gas_used,
        batch_l2_data_length,
        batch_l2_calldata_gas_used
    FROM l1_settle_txs
        LEFT JOIN batch_data on l1_settle_txs.l1_batch_number = batch_data.l1_batch_number
    ORDER BY l1_settle_txs.l1_batch_number
),
total_for_l1_post AS (
    SELECT hash,
        SUM(batch_l2_calldata_gas_used) AS l2_calldata_gas_used_total_for_l1_post
    FROM raw_join_data
    GROUP BY hash
)
SELECT raw_join_data.hash,
    l1_batch_number,
    l1_tx_type,
    l1_gas_price_gwei,
    l1_gas_used * CAST(batch_l2_calldata_gas_used AS DOUBLE) / CAST(l2_calldata_gas_used_total_for_l1_post AS DOUBLE) AS l1_gas_used,
    l1_tx_fee_eth * CAST(batch_l2_calldata_gas_used AS DOUBLE) / CAST(l2_calldata_gas_used_total_for_l1_post AS DOUBLE) AS l1_tx_fee_eth,
    l1_tx_data_length * CAST(batch_l2_calldata_gas_used AS DOUBLE) / CAST(l2_calldata_gas_used_total_for_l1_post AS DOUBLE) AS l1_tx_data_length,
    l1_calldata_gas_used * CAST(batch_l2_calldata_gas_used AS DOUBLE) / CAST(l2_calldata_gas_used_total_for_l1_post AS DOUBLE) AS l1_calldata_gas_used,
    batch_l2_gas_used,
    batch_l2_data_length,
    batch_l2_calldata_gas_used,
    CAST(batch_l2_calldata_gas_used AS DOUBLE) / CAST(l2_calldata_gas_used_total_for_l1_post AS DOUBLE) AS batch_calldata_gas_ratio_in_post
FROM raw_join_data
    LEFT JOIN total_for_l1_post on raw_join_data.hash = total_for_l1_post.hash
ORDER BY l1_batch_number