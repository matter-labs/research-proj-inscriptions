---- Dune query ID: 3685205
WITH l1_data AS (
    SELECT *
    FROM query_3700005 --WHERE l1_batch_number between 308707 and 420802
),
zksync_txs AS (
    SELECT l1_batch_number,
        success,
        block_time,
        gas_price / 1e9 As gas_price_gwei,
        gas_used,
        gas_price * gas_used / 1e18 AS tx_fee_eth,
        length(data) AS data_length,
        16 * (
            bytearray_length(data) - (
                length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))
            )
        ) --nonzero by
        + 4 * (
            (
                length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))
            )
        ) AS calldata_gas_used
    FROM zksync.transactions
    WHERE date(block_time) between date('2023-11-15') and date('2024-11-17')
)
SELECT l1_data.l1_batch_number,
    MAX(block_time) as last_block_time,
    COUNT(*) AS transactions,
    SUM(gas_used) AS total_gas_used,
    ANY_VALUE(l1_gas_used) AS total_gas_used_for_l1,
    SUM(gas_used) - ANY_VALUE(l1_gas_used) AS total_gas_used_for_l2,
    SUM(data_length) AS total_data_length_on_l2,
    ANY_VALUE(l1_tx_data_length) AS total_data_length_on_l1,
    SUM(calldata_gas_used) AS total_calldata_gas_used_on_l2,
    ANY_VALUE(l1_calldata_gas_used) AS total_calldata_gas_used_on_l1,
    AVG(gas_price_gwei) AS avg_gas_price_gwei,
    SUM(tx_fee_eth) AS total_tx_fee_eth,
    AVG(tx_fee_eth) AS avg_tx_fee_eth,
    ANY_VALUE(l1_tx_fee_eth) AS total_l1_fee_eth,
    ANY_VALUE(l1_tx_fee_eth) / COUNT(*) AS avg_l1_fee_eth,
    SUM(tx_fee_eth) - ANY_VALUE(l1_tx_fee_eth) AS total_l2_fee_eth,
    AVG(tx_fee_eth) - ANY_VALUE(l1_tx_fee_eth) / COUNT(*) AS avg_l2_fee_eth,
    ANY_VALUE(commit_l1_gas_used) AS total_gas_used_for_commit,
    ANY_VALUE(prove_l1_gas_used) AS total_gas_used_for_prove,
    ANY_VALUE(execute_l1_gas_used) AS total_gas_used_for_execute,
    ANY_VALUE(commit_l1_gas_price_gwei) AS l1_gas_price_for_commit_gwei,
    ANY_VALUE(prove_l1_gas_price_gwei) AS l1_gas_price_for_prove_gwei,
    ANY_VALUE(execute_l1_gas_price_gwei) AS l1_gas_price_for_execute_gwei,
    ANY_VALUE(commit_l1_tx_fee_eth) AS total_l1_tx_fee_for_commit_eth,
    ANY_VALUE(prove_l1_tx_fee_eth) AS total_l1_tx_fee_for_prove_eth,
    ANY_VALUE(execute_l1_tx_fee_eth) AS total_l1_tx_fee_for_execute_eth
FROM zksync_txs
    LEFT JOIN l1_data on zksync_txs.l1_batch_number = l1_data.l1_batch_number
WHERE success = true
GROUP BY l1_data.l1_batch_number
ORDER BY l1_data.l1_batch_number