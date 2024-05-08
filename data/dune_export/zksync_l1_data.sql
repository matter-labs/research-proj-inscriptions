---- Dune query ID: 3700005
WITH commit_data AS (
    SELECT l1_batch_number,
        ANY_VALUE(l1_gas_price_gwei) AS commit_l1_gas_price_gwei,
        SUM(l1_gas_used) AS commit_l1_gas_used,
        SUM(l1_tx_fee_eth) AS commit_l1_tx_fee_eth,
        SUM(l1_calldata_gas_used) AS commit_l1_calldata_gas_used
    FROM query_3702601
    WHERE l1_tx_type = 'commit'
    GROUP BY l1_batch_number
),
prove_data AS (
    SELECT l1_batch_number,
        ANY_VALUE(l1_gas_price_gwei) AS prove_l1_gas_price_gwei,
        SUM(l1_gas_used) AS prove_l1_gas_used,
        SUM(l1_tx_fee_eth) AS prove_l1_tx_fee_eth,
        SUM(l1_calldata_gas_used) AS prove_l1_calldata_gas_used
    FROM query_3702601
    WHERE l1_tx_type = 'prove'
    GROUP BY l1_batch_number
),
execute_data AS (
    SELECT l1_batch_number,
        ANY_VALUE(l1_gas_price_gwei) AS execute_l1_gas_price_gwei,
        SUM(l1_gas_used) AS execute_l1_gas_used,
        SUM(l1_tx_fee_eth) AS execute_l1_tx_fee_eth,
        SUM(l1_calldata_gas_used) AS execute_l1_calldata_gas_used
    FROM query_3702601
    WHERE l1_tx_type = 'execute'
    GROUP BY l1_batch_number
),
totals_data AS (
    SELECT l1_batch_number,
        SUM(l1_gas_used) AS l1_gas_used,
        SUM(l1_tx_fee_eth) AS l1_tx_fee_eth,
        SUM(l1_tx_data_length) AS l1_tx_data_length,
        SUM(l1_calldata_gas_used) AS l1_calldata_gas_used
    FROM query_3702601
    GROUP BY l1_batch_number
)
SELECT totals_data.l1_batch_number,
    l1_gas_used,
    l1_tx_fee_eth,
    l1_tx_data_length,
    l1_calldata_gas_used,
    commit_l1_gas_price_gwei,
    prove_l1_gas_price_gwei,
    execute_l1_gas_price_gwei,
    commit_l1_gas_used,
    prove_l1_gas_used,
    execute_l1_gas_used,
    commit_l1_tx_fee_eth,
    prove_l1_tx_fee_eth,
    execute_l1_tx_fee_eth,
    commit_l1_calldata_gas_used,
    prove_l1_calldata_gas_used,
    execute_l1_calldata_gas_used
FROM totals_data
    INNER JOIN commit_data ON totals_data.l1_batch_number = commit_data.l1_batch_number
    INNER JOIN prove_data ON totals_data.l1_batch_number = prove_data.l1_batch_number
    INNER JOIN execute_data ON totals_data.l1_batch_number = execute_data.l1_batch_number
ORDER BY totals_data.l1_batch_number