SELECT block_number,
    ANY_VALUE(block_time) AS block_time,
    COUNT(*) AS transactions,
    SUM(gas_used) AS total_gas_used,
    SUM(gas_used_for_l1) AS total_gas_used_for_l1,
    SUM(gas_used - gas_used_for_l1) AS total_gas_used_for_l2,
    SUM(length(data)) AS total_data_length,
    SUM(
        16 * (
            bytearray_length(data) - (
                length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))
            )
        ) --nonzero by
        + 4 * (
            (
                length(from_utf8(data)) - length(replace(from_utf8(data), chr(0), ''))
            )
        )
    ) AS total_calldata_gas_used,
    AVG(effective_gas_price / 1e9) AS avg_gas_price_qwei,
    SUM(effective_gas_price / 1e18 * gas_used) AS total_tx_fee_eth,
    AVG(effective_gas_price / 1e18 * gas_used) AS avg_tx_fee_eth,
    SUM(effective_gas_price / 1e18 * gas_used_for_l1) AS total_l1_fee_eth,
    AVG(effective_gas_price / 1e18 * gas_used_for_l1) AS avg_l1_fee_eth,
    SUM(
        effective_gas_price / 1e18 * (gas_used - gas_used_for_l1)
    ) AS total_l2_fee_eth,
    AVG(
        effective_gas_price / 1e18 * (gas_used - gas_used_for_l1)
    ) AS avg_l2_fee_eth
FROM arbitrum.transactions
WHERE date(block_date) between date('2023-11-15') AND date('2024-01-31')
    AND success
    AND "to" != 0x00000000000000000000000000000000000a4b05
GROUP BY block_number
ORDER BY block_number