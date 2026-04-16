-- Data quality checks expressed as SQL, runnable against a warehouse
-- (BigQuery, Snowflake, Redshift) where the raw_* tables have been loaded.
--
-- Each CTE isolates one rule; the final SELECT unions the findings into a
-- single failures table that a dbt test or Airflow operator can assert on.
--
-- Assumes the fund is APCRED, treasury wallet is `:treasury`, and concentration
-- limit is 10% of outstanding tokens.

WITH params AS (
  SELECT
    CAST(0.01 AS FLOAT64) AS recon_tolerance,
    CAST(0.10 AS FLOAT64) AS concentration_limit,
    CAST(0.03 AS FLOAT64) AS nav_move_threshold
),

-- REC-001: on-chain balance vs cap table per investor
onchain_balance AS (
  SELECT
    to_address AS wallet,
    SUM(tokens) AS tokens_in
  FROM raw_token_transfers
  GROUP BY to_address
),
onchain_outflow AS (
  SELECT from_address AS wallet, SUM(tokens) AS tokens_out
  FROM raw_token_transfers
  GROUP BY from_address
),
onchain_net AS (
  SELECT
    COALESCE(i.wallet, o.wallet) AS wallet,
    COALESCE(i.tokens_in, 0) - COALESCE(o.tokens_out, 0) AS on_chain_balance
  FROM onchain_balance i
  FULL OUTER JOIN onchain_outflow o USING (wallet)
),
rec_001 AS (
  SELECT
    'REC-001' AS rule_id,
    'critical' AS severity,
    'investor' AS entity_type,
    c.investor_id AS entity_id,
    CONCAT('TA balance ', CAST(c.ta_balance AS STRING),
           ' vs on-chain ', CAST(COALESCE(n.on_chain_balance, 0) AS STRING)) AS message
  FROM raw_cap_table c
  LEFT JOIN onchain_net n ON n.wallet = c.wallet
  CROSS JOIN params p
  WHERE ABS(c.ta_balance - COALESCE(n.on_chain_balance, 0)) > p.recon_tolerance
),

-- REC-002: total supply reconciliation
supply AS (
  SELECT
    SUM(CASE WHEN event_type = 'mint' THEN tokens ELSE 0 END)
    - SUM(CASE WHEN event_type = 'burn' THEN tokens ELSE 0 END) AS outstanding
  FROM raw_token_transfers
),
cap_total AS (
  SELECT SUM(ta_balance) AS ta_total FROM raw_cap_table
),
rec_002 AS (
  SELECT
    'REC-002' AS rule_id,
    'critical' AS severity,
    'fund' AS entity_type,
    'APCRED' AS entity_id,
    CONCAT('Outstanding ', CAST(s.outstanding AS STRING),
           ' vs cap table ', CAST(c.ta_total AS STRING)) AS message
  FROM supply s, cap_total c
  WHERE ABS(s.outstanding - c.ta_total) > 1.0
),

-- KYC-001: transfers to expired KYC wallets
kyc_001 AS (
  SELECT
    'KYC-001' AS rule_id,
    'critical' AS severity,
    'transfer' AS entity_type,
    t.tx_hash AS entity_id,
    CONCAT('Transfer to investor ', k.investor_id, ' on ',
           CAST(DATE(t.block_timestamp) AS STRING),
           ' but KYC expired ', CAST(k.kyc_expires AS STRING)) AS message
  FROM raw_token_transfers t
  JOIN raw_kyc_events k ON k.wallet = t.to_address
  WHERE k.kyc_status = 'expired'
     OR k.kyc_expires < DATE(t.block_timestamp)
),

-- KYC-002: US holders must be accredited
kyc_002 AS (
  SELECT
    'KYC-002' AS rule_id,
    'high' AS severity,
    'investor' AS entity_type,
    investor_id AS entity_id,
    CONCAT('US investor ', investor_id, ' holds ',
           CAST(ta_balance AS STRING), ' tokens without accreditation') AS message
  FROM raw_cap_table
  WHERE jurisdiction = 'US' AND NOT accredited AND ta_balance > 0
),

-- WHT-001: transfers to non-whitelisted wallets
whitelist AS (
  SELECT wallet FROM raw_cap_table
),
wht_001 AS (
  SELECT
    'WHT-001' AS rule_id,
    'critical' AS severity,
    'transfer' AS entity_type,
    t.tx_hash AS entity_id,
    CONCAT('Transfer of ', CAST(t.tokens AS STRING),
           ' to non-whitelisted wallet ', t.to_address) AS message
  FROM raw_token_transfers t
  LEFT JOIN whitelist w ON w.wallet = t.to_address
  WHERE t.event_type = 'transfer'
    AND w.wallet IS NULL
    AND t.to_address NOT IN ('0x000000000000000000000000000000000000dead')
),

-- BAL-001: negative cap table balances
bal_001 AS (
  SELECT
    'BAL-001' AS rule_id,
    'critical' AS severity,
    'investor' AS entity_type,
    investor_id AS entity_id,
    CONCAT('Investor ', investor_id, ' has negative balance ', CAST(ta_balance AS STRING)) AS message
  FROM raw_cap_table
  WHERE ta_balance < 0
),

-- CON-001: concentration limit
total_positive AS (
  SELECT SUM(GREATEST(ta_balance, 0)) AS total FROM raw_cap_table
),
con_001 AS (
  SELECT
    'CON-001' AS rule_id,
    'high' AS severity,
    'investor' AS entity_type,
    investor_id AS entity_id,
    CONCAT('Investor ', investor_id, ' holds ',
           CAST(ROUND(100 * ta_balance / t.total, 2) AS STRING), '% of outstanding') AS message
  FROM raw_cap_table c
  CROSS JOIN total_positive t
  CROSS JOIN params p
  WHERE c.ta_balance / NULLIF(t.total, 0) > p.concentration_limit
),

-- NAV-001: missing NAV on business days
business_days AS (
  SELECT day
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      (SELECT MIN(nav_date) FROM raw_nav_daily),
      (SELECT MAX(nav_date) FROM raw_nav_daily)
    )
  ) AS day
  WHERE EXTRACT(DAYOFWEEK FROM day) NOT IN (1, 7)
),
nav_001 AS (
  SELECT
    'NAV-001' AS rule_id,
    'critical' AS severity,
    'nav' AS entity_type,
    CAST(b.day AS STRING) AS entity_id,
    CONCAT('No NAV recorded for business day ', CAST(b.day AS STRING)) AS message
  FROM business_days b
  LEFT JOIN raw_nav_daily n ON n.nav_date = b.day
  WHERE n.nav_date IS NULL
),

-- NAV-002: implausible daily NAV moves
nav_moves AS (
  SELECT
    nav_date,
    nav_per_token,
    (nav_per_token / LAG(nav_per_token) OVER (ORDER BY nav_date)) - 1 AS pct_change
  FROM raw_nav_daily
),
nav_002 AS (
  SELECT
    'NAV-002' AS rule_id,
    'high' AS severity,
    'nav' AS entity_type,
    CAST(nav_date AS STRING) AS entity_id,
    CONCAT('NAV moved ', CAST(ROUND(100 * pct_change, 2) AS STRING),
           '% on ', CAST(nav_date AS STRING)) AS message
  FROM nav_moves, params p
  WHERE ABS(pct_change) > p.nav_move_threshold
),

-- ING-001: duplicate tx hashes
ing_001 AS (
  SELECT
    'ING-001' AS rule_id,
    'medium' AS severity,
    'transfer' AS entity_type,
    tx_hash AS entity_id,
    CONCAT('Duplicate tx_hash: ', tx_hash) AS message
  FROM raw_token_transfers
  GROUP BY tx_hash
  HAVING COUNT(*) > 1
)

SELECT * FROM rec_001
UNION ALL SELECT * FROM rec_002
UNION ALL SELECT * FROM kyc_001
UNION ALL SELECT * FROM kyc_002
UNION ALL SELECT * FROM wht_001
UNION ALL SELECT * FROM bal_001
UNION ALL SELECT * FROM con_001
UNION ALL SELECT * FROM nav_001
UNION ALL SELECT * FROM nav_002
UNION ALL SELECT * FROM ing_001
ORDER BY
  CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
  rule_id;
