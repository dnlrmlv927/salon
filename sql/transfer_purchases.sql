INSERT INTO dds_beauty_salon.purchases (
    purchase_created_at, purchase_id, client_id, m_purchase_amount
)
SELECT
    update_ts::DATE AS purchase_created_at,
    CAST((object_value::json)->>'purchase_id' AS UUID) AS purchase_id,
    (object_value::json)->>'client_id' AS client_id,
    COALESCE(NULLIF((object_value::json)->>'m_purchase_amount', 'NaN'), '0.0')::FLOAT AS m_purchase_amount
FROM staging_beauty_salon.purchases
ON CONFLICT (purchase_id) DO NOTHING;
