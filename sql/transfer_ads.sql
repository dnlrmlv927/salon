INSERT INTO dds_beauty_salon.ads (
    created_at, d_ad_account_id, d_utm_source, d_utm_medium,
    d_utm_campaign, d_utm_content, d_utm_term, m_clicks, m_cost
)
SELECT
    update_ts::DATE AS created_at,
    (object_value::json)->>'d_ad_account_id' AS d_ad_account_id,
    COALESCE((object_value::json)->>'d_utm_source', 'not_utm') AS d_utm_source,
    COALESCE((object_value::json)->>'d_utm_medium', 'not_utm') AS d_utm_medium,
    COALESCE((object_value::json)->>'d_utm_campaign', 'not_utm') AS d_utm_campaign,
    COALESCE((object_value::json)->>'d_utm_content', 'not_utm') AS d_utm_content,
    COALESCE((object_value::json)->>'d_utm_term', 'not_utm') AS d_utm_term,
    COALESCE(NULLIF((object_value::json)->>'m_clicks', 'NaN'), '0')::numeric AS m_clicks,
    COALESCE(NULLIF((object_value::json)->>'m_cost', 'NaN'), '0.0')::FLOAT AS m_cost
FROM staging_beauty_salon.ads;
