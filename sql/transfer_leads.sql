INSERT INTO dds_beauty_salon.leads (
    lead_created_at, lead_id, d_lead_utm_source, d_lead_utm_medium,
    d_lead_utm_campaign, d_lead_utm_content, d_lead_utm_term, client_id
)
SELECT
    update_ts AS lead_created_at,
    (object_value::json)->>'lead_id' AS lead_id,
    COALESCE((object_value::json)->>'d_lead_utm_source', 'not_utm') AS d_lead_utm_source,
    COALESCE((object_value::json)->>'d_lead_utm_medium', 'not_utm') AS d_lead_utm_medium,
    COALESCE((object_value::json)->>'d_lead_utm_campaign', 'not_utm') AS d_lead_utm_campaign,
    COALESCE((object_value::json)->>'d_lead_utm_content', 'not_utm') AS d_lead_utm_content,
    COALESCE((object_value::json)->>'d_lead_utm_term', 'not_utm') AS d_lead_utm_term,
    (object_value::json)->>'client_id' AS client_id
FROM staging_beauty_salon.leads
ON CONFLICT (lead_id) DO NOTHING;
