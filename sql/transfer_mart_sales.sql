       WITH leads_with_ads AS (
            SELECT
                l.lead_created_at,
                l.lead_id,
                l.client_id,
                l.d_lead_utm_source,
                l.d_lead_utm_medium,
                l.d_lead_utm_campaign,
                a.m_clicks AS clicks,
                a.m_cost AS cost
            FROM dds_beauty_salon.leads AS l
            JOIN dds_beauty_salon.ads AS a
                ON l.lead_created_at::date = a.created_at
                AND l.d_lead_utm_source = a.d_utm_source
                AND l.d_lead_utm_medium = a.d_utm_medium
                AND l.d_lead_utm_campaign = a.d_utm_campaign
                AND l.d_lead_utm_content = a.d_utm_content
                AND l.d_lead_utm_term = a.d_utm_term
        ),

        sales_with_leads AS (
            SELECT
                p.purchase_created_at,
                p.client_id,
                p.m_purchase_amount AS revenue,
                l.lead_id,
                l.lead_created_at,
                l.d_lead_utm_source,
                l.d_lead_utm_medium,
                l.d_lead_utm_campaign,
                ROW_NUMBER() OVER (
                    PARTITION BY p.client_id, p.purchase_created_at
                    ORDER BY l.lead_created_at DESC
                ) AS rn
            FROM dds_beauty_salon.purchases AS p
            JOIN leads_with_ads AS l
                ON p.client_id = l.client_id
                AND p.purchase_created_at >= l.lead_created_at::date
                AND p.purchase_created_at <= l.lead_created_at::date + INTERVAL '15 days'
        )

        INSERT INTO dm_beauty_salon.global_metrics
        SELECT
            DATE_TRUNC('day', l.lead_created_at) AS date,
            l.d_lead_utm_source AS utm_source,
            l.d_lead_utm_medium AS utm_medium,
            l.d_lead_utm_campaign AS utm_campaign,
            SUM(l.clicks) AS clicks,
            SUM(l.cost) AS cost,
            COUNT(DISTINCT l.lead_id) AS leads,
            COUNT(DISTINCT CASE WHEN s.lead_id IS NOT NULL THEN s.lead_id END) AS purchases,
            SUM(s.revenue) AS revenue,
            SUM(l.cost) / NULLIF(COUNT(DISTINCT l.lead_id), 0) AS cpl,
            SUM(s.revenue) / NULLIF(SUM(l.cost), 0) AS roas
        FROM leads_with_ads AS l
        LEFT JOIN sales_with_leads AS s
            ON l.lead_id = s.lead_id
            AND s.rn = 1
        GROUP BY
            DATE_TRUNC('day', l.lead_created_at),
            l.d_lead_utm_source,
            l.d_lead_utm_medium,
            l.d_lead_utm_campaign
        ORDER BY
            date, utm_source, utm_medium, utm_campaign;
