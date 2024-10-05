WITH months AS (
    SELECT DISTINCT date_trunc('month', payment_date)::date AS monthly_payment
    FROM project.games_payments
),
MRR_Monthly AS (
    SELECT 
        user_id,
        date_trunc('month', payment_date)::date AS monthly_payment,
        SUM(revenue_amount_usd) AS MRR 
    FROM 
        project.games_payments gp
    GROUP BY 
        user_id, monthly_payment
),
paid_users AS (
    SELECT 
        monthly_payment,
        COUNT(DISTINCT user_id) AS paid_users,
        SUM(MRR) AS total_mrr
    FROM 
        MRR_Monthly
    GROUP BY 
        monthly_payment
),
ARPPU AS (
    SELECT 
        monthly_payment,
        ROUND(SUM(MRR) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS ARPPU
    FROM 
        MRR_Monthly
    GROUP BY 
        monthly_payment
),
New_MRR AS (
    SELECT 
        user_id, 
        MIN(date_trunc('month', payment_date)::date) AS first_monthly_payment
    FROM 
        project.games_payments gp
    GROUP BY 
        user_id
),
New_MRR_users AS (
    SELECT 
        nw_mrr.first_monthly_payment AS monthly_payment,
        COUNT(nw_mrr.user_id) AS new_users_paid,
        SUM(mrr.MRR) AS new_MRR
    FROM 
        New_MRR nw_mrr
    JOIN 
        MRR_Monthly mrr ON nw_mrr.user_id = mrr.user_id
        AND nw_mrr.first_monthly_payment = mrr.monthly_payment
    GROUP BY 
        nw_mrr.first_monthly_payment
),
churn_users_date AS (
    SELECT 
        user_id,
        MAX(date_trunc('month', payment_date)::date) AS last_payment_month
    FROM 
        project.games_payments gp
    GROUP BY 
        user_id
),
churned_users AS (
    SELECT 
        cu.user_id,
        cu.last_payment_month AS churn_month
    FROM 
        churn_users_date cu
    WHERE 
        last_payment_month < (SELECT MAX(monthly_payment) FROM months)
),
churned_revenue AS (
    SELECT 
        cu.churn_month,
        SUM(mrr.MRR) AS churned_revenue
    FROM 
        churned_users cu
    JOIN 
        MRR_Monthly mrr ON cu.user_id = mrr.user_id
        AND cu.churn_month = mrr.monthly_payment
    GROUP BY 
        cu.churn_month
),
Churn_Rate AS (
    SELECT 
        cu.churn_month,
        COUNT(cu.user_id) AS churned_users,
        cr.churned_revenue,
        LAG(pu.paid_users) OVER (ORDER BY cu.churn_month) AS prev_paid_users,
        CASE 
            WHEN LAG(pu.paid_users) OVER (ORDER BY cu.churn_month) IS NULL OR LAG(pu.paid_users) OVER (ORDER BY cu.churn_month) = 0
            THEN 0
            ELSE ROUND(
                (COUNT(cu.user_id)::NUMERIC / LAG(pu.paid_users) OVER (ORDER BY cu.churn_month)) * 100, 
                2
            )
        END AS churn_rate
    FROM 
        churned_users cu
    LEFT JOIN churned_revenue cr ON cu.churn_month = cr.churn_month
    LEFT JOIN paid_users pu ON cu.churn_month = pu.monthly_payment
    GROUP BY 
        cu.churn_month, cr.churned_revenue, pu.paid_users
),
expansion_contraction_mrr AS (
    SELECT 
        mrr.user_id,
        mrr.monthly_payment,
        mrr.MRR,
        mrr.MRR - LAG(mrr.MRR) OVER (PARTITION BY mrr.user_id ORDER BY mrr.monthly_payment) AS delta_mrr
    FROM 
        MRR_Monthly mrr
),
expansion_mrr AS (
    SELECT 
        monthly_payment,
        SUM(delta_mrr) AS expansion_mrr
    FROM 
        expansion_contraction_mrr
    WHERE 
        delta_mrr > 0
    GROUP BY 
        monthly_payment
),
contraction_mrr AS (
    SELECT 
        monthly_payment,
        SUM(-delta_mrr) AS contraction_mrr
    FROM 
        expansion_contraction_mrr
    WHERE 
        delta_mrr < 0
    GROUP BY 
        monthly_payment
)
SELECT 
    months.monthly_payment,
    pu.paid_users,
    pu.total_mrr,
    arppu.ARPPU,
    n_mrr.new_users_paid,
    n_mrr.new_MRR,
    ch_r.churned_users,
    ch_r.churned_revenue,
    ch_r.churn_rate,
    ex_mrr.expansion_mrr,
    cont_mrr.contraction_mrr
FROM 
    months
LEFT JOIN paid_users pu ON months.monthly_payment = pu.monthly_payment
LEFT JOIN ARPPU arppu ON months.monthly_payment = arppu.monthly_payment
LEFT JOIN New_MRR_users n_mrr ON months.monthly_payment = n_mrr.monthly_payment
LEFT JOIN Churn_Rate ch_r ON months.monthly_payment = ch_r.churn_month
LEFT JOIN expansion_mrr ex_mrr ON months.monthly_payment = ex_mrr.monthly_payment
LEFT JOIN contraction_mrr cont_mrr ON months.monthly_payment = cont_mrr.monthly_payment
ORDER BY 
    months.monthly_payment;
