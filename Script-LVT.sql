WITH user_lifetime_data AS (
    SELECT 
        user_id,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date
    FROM 
        project.games_payments gp
    GROUP BY 
        user_id
)
SELECT 
    user_id,
    first_payment_date,
    last_payment_date,
    last_payment_date - first_payment_date AS lifetime_days
FROM 
    user_lifetime_data
ORDER BY 
    user_id, first_payment_date;
   
 WITH user_total_revenue AS (
    SELECT 
        user_id,
        SUM(revenue_amount_usd) AS total_revenue
    FROM 
        project.games_payments gp
    GROUP BY 
        user_id
)
SELECT 
    user_id,
    total_revenue
FROM 
    user_total_revenue
ORDER BY 
    user_id
 
 