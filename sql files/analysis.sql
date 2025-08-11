-- ALL SQL QUERIES ARE AS PER MYSQL

-- (Question 1: Basic Claims Analysis)
-- Objective:
-- Find all customers who have made more than 2 claims in the year 2024.
-- Display customer_id, customer_name, total_claims, and total_claim_amount.
-- Sort results by total claim amount in descending order.

SELECT
    c.customer_id,                     
    c.name AS customer_name,           
    COUNT(cl.claim_id) AS total_claims, -- Total number of claims made by the customer
    ROUND(SUM(cl.claim_amount), 2) AS total_claim_amount -- Total claim amount rounded to 2 decimal places
FROM
    claims_sample AS cl                
JOIN policies_sample AS p               -- Join with policies table to link claims to policies
    ON cl.policy_id = p.policy_id
JOIN customers_sample AS c              -- Join with customers table to link policies to customers
    ON p.customer_id = c.customer_id
WHERE
    YEAR(cl.claim_date) = 2024           -- Filter: Only claims made in the year 2024
GROUP BY
    c.customer_id, c.name                -- Group by customer to aggregate claims
HAVING
    COUNT(cl.claim_id) > 2               -- Only include customers with more than 2 claims
ORDER BY
    total_claim_amount DESC;             -- Sorted by total claim amount in descending order



--Question 2: Premium Analysis by Demographics
--Calculate the average annual premium by age group and policy type. Group customers as:
-- Young (18-30)
-- Middle-aged (31-50)
-- Senior (51+)
-- Display age_group, policy_type, avg_premium, and customer_count. Order by age_group and policy_type.

SELECT
    -- Categorize customers into age groups
    CASE
        WHEN c.age BETWEEN 18 AND 30 THEN 'Young'
        WHEN c.age BETWEEN 31 AND 50 THEN 'Middle-aged'
        WHEN c.age >= 51 THEN 'Senior'
        ELSE 'Other'
    END AS age_group,
    p.policy_type,                              
    ROUND(AVG(p.annual_premium), 2) AS avg_premium, -- Average annual premium (2 decimal places)
    COUNT(DISTINCT p.customer_id) AS customer_count -- Number of unique customers in this group
FROM
    policies_sample AS p                     
JOIN customers_sample AS c                        -- Join with customers table to access age
    ON p.customer_id = c.customer_id
GROUP BY
    age_group, p.policy_type                      -- Group by both age group and policy type
ORDER BY
    -- Custom sort order for age groups 
    CASE 
        WHEN age_group = 'Young' THEN 1
        WHEN age_group = 'Middle-aged' THEN 2
        WHEN age_group = 'Senior' THEN 3
        ELSE 4
    END,
    p.policy_type;                              



--Question 3: Advanced Fraud Detection Analysis
--This is the challenging question requiring advanced SQL skills
--Create a comprehensive fraud analysis query that identifies:
--1. The detection method (detected_by) with the highest fraud catch rate
--2. Policy types most susceptible to fraud
--3. Customers with multiple fraudulent claims
--4. The monthly trend of fraud detection in 2024
--Your query should return:
--• detection_method
--• total_frauds_detected
--• fraud_catch_rate (percentage)
--• most_fraud_prone_policy_type
--• avg_fraud_amount
--• high_risk_customers (customers with 2+ fraudulent claims)
--Use CTEs, window functions, and advanced aggregations. Include proper handling of NULL values.


WITH 
-- CTE 1: Calculate each method's SHARE of all detected frauds
-- This CTE calculates key statistics for each detection method.
fraud_stats AS (
    SELECT 
        -- Standardize 'detected_by' by replacing NULL or empty values with 'AUTOMATED_SYSTEM'.
        CASE 
            WHEN fd.detected_by IS NULL OR fd.detected_by = '' THEN 'AUTOMATED_SYSTEM'
            ELSE fd.detected_by 
        END AS detected_by,
        -- Count the number of frauds detected by each method.
        SUM(CASE WHEN fd.is_fraudulent = 'True' THEN 1 ELSE 0 END) AS total_frauds_detected,
        
        -- CORRECTED CALCULATION: This now calculates the share of total frauds for each method.
        -- The denominator is a subquery that gets the total count of ALL fraudulent claims.
        CASE 
            -- Prevents division by zero if there are no fraudulent claims at all.
            WHEN (SELECT COUNT(*) FROM fraud_detection_sample WHERE is_fraudulent = 'True') = 0 THEN 0
            ELSE ROUND(
                (SUM(CASE WHEN fd.is_fraudulent = 'True' THEN 1 ELSE 0 END) * 100.0 / 
                (SELECT COUNT(*) FROM fraud_detection_sample WHERE is_fraudulent = 'True')), 2
            )
        END AS fraud_catch_rate
        
    FROM fraud_detection_sample fd
    -- Group the results by the standardized detection method.
    GROUP BY CASE 
        WHEN fd.detected_by IS NULL OR fd.detected_by = '' THEN 'AUTOMATED_SYSTEM'
        ELSE fd.detected_by 
    END
),

-- CTE 2: Find policy types most susceptible to fraud
-- This CTE counts the number of fraudulent claims for each policy type.
policy_fraud_stats AS (
    SELECT 
        p.policy_type,
        COUNT(*) AS fraud_count
    FROM policies_sample p
    INNER JOIN claims_sample c ON p.policy_id = c.policy_id
    INNER JOIN fraud_detection_sample fd ON c.claim_id = fd.claim_id
    WHERE fd.is_fraudulent = 'True'
    GROUP BY p.policy_type
),

-- CTE 3: Get the most fraud-prone policy type
-- This selects the single policy type with the highest fraud count from the previous CTE.
top_fraud_policy AS (
    SELECT policy_type
    FROM policy_fraud_stats
    ORDER BY fraud_count DESC
    LIMIT 1
),

-- CTE 4: Identify customers with 2+ fraudulent claims
-- This finds customers who have had two or more fraudulent claims.
high_risk_customers AS (
    SELECT 
        p.customer_id,
        COUNT(*) as fraud_count
    FROM policies_sample p
    INNER JOIN claims_sample c ON p.policy_id = c.policy_id
    INNER JOIN fraud_detection_sample fd ON c.claim_id = fd.claim_id
    WHERE fd.is_fraudulent = 'True'
    GROUP BY p.customer_id
    HAVING COUNT(*) >= 2
    ORDER BY p.customer_id
),

-- CTE 5: Calculate average fraud amounts by detection method
-- This calculates the average claim amount for frauds caught by each detection method.
avg_fraud_amounts AS (
    SELECT 
        CASE 
            WHEN fd.detected_by IS NULL OR fd.detected_by = '' THEN 'AUTOMATED_SYSTEM'
            ELSE fd.detected_by 
        END AS detected_by,
        ROUND(AVG(c.claim_amount), 2) AS avg_fraud_amount
    FROM fraud_detection_sample fd
    INNER JOIN claims_sample c ON fd.claim_id = c.claim_id
    WHERE fd.is_fraudulent = 'True'
    GROUP BY CASE 
        WHEN fd.detected_by IS NULL OR fd.detected_by = '' THEN 'AUTOMATED_SYSTEM'
        ELSE fd.detected_by 
    END
),

-- CTE 6: Monthly fraud trend for 2024
-- This aggregates the number of fraudulent claims for each month in 2024.
monthly_fraud_2024 AS (
    SELECT 
        DATE_FORMAT(fd.detection_date, '%Y-%m') AS month_year,
        SUM(CASE WHEN fd.is_fraudulent = 'True' THEN 1 ELSE 0 END) AS fraud_count
    FROM fraud_detection_sample fd
    WHERE YEAR(fd.detection_date) = 2024
    GROUP BY DATE_FORMAT(fd.detection_date, '%Y-%m')
    ORDER BY month_year
),

-- CTE 7: Create complete month series for 2024
-- This creates a reference table of all 12 months in 2024 to ensure no months are missed in the final report.
all_months_2024 AS (
    SELECT '2024-01' as month_year UNION ALL SELECT '2024-02' UNION ALL SELECT '2024-03' UNION ALL
    SELECT '2024-04' UNION ALL SELECT '2024-05' UNION ALL SELECT '2024-06' UNION ALL
    SELECT '2024-07' UNION ALL SELECT '2024-08' UNION ALL SELECT '2024-09' UNION ALL
    SELECT '2024-10' UNION ALL SELECT '2024-11' UNION ALL SELECT '2024-12'
),

-- CTE 8: Complete monthly trend with zeros
-- This joins the actual monthly fraud counts with the complete list of months, filling in 0 for months with no fraud.
complete_monthly_trend AS (
    SELECT 
        am.month_year,
        COALESCE(mf.fraud_count, 0) as fraud_count
    FROM all_months_2024 am
    LEFT JOIN monthly_fraud_2024 mf ON am.month_year = mf.month_year
    ORDER BY am.month_year
),

-- CTE 9: Get high-risk customer list
-- This compiles the list of high-risk customer IDs into a single comma-separated string.
high_risk_list AS (
    SELECT GROUP_CONCAT(customer_id ORDER BY customer_id SEPARATOR ',') AS customer_list
    FROM high_risk_customers
),

-- CTE 10: Get monthly trend string
-- This formats the complete monthly trend data into the final string format (e.g., '2024-01:0;2024-02:1').
trend_string AS (
    SELECT GROUP_CONCAT(
        CONCAT(month_year, ':', fraud_count) 
        ORDER BY month_year 
        SEPARATOR ';'
    ) AS monthly_trend
    FROM complete_monthly_trend
)

-- Final query - get the detection method with highest fraud catch rate
-- This assembles the final single-row report from all the previous CTEs.
SELECT 
    fs.detected_by AS detection_method,
    fs.total_frauds_detected,
    fs.fraud_catch_rate,
    tfp.policy_type AS most_fraud_prone_policy_type,
    afa.avg_fraud_amount,
    hrl.customer_list AS high_risk_customers,
    ts.monthly_trend AS monthly_fraud_trend_2024
FROM fraud_stats fs
-- CROSS JOIN is used for CTEs that produce a single row of summary data.
CROSS JOIN top_fraud_policy tfp
-- LEFT JOIN is used to connect the average fraud amount to its corresponding detection method.
LEFT JOIN avg_fraud_amounts afa ON fs.detected_by = afa.detected_by
CROSS JOIN high_risk_list hrl
CROSS JOIN trend_string ts
-- This WHERE clause filters the final result to only show the method(s) with the highest catch rate.
WHERE fs.fraud_catch_rate = (
    SELECT MAX(fraud_catch_rate) FROM fraud_stats
)
-- Order the results for consistency.
ORDER BY fs.fraud_catch_rate DESC, fs.total_frauds_detected DESC;
