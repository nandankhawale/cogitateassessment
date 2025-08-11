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

-- CTE 1: Create a base table of all confirmed fraudulent claims.
-- This is the most important simplification. It joins all the tables once
-- and filters for fraud, so we don't have to repeat these joins in later steps.
WITH FraudulentClaims AS (
    SELECT
        p.policy_type,
        p.customer_id,
        c.claim_amount,
        fd.detection_date,
        -- Standardize the 'detected_by' field, handling NULL or empty values.
        COALESCE(NULLIF(fd.detected_by, ''), 'AUTOMATED_SYSTEM') AS detected_by
    FROM claims_sample c
    JOIN policies_sample p ON c.policy_id = p.policy_id
    JOIN fraud_detection_sample fd ON c.claim_id = fd.claim_id
    WHERE fd.is_fraudulent = 'True'
),

-- CTE 2: Calculate all statistics related to the detection methods.
-- This combines several of your original CTEs into one.
DetectionMethodStats AS (
    SELECT
        detected_by,
        COUNT(*) AS total_frauds_detected,
        ROUND(AVG(claim_amount), 2) AS avg_fraud_amount,
        -- Calculate the share of total frauds found by this method.
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM FraudulentClaims), 2) AS fraud_catch_rate
    FROM FraudulentClaims
    GROUP BY detected_by
)

-- Final Query: Select from the main stats and use subqueries for the rest.
-- This structure avoids the need for many separate CTEs for formatting.
SELECT
    dms.detected_by AS detection_method,
    dms.total_frauds_detected,
    dms.fraud_catch_rate,
    dms.avg_fraud_amount,

    -- Subquery to get the most fraud-prone policy type.
    (SELECT policy_type FROM FraudulentClaims GROUP BY policy_type ORDER BY COUNT(*) DESC LIMIT 1) AS most_fraud_prone_policy_type,

    -- Subquery to get the comma-separated list of high-risk customers.
    (SELECT GROUP_CONCAT(customer_id ORDER BY customer_id)
     FROM (SELECT customer_id FROM FraudulentClaims GROUP BY customer_id HAVING COUNT(*) >= 2) AS high_risk_subquery
    ) AS high_risk_customers,

    -- Subquery to generate the complete monthly trend string for 2024.
    (SELECT GROUP_CONCAT(CONCAT(m.month_year, ':', COALESCE(f.fraud_count, 0)) ORDER BY m.month_year SEPARATOR ';')
     FROM (
         -- Create a reference table of all 12 months in 2024.
         SELECT '2024-01' as month_year UNION ALL SELECT '2024-02' UNION ALL SELECT '2024-03' UNION ALL
         SELECT '2024-04' UNION ALL SELECT '2024-05' UNION ALL SELECT '2024-06' UNION ALL
         SELECT '2024-07' UNION ALL SELECT '2024-08' UNION ALL SELECT '2024-09' UNION ALL
         SELECT '2024-10' UNION ALL SELECT '2024-11' UNION ALL SELECT '2024-12'
     ) AS m
     -- Join the actual fraud counts, filling in 0 for months with no fraud.
     LEFT JOIN (
         SELECT DATE_FORMAT(detection_date, '%Y-%m') AS month_year, COUNT(*) AS fraud_count
         FROM FraudulentClaims
         WHERE YEAR(detection_date) = 2024
         GROUP BY month_year
     ) AS f ON m.month_year = f.month_year
    ) AS monthly_fraud_trend_2024

FROM DetectionMethodStats dms
-- Filter to only show the detection method with the highest catch rate.
WHERE dms.fraud_catch_rate = (SELECT MAX(fraud_catch_rate) FROM DetectionMethodStats)
ORDER BY dms.fraud_catch_rate DESC, dms.total_frauds_detected DESC;
