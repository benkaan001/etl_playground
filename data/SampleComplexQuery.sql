/* ========================================
   Query: Customer Purchase Analysis
   Created: 1993-07-15
   Created by: Team_A
   Purpose: Analyzing customer purchases and loyalty
   ========================================
*/

WITH customer_purchases AS (
    SELECT
        -- Customer purchase details
        customer_id,
        purchase_date,
        purchase_amount,
        -- Commented out: additional column below
        -- purchase_type, // Update: 1998-05-20 - Deprecated field, kept for historical compatibility
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY purchase_date DESC) AS purchase_number
        -- Update: 2005-11-30 - Improved performance by optimizing window function
    FROM RANDOM_SCHEMA1.PURCHASES
),
customer_loyalty AS (
    SELECT
        -- Customer loyalty details
        customer_id,
        SUM(purchase_amount) AS total_spent,
        CASE
            WHEN total_spent >= 1000 THEN 'Gold'
            WHEN total_spent >= 500 THEN 'Silver'
            ELSE 'Bronze'
        END AS loyalty_level
    FROM customer_purchases
    GROUP BY customer_id
),
product_categories AS (
    SELECT
        -- Product categories
        product_id,
        category_id
    FROM RANDOM_SCHEMA2.PRODUCT_CATEGORIES
),
product_details AS (
    SELECT
        -- Product details
        product_id,
        product_name,
        product_description,
        product_price,
        -- Commented out: additional columns below
        -- product_weight, // Update: 2000-09-18 - Removed due to redundant information
        -- product_dimension // Update: 2012-04-03 - Renamed to product_size for consistency
    FROM RANDOM_SCHEMA3.PRODUCT_DETAILS
),
product_reviews AS (
    SELECT
        -- Product reviews
        product_id,
        reviewer_id,
        review_rating,
        review_comment
    FROM RANDOM_SCHEMA4.PRODUCT_REVIEWS
)

SELECT
    -- Final output
    cp.customer_id,
    cl.loyalty_level,
    pd.product_name,
    pd.product_description,
    pd.product_price,
    pr.review_rating,
    pr.review_comment
    -- Commented out: additional columns below
    -- pr.review_date, // Update: 2018-10-05 - Removed to streamline output
    -- pr.reviewer_name // Update: 2021-06-30 - Added reviewer ID for anonymity
FROM customer_purchases cp
INNER JOIN customer_loyalty cl ON cp.customer_id = cl.customer_id
INNER JOIN product_categories pc ON cp.product_id = pc.product_id
INNER JOIN product_details pd ON pc.product_id = pd.product_id
LEFT JOIN product_reviews pr ON pd.product_id = pr.product_id
WHERE cp.purchase_number <= 3 -- Limit to the most recent 3 purchases
ORDER BY cp.customer_id, pd.product_name;

/************** UPDATED ***************/

WITH customer_purchases AS (
    SELECT
        c.customer_id,
        p.product_id,
        p.purchase_date,
        p.purchase_quantity,
        RANK() OVER (PARTITION BY c.customer_id ORDER BY p.purchase_date DESC) AS purchase_rank
    FROM CUSTOMERS_SCHEMA.customers AS c
    JOIN PURCHASES_SCHEMA.purchases AS p ON c.customer_id = p.customer_id
), customer_preferences AS (
    SELECT
        cp.customer_id,
        cp.product_id,
        cp.purchase_quantity,
        ROW_NUMBER() OVER (PARTITION BY cp.customer_id ORDER BY cp.purchase_quantity DESC) AS preference_rank
    FROM customer_purchases AS cp
), product_categories AS (
    SELECT
        p.product_id,
        c.category_name
    FROM PRODUCTS_SCHEMA.products AS p
    JOIN CATEGORIES_SCHEMA.categories AS c ON p.category_id = c.category_id
), product_reviews AS (
    SELECT
        pr.product_id,
        pr.reviewer_id,
        pr.review_rating,
        AVG(pr.review_rating) OVER (PARTITION BY pr.product_id) AS avg_rating
    FROM PRODUCT_REVEIES_SCHEMA.product_reviews AS pr
), product_recommendations AS (
    SELECT
        cp.customer_id,
        cpc.product_id,
        cpc.purchase_quantity,
        cpc.preference_rank,
        pc.category_name,
        pr.review_rating,
        pr.avg_rating
    FROM customer_purchases AS cp
    JOIN customer_preferences AS cpc ON cp.customer_id = cpc.customer_id AND cp.product_id = cpc.product_id
    JOIN product_categories AS pc ON cp.product_id = pc.product_id
    LEFT JOIN product_reviews AS pr ON cp.product_id = pr.product_id AND cpc.customer_id != pr.reviewer_id
    WHERE cpc.purchase_rank <= 3
    AND cpc.preference_rank <= 5
    AND pc.category_name IN ('Electronics', 'Home Appliances', 'Clothing')
    AND pr.review_rating >= pr.avg_rating
)
SELECT *
FROM product_recommendations
ORDER BY customer_id, preference_rank;

