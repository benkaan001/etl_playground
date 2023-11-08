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

