-- ================================
-- BASIC METRICS
-- ================================

-- Total Sales
SELECT SUM(sales) AS total_sales 
FROM gold.fact_sales;

-- Total Items Sold
SELECT SUM(quantity) AS total_quantity 
FROM gold.fact_sales;

-- Average Selling Price
SELECT AVG(price) AS avg_price 
FROM gold.fact_sales;

-- Total Orders
SELECT COUNT(order_id) AS total_orders 
FROM gold.fact_sales;

-- Total Unique Orders
SELECT COUNT(DISTINCT order_id) AS total_unique_orders 
FROM gold.fact_sales;

-- Total Products
SELECT COUNT(product_id) AS total_products 
FROM gold.dim_product;

-- Total Customers
SELECT COUNT(DISTINCT customer_id) AS total_customers 
FROM gold.dim_customers;

-- Customers Who Have Placed Orders
SELECT COUNT(DISTINCT customer_key) AS total_ordered_customers 
FROM gold.fact_sales;

-- ================================
-- ALL METRICS SUMMARY REPORT
-- ================================

SELECT 'Total Sales' AS metric, SUM(sales) AS value FROM gold.fact_sales
UNION 
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION
SELECT 'Total Unique Orders', COUNT(DISTINCT order_id) FROM gold.fact_sales
UNION
SELECT 'Total Products', COUNT(product_id) FROM gold.dim_product
UNION
SELECT 'Total Customers', COUNT(DISTINCT customer_id) FROM gold.dim_customers;

-- ================================
-- CUSTOMER AND PRODUCT DISTRIBUTION
-- ================================

-- Customers by Country
SELECT country, COUNT(DISTINCT customer_id) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Customers by Gender
SELECT gender, COUNT(DISTINCT customer_id) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- Products by Category
SELECT category, COUNT(DISTINCT product_id) AS total_products
FROM gold.dim_product
GROUP BY category
ORDER BY total_products DESC;

-- Average Cost per Category
SELECT 
    p.category,
    AVG(s.price) AS avg_cost
FROM gold.fact_sales AS s
LEFT JOIN gold.dim_product AS p ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY avg_cost DESC;

-- Total Revenue per Category
SELECT 
    p.category,
    SUM(s.sales) AS total_revenue
FROM gold.fact_sales AS s
LEFT JOIN gold.dim_product AS p ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Revenue by Customer
SELECT 
    customer_key,
    SUM(sales) AS total_revenue
FROM gold.fact_sales
GROUP BY customer_key
ORDER BY total_revenue DESC;

-- Customer Details and Revenue
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.gender,
    c.marital_status,
    c.country,
    c.birth_date,
    SUM(s.sales) AS total_revenue
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales s ON c.customer_key = s.customer_key
GROUP BY c.customer_id, c.first_name, c.last_name, c.gender, c.marital_status, c.country, c.birth_date
ORDER BY total_revenue DESC;

-- Sold Items Distribution by Country
SELECT 
    c.country,
    SUM(s.quantity) AS total_quantity
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON s.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_quantity DESC;

-- ================================
-- PRODUCT PERFORMANCE ANALYSIS
-- ================================

-- Top 5 Highest Revenue Products
SELECT TOP 5 *
FROM (
    SELECT
        p.product_id,
        p.product_number,
        p.product_name,
        p.category,
        p.subcategory,
        SUM(s.sales) AS total_revenue,
        RANK() OVER(ORDER BY SUM(s.sales) DESC) AS revenue_rank
    FROM gold.dim_product p
    LEFT JOIN gold.fact_sales s ON s.product_key = p.product_key
    GROUP BY p.product_id, p.product_number, p.product_name, p.category, p.subcategory
) AS ranked_products;

-- ================================
-- TIME-BASED PERFORMANCE
-- ================================

-- Monthly Revenue & Quantity
SELECT 
    YEAR(order_date) AS order_year,
    DATEPART(MONTH, order_date) AS order_month,
    SUM(sales) AS total_revenue,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT customer_key) AS total_customers
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), DATEPART(MONTH, order_date)
ORDER BY order_year, order_month;

-- Running Monthly Total
SELECT 
    *,
    SUM(total_revenue) OVER(PARTITION BY YEAR(order_date) ORDER BY order_date) AS cumulative_sales
FROM (
    SELECT 
        DATETRUNC(MONTH, order_date) AS order_date,
        SUM(sales) AS total_revenue
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(MONTH, order_date)
) AS monthly;

-- Year-over-Year Product Performance
WITH product_revenue AS (
    SELECT 
        p.product_key,
        p.product_name,
        YEAR(s.order_date) AS order_year,
        SUM(s.sales) AS total_revenue
    FROM gold.dim_product p
    LEFT JOIN gold.fact_sales s ON p.product_key = s.product_key
    WHERE s.order_date IS NOT NULL
    GROUP BY p.product_key, p.product_name, YEAR(s.order_date)
),
sales_aggregation AS (
    SELECT *,
        AVG(total_revenue) OVER(PARTITION BY product_key) AS avg_revenue,
        LAG(total_revenue) OVER(PARTITION BY product_key ORDER BY order_year) AS previous_year_revenue
    FROM product_revenue
),
sales_variance AS (
    SELECT *,
        total_revenue - avg_revenue AS variance_avg,
        total_revenue - previous_year_revenue AS variance_yoy
    FROM sales_aggregation
)
SELECT * FROM sales_variance;

-- ================================
-- CATEGORY CONTRIBUTION TO SALES
-- ================================

WITH sales_category AS (
    SELECT 
        p.category,
        SUM(s.sales) AS category_sales
    FROM gold.dim_product p
    LEFT JOIN gold.fact_sales s ON p.product_key = s.product_key
    WHERE p.category IS NOT NULL
    GROUP BY p.category
)
SELECT 
    category,
    category_sales,
    SUM(category_sales) OVER() AS total_sales,
    ROUND(CAST(category_sales * 100.0 / SUM(category_sales) OVER() AS FLOAT), 2) AS contribution_percent
FROM sales_category;

-- ================================
-- PRODUCT SEGMENTATION BY COST
-- ================================

SELECT 
    CASE 
        WHEN cost < 700 THEN 'Low'
        WHEN cost >= 700 AND cost < 1500 THEN 'Average'
        ELSE 'High'
    END AS cost_segment,
    COUNT(product_key) AS total_products
FROM gold.dim_product
GROUP BY 
    CASE 
        WHEN cost < 700 THEN 'Low'
        WHEN cost >= 700 AND cost < 1500 THEN 'Average'
        ELSE 'High'
    END
ORDER BY total_products DESC;

-- ================================
-- CUSTOMER SPENDING SEGMENTATION
-- ================================

-- Simple Spending Tier
SELECT *,
    CASE
        WHEN total_revenue <= 1499 THEN 'Low'
        WHEN total_revenue BETWEEN 1500 AND 2499 THEN 'Medium'
        ELSE 'High'
    END AS spending_segment
FROM (
    SELECT 
        c.customer_key, 
        c.first_name,
        c.last_name,
        SUM(s.sales) AS total_revenue
    FROM gold.dim_customers c
    LEFT JOIN gold.fact_sales s ON c.customer_key = s.customer_key
    GROUP BY c.customer_key, c.first_name, c.last_name
) AS spending;

-- Full Segmentation: VIP, Regular, New
WITH customer_activity AS (
    SELECT 
        c.customer_key,
        MIN(s.order_date) AS first_order,
        MAX(s.order_date) AS last_order,
        SUM(s.sales) AS total_spent
    FROM gold.dim_customers c
    LEFT JOIN gold.fact_sales s ON c.customer_key = s.customer_key
    GROUP BY c.customer_key
),
segmented AS (
    SELECT *,
        DATEDIFF(MONTH, first_order, last_order) AS months_active
    FROM customer_activity
)
SELECT 
    CASE 
        WHEN months_active >= 12 AND total_spent > 5000 THEN 'VIP'
        WHEN months_active >= 12 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    COUNT(*) AS total_customers
FROM segmented
GROUP BY 
    CASE 
        WHEN months_active >= 12 AND total_spent > 5000 THEN 'VIP'
        WHEN months_active >= 12 THEN 'Regular'
        ELSE 'New'
    END;

-- ================================
-- CUSTOMER SALES VIEW
-- ================================

CREATE VIEW customer_sales AS
SELECT
    customer_key,
    first_name,
    last_name,
    DATEDIFF(YEAR, birth_date, GETDATE()) AS age,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS order_span_months,
    DATEDIFF(MONTH, MAX(order_date), GETDATE()) AS last_order_recency,
    SUM(sales) AS total_revenue,
    COUNT(order_id) AS total_orders,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT product_key) AS total_products,
    SUM(sales) / NULLIF(COUNT(order_id), 0) AS avg_order_value,
    SUM(sales) / NULLIF(DATEDIFF(MONTH, MIN(order_date), MAX(order_date)), 0) AS avg_monthly_spend
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales s ON c.customer_key = s.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name, c.birth_date;

GO

-- ================================
-- CUSTOMER ANALYTICS VIEW
-- ================================

CREATE VIEW gold.customer_sales_analytics AS
SELECT *,
    CASE 
        WHEN order_span_months >= 12 AND total_revenue > 5000 THEN 'VIP'
        WHEN order_span_months >= 12 THEN 'Regular'
        ELSE 'New'
    END AS sales_segment,
    CASE 
        WHEN age < 35 THEN 'Adult'
        WHEN age BETWEEN 35 AND 44 THEN 'Early Mid-Age'
        WHEN age BETWEEN 45 AND 54 THEN 'Mid-Age'
        WHEN age BETWEEN 55 AND 64 THEN 'Late Mid-Age'
        ELSE 'Retired'
    END AS age_group
FROM customer_sales;

GO
