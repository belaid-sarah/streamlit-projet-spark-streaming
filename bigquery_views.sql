-- Vue 1: Préférences par tranche d'âge
CREATE OR REPLACE VIEW `spark-streaming-483317.shopping.v_age_preferences` AS
WITH age_buckets AS (
  SELECT
    CASE
      WHEN age < 25 THEN '18-24'
      WHEN age BETWEEN 25 AND 34 THEN '25-34'
      WHEN age BETWEEN 35 AND 44 THEN '35-44'
      WHEN age BETWEEN 45 AND 54 THEN '45-54'
      WHEN age BETWEEN 55 AND 64 THEN '55-64'
      ELSE '65+'
    END AS age_bucket,
    category,
    purchase_amount_usd,
    review_rating
  FROM `spark-streaming-483317.shopping.orders`
),
age_stats AS (
  SELECT
    age_bucket,
    COUNT(*) AS orders,
    AVG(purchase_amount_usd) AS avg_spend,
    AVG(review_rating) AS avg_rating
  FROM age_buckets
  GROUP BY age_bucket
),
top_categories AS (
  SELECT
    age_bucket,
    category,
    COUNT(*) AS category_count
  FROM age_buckets
  GROUP BY age_bucket, category
  QUALIFY ROW_NUMBER() OVER (PARTITION BY age_bucket ORDER BY COUNT(*) DESC) = 1
)
SELECT
  a.age_bucket,
  a.orders,
  a.avg_spend,
  a.avg_rating,
  t.category AS top_category
FROM age_stats a
LEFT JOIN top_categories t ON a.age_bucket = t.age_bucket;

-- Vue 2: Préférences par genre
CREATE OR REPLACE VIEW `spark-streaming-483317.shopping.v_gender_preferences` AS
WITH gender_stats AS (
  SELECT
    gender,
    COUNT(*) AS orders,
    AVG(purchase_amount_usd) AS avg_spend,
    AVG(review_rating) AS avg_rating
  FROM `spark-streaming-483317.shopping.orders`
  GROUP BY gender
),
top_categories AS (
  SELECT
    gender,
    category,
    COUNT(*) AS category_count
  FROM `spark-streaming-483317.shopping.orders`
  GROUP BY gender, category
  QUALIFY ROW_NUMBER() OVER (PARTITION BY gender ORDER BY COUNT(*) DESC) = 1
)
SELECT
  g.gender,
  g.orders,
  g.avg_spend,
  g.avg_rating,
  t.category AS top_category
FROM gender_stats g
LEFT JOIN top_categories t ON g.gender = t.gender;

-- Vue 3: Préférences par localisation
CREATE OR REPLACE VIEW `spark-streaming-483317.shopping.v_location_preferences` AS
WITH location_stats AS (
  SELECT
    location,
    COUNT(*) AS orders,
    AVG(purchase_amount_usd) AS avg_spend
  FROM `spark-streaming-483317.shopping.orders`
  GROUP BY location
),
top_categories AS (
  SELECT
    location,
    category,
    COUNT(*) AS category_count
  FROM `spark-streaming-483317.shopping.orders`
  GROUP BY location, category
  QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY COUNT(*) DESC) <= 3
)
SELECT
  l.location,
  l.orders,
  l.avg_spend,
  ARRAY_AGG(t.category ORDER BY t.category_count DESC) AS top_categories
FROM location_stats l
LEFT JOIN top_categories t ON l.location = t.location
GROUP BY l.location, l.orders, l.avg_spend;

-- Vue 4: Analyse combinée âge x genre x catégorie
CREATE OR REPLACE VIEW `spark-streaming-483317.shopping.v_age_gender_category` AS
WITH base AS (
  SELECT
    CASE
      WHEN age < 25 THEN '18-24'
      WHEN age BETWEEN 25 AND 34 THEN '25-34'
      WHEN age BETWEEN 35 AND 44 THEN '35-44'
      WHEN age BETWEEN 45 AND 54 THEN '45-54'
      WHEN age BETWEEN 55 AND 64 THEN '55-64'
      ELSE '65+'
    END AS age_bucket,
    gender,
    category,
    purchase_amount_usd
  FROM `spark-streaming-483317.shopping.orders`
)
SELECT
  age_bucket,
  gender,
  category,
  COUNT(*) AS orders,
  AVG(purchase_amount_usd) AS avg_spend
FROM base
GROUP BY age_bucket, gender, category;


