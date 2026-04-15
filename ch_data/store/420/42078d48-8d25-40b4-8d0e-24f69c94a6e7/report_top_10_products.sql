ATTACH TABLE _ UUID 'bc120cc5-bb3a-4883-8012-2a33994c124d'
(
    `product_id` Int32,
    `product_name` String,
    `total_sold_quantity` Int64,
    `total_revenue` Float64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
