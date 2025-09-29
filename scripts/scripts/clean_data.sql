-- piccha_mart.purchases_mart
CREATE MATERIALIZED VIEW piccha_mart.purchases_mart_mv
TO piccha_mart.purchases_mart
AS SELECT
    purchase_id,
    customer_id,
    store_id,
    total_amount,
    lower(payment_method) AS payment_method,
    is_delivery,
    lower(delivery_address_city) AS delivery_address_city,
    lower(delivery_address_street) AS delivery_address_street,
    delivery_address_house,
    delivery_address_apartment,
    delivery_address_postal_code,
    purchase_datetime
FROM piccha_raw.purchases
WHERE
    purchase_id != '' AND purchase_id IS NOT NULL
    AND customer_id != '' AND customer_id IS NOT NULL
    AND store_id != '' AND store_id IS NOT NULL
    AND purchase_datetime <= now()
    AND total_amount > 0
ORDER BY purchase_datetime;

-- piccha_mart.customers_mart
CREATE MATERIALIZED VIEW piccha_mart.customers_mart_mv
TO piccha_mart.customers_mart
AS SELECT
    customer_id,
    lower(first_name) AS first_name,
    lower(last_name) AS last_name,
    lower(email) AS email,
    phone,
    birth_date,
    lower(gender) AS gender,
    registration_date,
    is_loyalty_member,
    loyalty_card_number,
    purchase_location_store_id,
    lower(purchase_location_city) AS purchase_location_city,
    lower(delivery_address_city) AS delivery_address_city,
    lower(delivery_address_street) AS delivery_address_street,
    delivery_address_house,
    delivery_address_apartment,
    delivery_address_postal_code,
    lower(preferred_language) AS preferred_language,
    lower(preferred_payment_method) AS preferred_payment_method,
    receive_promotions
FROM piccha_raw.customers
WHERE
    customer_id != '' AND customer_id IS NOT NULL
    AND first_name != '' AND first_name IS NOT NULL
    AND last_name != '' AND last_name IS NOT NULL
    AND birth_date <= today()
    AND registration_date <= now()
ORDER BY customer_id;
