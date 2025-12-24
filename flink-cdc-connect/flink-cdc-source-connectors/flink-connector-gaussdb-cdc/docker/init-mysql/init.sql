-- Create products_sink table for GaussDB CDC sync testing
CREATE TABLE IF NOT EXISTS products_sink (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(255),
    price DECIMAL(10, 2),
    stock INT
);

-- Grant permissions
GRANT ALL PRIVILEGES ON inventory.* TO 'flinkuser'@'%';
FLUSH PRIVILEGES;
