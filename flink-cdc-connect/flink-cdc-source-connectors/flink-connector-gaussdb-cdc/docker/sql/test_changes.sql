-- GaussDB 变更测试脚本
-- 在 GaussDB 中执行此脚本来测试 CDC 变更捕获

-- ============================================
-- 测试 INSERT 操作
-- ============================================

-- 插入新订单
INSERT INTO orders (customer_name, product_name, price, quantity, order_status)
VALUES ('测试用户A', 'iPhone 15 Pro', 9999.00, 1, 'PENDING');

INSERT INTO orders (customer_name, product_name, price, quantity, order_status)
VALUES ('测试用户B', 'MacBook Air', 9999.00, 2, 'PENDING');

-- 等待 2 秒
SELECT pg_sleep(2);

-- ============================================
-- 测试 UPDATE 操作
-- ============================================

-- 更新订单状态
UPDATE orders
SET order_status = 'PROCESSING', updated_at = CURRENT_TIMESTAMP
WHERE customer_name = '测试用户A';

UPDATE orders
SET order_status = 'SHIPPED', updated_at = CURRENT_TIMESTAMP
WHERE customer_name = '测试用户B';

-- 等待 2 秒
SELECT pg_sleep(2);

-- 更新价格
UPDATE orders
SET price = 8999.00, updated_at = CURRENT_TIMESTAMP
WHERE customer_name = '测试用户A';

-- 等待 2 秒
SELECT pg_sleep(2);

-- ============================================
-- 测试 DELETE 操作
-- ============================================

-- 删除测试订单
DELETE FROM orders WHERE customer_name = '测试用户A';

-- 等待 2 秒
SELECT pg_sleep(2);

-- ============================================
-- 批量操作测试
-- ============================================

-- 批量插入
INSERT INTO orders (customer_name, product_name, price, quantity, order_status) VALUES
('批量用户1', 'Product A', 100.00, 1, 'PENDING'),
('批量用户2', 'Product B', 200.00, 2, 'PENDING'),
('批量用户3', 'Product C', 300.00, 3, 'PENDING');

-- 等待 2 秒
SELECT pg_sleep(2);

-- 批量更新
UPDATE orders
SET order_status = 'COMPLETED', updated_at = CURRENT_TIMESTAMP
WHERE customer_name LIKE '批量用户%';

-- 等待 2 秒
SELECT pg_sleep(2);

-- 批量删除
DELETE FROM orders WHERE customer_name LIKE '批量用户%';

-- ============================================
-- 清理测试数据
-- ============================================

-- 删除剩余测试数据
DELETE FROM orders WHERE customer_name = '测试用户B';

-- 查看最终结果
SELECT * FROM orders ORDER BY order_id;

COMMIT;
