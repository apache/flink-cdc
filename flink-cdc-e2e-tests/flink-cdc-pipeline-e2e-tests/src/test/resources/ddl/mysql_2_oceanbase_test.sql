CREATE TABLE products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  description VARCHAR(512),
  weight FLOAT,
  enum_c enum('red', 'white') default 'red',  -- test some complex types as well,
  json_c JSON,                                -- because we use additional dependencies to deserialize complex types.
  point_c POINT
);
ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (default,"scooter","Small 2-wheel scooter",3.14, 'red', '{"key1": "value1"}', ST_GeomFromText('POINT(1 1)')),
       (default,"car battery","12V car battery",8.1, 'white', '{"key2": "value2"}', ST_GeomFromText('POINT(2 2)')),
       (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8, 'red', '{"key3": "value3"}', ST_GeomFromText('POINT(3 3)')),
       (default,"hammer","12oz carpenter's hammer",0.75, 'white', '{"key4": "value4"}', ST_GeomFromText('POINT(4 4)')),
       (default,"hammer","14oz carpenter's hammer",0.875, 'red', '{"k1": "v1", "k2": "v2"}', ST_GeomFromText('POINT(5 5)')),
       (default,"hammer","16oz carpenter's hammer",1.0, null, null, null),
       (default,"rocks","box of assorted rocks",5.3, null, null, null),
       (default,"jacket","water resistent black wind breaker",0.1, null, null, null),
       (default,"spare tire","24 inch spare tire",22.2, null, null, null);