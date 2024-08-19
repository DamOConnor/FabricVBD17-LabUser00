-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "d8b07883-20af-4902-95af-b7c0932273ee",
-- META       "default_lakehouse_name": "lh_tpch",
-- META       "default_lakehouse_workspace_id": "8fe40bbb-ca75-4743-948c-f527f765f011"
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # tpc-h queries
-- Adapted from here:  
-- https://examples.citusdata.com/tpch.queries.html

-- MARKDOWN ********************

-- ## Query 1

-- CELL ********************

-- MAGIC %%sql
-- MAGIC -- tpc-h query 1
-- MAGIC SELECT
-- MAGIC     returnflag,
-- MAGIC     linestatus,
-- MAGIC     sum(quantity) as sum_qty,
-- MAGIC     sum(extendedprice) as sum_base_price,
-- MAGIC     sum(extendedprice * (1 - discount)) as sum_disc_price,
-- MAGIC     sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,
-- MAGIC     avg(quantity) as avg_qty,
-- MAGIC     avg(extendedprice) as avg_price,
-- MAGIC     avg(discount) as avg_disc,
-- MAGIC     count(*) as count_order
-- MAGIC FROM
-- MAGIC     lineitem
-- MAGIC WHERE
-- MAGIC     shipdate <= date '1998-12-01' - interval '90' day
-- MAGIC GROUP BY
-- MAGIC     returnflag,
-- MAGIC     linestatus
-- MAGIC ORDER BY
-- MAGIC     returnflag,
-- MAGIC     linestatus;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Query 3

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT
-- MAGIC     l.orderkey,
-- MAGIC     sum(l.extendedprice * (1 - l.discount)) as revenue,
-- MAGIC     o.orderdate,
-- MAGIC     o.shippriority
-- MAGIC FROM
-- MAGIC     customer c,
-- MAGIC     orders o,
-- MAGIC     lineitem l
-- MAGIC WHERE
-- MAGIC     c.mktsegment = 'BUILDING'
-- MAGIC     AND c.custkey = o.custkey
-- MAGIC     AND l.orderkey = o.orderkey
-- MAGIC     AND o.orderdate < date '1995-03-15'
-- MAGIC     AND l.shipdate > date '1995-03-15'
-- MAGIC GROUP BY
-- MAGIC     l.orderkey,
-- MAGIC     o.orderdate,
-- MAGIC     o.shippriority
-- MAGIC ORDER BY
-- MAGIC     revenue desc,
-- MAGIC     o.orderdate
-- MAGIC LIMIT 20;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Query 5

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT
-- MAGIC     n.name,
-- MAGIC     sum(l.extendedprice * (1 - l.discount)) as revenue
-- MAGIC FROM
-- MAGIC     customer c,
-- MAGIC     orders o,
-- MAGIC     lineitem l,
-- MAGIC     supplier s,
-- MAGIC     nation n,
-- MAGIC     region r
-- MAGIC WHERE
-- MAGIC     c.custkey = o.custkey
-- MAGIC     AND l.orderkey = o.orderkey
-- MAGIC     AND l.suppkey = s.suppkey
-- MAGIC     AND c.nationkey = s.nationkey
-- MAGIC     AND s.nationkey = n.nationkey
-- MAGIC     AND n.regionkey = r.regionkey
-- MAGIC     AND r.name = 'ASIA'
-- MAGIC     AND o.orderdate >= date '1994-01-01'
-- MAGIC     AND o.orderdate < date '1994-01-01' + interval '1' year
-- MAGIC GROUP BY
-- MAGIC     n.name
-- MAGIC ORDER BY
-- MAGIC     revenue desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Query 6

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT
-- MAGIC     sum(extendedprice * discount) as revenue
-- MAGIC FROM
-- MAGIC     lineitem
-- MAGIC WHERE
-- MAGIC     shipdate >= date '1994-01-01'
-- MAGIC     AND shipdate < date '1994-01-01' + interval '1' year
-- MAGIC     AND discount between 0.06 - 0.01 AND 0.06 + 0.01
-- MAGIC     AND quantity < 24;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Query 10

-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT
-- MAGIC     c.custkey,
-- MAGIC     c.name,
-- MAGIC     sum(l.extendedprice * (1 - l.discount)) as revenue,
-- MAGIC     c.acctbal,
-- MAGIC     n.name,
-- MAGIC     c.address,
-- MAGIC     c.phone,
-- MAGIC     c.comment
-- MAGIC FROM
-- MAGIC     customer c,
-- MAGIC     orders o,
-- MAGIC     lineitem l,
-- MAGIC     nation n
-- MAGIC WHERE
-- MAGIC     c.custkey = o.custkey
-- MAGIC     AND l.orderkey = o.orderkey
-- MAGIC     AND o.orderdate >= date '1993-10-01'
-- MAGIC     AND o.orderdate < date '1993-10-01' + interval '3' month
-- MAGIC     AND l.returnflag = 'R'
-- MAGIC     AND c.nationkey = n.nationkey
-- MAGIC GROUP BY
-- MAGIC     c.custkey,
-- MAGIC     c.name,
-- MAGIC     c.acctbal,
-- MAGIC     c.phone,
-- MAGIC     n.name,
-- MAGIC     c.address,
-- MAGIC     c.comment
-- MAGIC ORDER BY
-- MAGIC     revenue desc
-- MAGIC LIMIT 20;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Query 12

-- CELL ********************

SELECT
    l.shipmode,
    sum(case
        when o.orderpriority = '1-URGENT'
            OR o.orderpriority = '2-HIGH'
            then 1
        else 0
    end) as high_line_count,
    sum(case
        when o.orderpriority <> '1-URGENT'
            AND o.orderpriority <> '2-HIGH'
            then 1
        else 0
    end) AS low_line_count
FROM
    orders o,
    lineitem l
WHERE
    o.orderkey = l.orderkey
    AND l.shipmode in ('MAIL', 'SHIP')
    AND l.commitdate < l.receiptdate
    AND l.shipdate < l.commitdate
    AND l.receiptdate >= date '1994-01-01'
    AND l.receiptdate < date '1994-01-01' + interval '1' year
GROUP BY
    l.shipmode
ORDER BY
    l.shipmode;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Query 14

-- CELL ********************

SELECT
    100.00 * sum(case
        when p.type like 'PROMO%'
            then l.extendedprice * (1 - l.discount)
        else 0
    end) / sum(l.extendedprice * (1 - l.discount)) as promo_revenue
FROM
    lineitem l,
    part p
WHERE
    l.partkey = p.partkey
    AND l.shipdate >= date '1995-09-01'
    AND l.shipdate < date '1995-09-01' + interval '1' month;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Query 19

-- CELL ********************

SELECT
    sum(l.extendedprice* (1 - l.discount)) as revenue
FROM
    lineitem l,
    part p
WHERE
    (
        p.partkey = l.partkey
        AND p.brand = 'Brand#12'
        AND p.container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l.quantity >= 1 AND l.quantity <= 1 + 10
        AND p.size between 1 AND 5
        AND l.shipmode in ('AIR', 'AIR REG')
        AND l.shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.partkey = l.partkey
        AND p.brand = 'Brand#23'
        AND p.container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l.quantity >= 10 AND l.quantity <= 10 + 10
        AND p.size between 1 AND 10
        AND l.shipmode in ('AIR', 'AIR REG')
        AND l.shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.partkey = l.partkey
        AND p.brand = 'Brand#34'
        AND p.container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l.quantity >= 20 AND l.quantity <= 20 + 10
        AND p.size between 1 AND 15
        AND l.shipmode in ('AIR', 'AIR REG')
        AND l.shipinstruct = 'DELIVER IN PERSON'
    );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT
    sum(l.extendedprice* (1 - l.discount)) as revenue
FROM
    lineitem l
        INNER JOIN part p ON p.partkey = l.partkey
WHERE
    (
        p.brand = 'Brand#12'
        AND p.container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l.quantity >= 1 AND l.quantity <= 1 + 10
        AND p.size between 1 AND 5
        AND l.shipmode in ('AIR', 'AIR REG')
        AND l.shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.brand = 'Brand#23'
        AND p.container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l.quantity >= 10 AND l.quantity <= 10 + 10
        AND p.size between 1 AND 10
        AND l.shipmode in ('AIR', 'AIR REG')
        AND l.shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.brand = 'Brand#34'
        AND p.container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l.quantity >= 20 AND l.quantity <= 20 + 10
        AND p.size between 1 AND 15
        AND l.shipmode in ('AIR', 'AIR REG')
        AND l.shipinstruct = 'DELIVER IN PERSON'
    );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
