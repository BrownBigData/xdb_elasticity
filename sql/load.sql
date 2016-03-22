load data local 
INFILE '/Users/cbinnig/Temp/s10/lineitem.tbl' 
INTO TABLE lineitem
FIELDS TERMINATED BY '|';

load data local 
INFILE '/Users/cbinnig/Temp/s10/orders.tbl' 
INTO TABLE orders
FIELDS TERMINATED BY '|';

load data local 
INFILE '/Users/cbinnig/Temp/s10/customer.tbl' 
INTO TABLE customer
FIELDS TERMINATED BY '|';

load data local 
INFILE '/Users/cbinnig/Temp/s10/supplier.tbl' 
INTO TABLE supplier
FIELDS TERMINATED BY '|';

load data local 
INFILE '/Users/cbinnig/Temp/s10/partsupp.tbl' 
INTO TABLE partsupp
FIELDS TERMINATED BY '|';

load data local 
INFILE '/Users/cbinnig/Temp/s10/part.tbl' 
INTO TABLE part
FIELDS TERMINATED BY '|';

load data local 
INFILE '/Users/cbinnig/Temp/s10/nation.tbl' 
INTO TABLE nation
FIELDS TERMINATED BY '|';

load data local 
INFILE '/Users/cbinnig/Temp/s10/region.tbl' 
INTO TABLE region
FIELDS TERMINATED BY '|';

OPTIMIZE TABLE lineitem, customer, nation, region, orders, part, partsupp, supplier;

SHOW TABLE STATUS;