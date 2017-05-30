create table Part(
	partkey INTEGER PRIMARY KEY, 
  name STRING, 
  mfgr STRING, 
  brand STRING, 
  type STRING, 
  size INTEGER, 
  container STRING, 
  retailprice REAL, 
  comment STRING
);

create table Supplier(
	suppKey INTEGER PRIMARY KEY, 
  name STRING, 
  address STRING, 
  nationkey INTEGER, 
  phone STRING, 
  acctbal REAL, 
  comment STRING
);

create table PartSupp(
	partKey INTEGER, 
  suppKey INTEGER, 
  availqty INTEGER, 
  supplycost REAL, 
  comment STRING
);

create table Customer(
  custKey INTEGER PRIMARY KEY, 
  name STRING, 
  address STRING, 
  nationkey INTEGER, 
  phone STRING, 
  acctbal REAL, 
  mktsegment STRING, 
  comment STRING
);

create table Nation(
	nationkey INTEGER PRIMARY KEY, 
  name STRING, 
  regionkey INTEGER, 
  comment STRING
);

create table Region(
	regionkey INTEGER PRIMARY KEY, 
  name STRING, 
  comment STRING
);

create table LineItem(
	orderKey INTEGER, 
  partKey INTEGER, 
  suppKey INTEGER, 
  lineNumber INTEGER, 
  quantity INTEGER, 
  extendedPrice REAL, 
  discount REAL, 
  tax REAL, 
  returnFlag STRING, 
  lineStatus STRING, 
  shipDate DATE, 
  commitDate DATE, 
  receiptDate DATE, 
  shipInstruct STRING, 
  shipMode STRING, 
  comment STRING
);

create table Orders(
	orderKey INTEGER PRIMARY KEY, 
  custKey INTEGER, 
  orderStatus STRING, 
  totalPrice REAL, 
  orderDate DATE, 
  orderPriority STRING, 
  clerk STRING, 
  shipPriority STRING, 
  comment STRING
);

create index PartPKI on Part(partkey);
create index SupplierPK on Supplier(suppkey);
create index PartSuppPKI on PartSupp(partkey, suppkey);
create index CustomerPKI on Customer(custkey);
create index NationPKI on Nation(nationkey);
create index RegionPKI on Region(regionkey);
create index LineItemPKI on LineItem(orderkey, lineNumber);
create index OrdersPKI on Orders(orderkey);

create index SupplierNations on Supplier(nationkey);
create index CustomerNations on Customer(nationkey);
create index LineItemParts on LineItem(partkey);
create index LineItemSuppliers on LineItem(suppkey);
create index OrderCustomers on Orders(custKey);
create index PartSuppSupp on PartSupp(suppkey);

-- create index OrderDate on Orders(orderDate);