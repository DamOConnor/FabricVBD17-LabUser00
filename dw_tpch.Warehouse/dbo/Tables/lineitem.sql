CREATE TABLE [dbo].[lineitem] (

	[orderkey] int NULL, 
	[partkey] int NULL, 
	[suppkey] int NULL, 
	[linenumber] int NULL, 
	[quantity] decimal(10,2) NULL, 
	[extendedprice] decimal(10,2) NULL, 
	[discount] decimal(10,2) NULL, 
	[tax] decimal(10,2) NULL, 
	[returnflag] char(1) NULL, 
	[linestatus] char(1) NULL, 
	[shipdate] date NULL, 
	[commitdate] date NULL, 
	[receiptdate] date NULL, 
	[shipinstruct] varchar(25) NULL, 
	[shipmode] varchar(10) NULL, 
	[comment] varchar(44) NULL
);

