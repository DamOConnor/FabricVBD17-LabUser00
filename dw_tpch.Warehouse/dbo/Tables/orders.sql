CREATE TABLE [dbo].[orders] (

	[orderkey] int NULL, 
	[custkey] int NULL, 
	[orderstatus] char(1) NULL, 
	[totalprice] decimal(10,2) NULL, 
	[orderdate] date NULL, 
	[orderpriority] varchar(15) NULL, 
	[clerk] varchar(15) NULL, 
	[shippriority] int NULL, 
	[comment] varchar(79) NULL
);

