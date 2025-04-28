CREATE TABLE [dbo].[DimProduct] (

	[ProductKey] int NULL, 
	[ProductID_OLTP] int NULL, 
	[ProductName] varchar(8000) NULL, 
	[ProductCategory] varchar(8000) NULL, 
	[ProductBrand] varchar(8000) NULL
);