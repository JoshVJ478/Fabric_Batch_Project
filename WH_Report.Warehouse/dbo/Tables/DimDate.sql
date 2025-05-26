CREATE TABLE [dbo].[DimDate] (

	[Date] date NULL, 
	[DateKey] int NULL, 
	[FullDate] varchar(8000) NULL, 
	[Month] varchar(8000) NULL, 
	[MonthName] varchar(8000) NULL, 
	[Year] varchar(8000) NULL, 
	[DayOfWeek] int NULL, 
	[DayOfWeekName] varchar(8000) NULL, 
	[DayOfMonth] varchar(8000) NULL, 
	[DayOfYear] varchar(8000) NULL, 
	[Quarter] varchar(8000) NULL, 
	[WeekOfYear] int NULL
);