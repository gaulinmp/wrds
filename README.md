# wrds: Python Interface for querying WRDS datasets (CRSP, COMPUSTAT)

Author: Eddy (Edwin) Hu

Institution: [Rice University](http://business.rice.edu)

Contact: eddyhu at the gmails

## What is it
**wrds** is a Python package for interfacing with [**WRDS**](http://wrds.wharton.upenn.edu) data (currently local dbs only). It simplifies a lot of the standard data munging activities (querying, merging, cleaning) for **CRSP** and **COMPUSTAT** data, and allows the user to quickly access key variables of interest to conduct empirical analyses.

## Querying simplified ##
Suppose you want to pull stock returns and market caps in order to do annual portfolio sorts on size. This is conceptually easy, but involves quite a bit of upfront data munging. **wrds** does all the data munging and makes accessing the relevant data as easy as:

	import sqlalchemy as sa
	import wrds

```python
	msf_query = wrds.MSFQuery().query.alias('msf_query')
	q = sa.select([msf_query.c.permno,
               msf_query.c.date,
               msf_query.c.ret_adj,
               msf_query.c.me,
               msf_query.c.vweight])
```
Which creates a SQL statement:

```sql
	SELECT
		msf_query.permno,
		msf_query.date,
		msf_query.ret_adj,
		msf_query.me,
		msf_query.vweight
	FROM ...
```

the `...` is where the magic happens. **wrds** makes sure that **CRSP** **permnos** are correctly lined up with the company names, computes the market cap, adjust returns for delistings, and computes annual buy-and-hold portfolio weights.

Pulling book equity from **COMPUSTAT** is just as simple:

```python
	funda_query = wrds.FUNDAQuery().query.alias('funda_query')
	q = sa.select([funda_query.c.lpermno,
               funda_query.c.gvkey,
               funda_query.c.datadate,
               funda_query.c.be
               ])
```
Which creates a SQL statement:

```sql
	SELECT
		funda_query.lpermno,
		funda_query.gvkey,
		funda_query.datadate,
		funda_query.be
	FROM ...
```

**wrds** merges in **CRSP** **permno**s so that the **permno-gvkey** link is unique, and computes book equity following [Fama and French (1993)](http://www.sciencedirect.com/science/article/pii/0304405X93900235), and [Davis, Fama, and French (2000)](http://onlinelibrary.wiley.com/doi/10.1111/0022-1082.00209/abstract).

## Features
- CRSP Monthly, COMPUSTAT Annual and Quarterly data
	- Aligns accounting fundamentals with market prices
	- Computes delisting returns [Shumway (1997)](http://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.1997.tb03818.x/abstract)
	- Computes annual buy-and-hold market equity for use as portfolio value weights
	- Convenience functions for accessing Fama-French Factors
- Computes several anomaly characteristics
	- net stock issuance
	- composie equity issuance
	- total accruals
	- gross profitability
	- net operating assets
	- asset growth
	- investment to assets
	- return on assets
	- Ohlson's O-Score
- Uses PostgreSQL (via SQLAlchemy) to access/store data
	- Easily configurable for MySQL
- Returns query results as pandas.DataFrames or record tuples

## TO-DO:
- Implement DSF support
	- basic support added 2014/03/04
	- MOM
	- IVOL
	- BAB
	- CHSDP
- MSF
	- aggregate market equity by permco
