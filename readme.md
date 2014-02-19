# wrds: Python Interface for querying WRDS datasets (CRSP, COMPUSTAT)

Author: Eddy (Edwin) Hu

Institution: [Rice University](http://business.rice.edu)

Contact: eddyhu at the gmails

## What is it
**wrds** is a Python package for interfacing with **WRDS**(http://wrds.wharton.upenn.edu) data (currently local dbs only). It simplifies a lot of the standard data munging activities (querying, merging, cleaning) for **CRSP** and **COMPUSTAT** data, and allows the user to quickly access key variables of interest to conduct empirical analyses.

## Features
	- CRSP Monthly, COMPUSTAT Annual and Quarterly data
		- Aligns accounting fundamentals with market prices
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
		- MOM
		- IVOL
		- BAB
		- CHSDP
	- MSF
		- adjust returns for delistings
		- compute annual market equity based vwret
