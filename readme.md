# wrds: Python Interface for querying WRDS datasets (CRSP, COMPUSTAT)

Author: Eddy Hu

Institution: Rice University

Contact: eddyhu at the gmails

## What is it
**wrds** is a Python package for interfacing with [`WRDS`](http://wrds.wharton.upenn.edu) data (currently local dbs only). It simplifies a lot of the standard data munging activities (querying, merging, cleaning) for `CRSP` and `COMPUSTAT` data, and allows the user to quickly access key variables of interest to conduct empirical analyses.

## Features
	- CRSP Monthly, COMPUSTAT Annual and Quarterly data
		- Aligns accounting fundamentals with market prices
	- Computes several anomaly characteristics
		- net stock issuance, total accruals, net operating assets, asset growth, investment to assets, return on assets, Ohlson's O-Score
	- Uses PostgreSQL (via SQLAlchemy) to access/store data
		- Easily configurable for MySQL
	- Returns query results as pandas.DataFrames or record tuples

## TO-DO:
	- Implement DSF support for MOM, IVOL, and BAB anomalies