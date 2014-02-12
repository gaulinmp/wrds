Author: Eddy Hu
Institution: Rice University
Contact: eddyhu@gmail.com

Python Interface for querying WRDS datasets (CRSP, COMPUSTAT)
and for computing several "anomalies" documented in the finance
and accounting literatures.

Uses PostgreSQL (via SQLAlchemy) and pandas to manipulate data.

Included are convenience functions for computing anomaly characteristics.

TO-DO:
	* Implement DSF support for IVOL and BAB anomalies