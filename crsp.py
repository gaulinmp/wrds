import pandas as pd
import numpy as np
import statsmodels as sm

from numpy import log, exp

def compound_ret(ret):
    return exp(sum(log(1+ret)))-1

def MOM(ret, months=11):
    grp = ret.groupby(level='permno')
    return pd.Series(grp.apply(pd.rolling_apply,months,
                     lambda x: np.prod(1 + x) - 1),
                     name='mom_2_{0}'.format(months+1))

def VOL(ret, days=60):
    raise NotImplementedError('No dsf support yet.')

# useful for computing ivol
def rmse(data, yvar, xvars):
    raise NotImplementedError('No dsf support yet.')
    Y = data[yvar]
    X = data[xvars]
    X['intercept'] = 1.
    result = sm.OLS(Y, X).fit()
    return pd.Series({'ivol':sqrt(result.mse_resid), 'n':result.nobs})
