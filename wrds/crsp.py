import pandas as pd
import numpy as np
from .util import *
import statsmodels as sm

# CRSP Convenience Functions

from numpy import log, exp
from numpy.ma import masked_array

def compound_ret(ret):
    return exp(sum(log(1+ret)))-1

# useful for computing ivol
def rmse(data, yvar, xvars):
    raise NotImplementedError('No dsf support yet.')
    Y = data[yvar]
    X = data[xvars]
    X['intercept'] = 1.
    result = sm.OLS(Y, X).fit()
    return pd.Series({'ivol':sqrt(result.mse_resid), 'n':result.nobs})

def MOM(ret, n=11):
    grp = ret.groupby(level='permno')
    return pd.Series(grp.apply(pd.rolling_apply,n,
                     lambda x: np.prod(1 + x) - 1),
                     name='mom_{0}'.format(n+1))

def VOL(ret, days=60):
    raise NotImplementedError('No dsf support yet.')

def BAB(ret):
    raise NotImplementedError('No dsf support yet.')

def CEI(ret, me, n=60):
    ret = ret.fillna(0)
    BR = exp(DIF(log(me),n,group='permno'))
    LR = LAG(MOM(ret,n),group='permno')

    return pd.Series(log(BR) - log(LR), name='cei')


