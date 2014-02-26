from __future__ import division
import pandas as pd
import logging
from numpy import log, exp
from time import clock

TIME_FUNCTIONS = True

def timeit(f):
    if not TIME_FUNCTIONS:
        return f
    else:
        def timed(*args, **kw):
            ts = clock()
            result = f(*args, **kw)
            te = clock()

            logging.debug('\t:%r.%r took: %2.4f sec'\
                ,f.__module__, f.__name__, te-ts)
            return result

        return timed

# pandas convenience functions

def LAG(x, n=1, group='gvkey'):
    grp = x.groupby(level=group)
    return grp.apply(lambda x: x.shift(n))

def DIF(x, n=1, group='gvkey'):
    grp = x.groupby(level=group)
    return grp.apply(lambda x: x - x.shift(n))

def COALESCE(x, varlist):
    if not varlist:
        return x
    nix = x.isnull()
    x[nix] = varlist.pop(0)[nix]
    return COALESCE(x, varlist)

@timeit
def to_df(res, firm_id='permno', date='date', delay=None):

    df = pd.DataFrame(res.fetchall(),columns=res.keys())
    df[date] = pd.to_datetime(df[date])

    if delay:
        df.set_index([date],inplace=True)
        df = df.tshift(delay,'M')
        df.reset_index(inplace=True)

    if date:
        df.set_index(date,inplace=True)
    if firm_id:
        df.set_index(firm_id,inplace=True,append=True)
    df.sort_index(inplace=True)

    return df
