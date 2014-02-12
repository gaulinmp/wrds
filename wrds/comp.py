import pandas as pd
from numpy import log
from .util import *

# COMPUSTAT Convenience Functions

def NSI(CSHO, AJEX):
    SI = log(CSHO*AJEX)
    return pd.Series(DIF(SI), name='nsi')

def TAC(ACT, CHE, LCT, DLC, TXP, DP, AT):
    ACC = ((DIF(ACT) - DIF(CHE)) - (DIF(LCT) - DIF(DLC) - DIF(TXP)) - DP)
    AT_AVG = AT.groupby(level='gvkey').apply(lambda x: (x+x.shift(1))/2)
    return pd.Series(ACC/AT_AVG, name='tac')

def NOA(AT, CHE, DLC, DLTT, MIB, PSTK, CEQ):
    OA = ( (AT - CHE) - (AT - DLC - DLTT - MIB - PSTK - CEQ) )
    return pd.Series(OA/LAG(AT), name='noa')

def GPA(GP, AT):
    return pd.Series(GP/AT, name='gpa')

def AG(AT):
    return pd.Series(AT/LAG(AT) - 1, name='ag')

def IA(PPEGT, INVT, AT):
    return pd.Series( ( DIF(PPEGT) + DIF(INVT) ) / LAG(AT) , name='ia')

def ROA(IB, AT):
    return pd.Series(IB/AT, name='roa')

def OSCORE(AT, DLTT, DLC, LT, LCT, ACT, NI, SEQ, WCAP, EBITDA):
    INTWO = (LAG(NI) < 0) & (LAG(NI,2) < 0)
    OENEG = (SEQ < 0)
    # Should divide AT by GDP Deflator
    OS = -1.32 - 0.407*log(AT) \
          + 6.03*COALESCE(DLTT+DLC,[LT])/AT - 1.43*WCAP/AT \
          + 0.076*LCT/ACT - 2.37*NI/AT - 1.83*EBITDA/COALESCE(DLTT+DLC,[LT]) \
          + 0.285*INTWO - 1.72*OENEG - 0.521*DIF(NI)/(abs(NI)+abs(LAG(NI)))
    return pd.Series(OS, name='oscore')

def ROAQ(IBQ, ATQ):
    return pd.Series(IBQ/LAG(ATQ), name='roaq')
