import sqlalchemy as sa
import pandas as pd
import numpy as np
import datetime
import itertools
import sql

import logging, pdb
from sqlalchemy.sql import func
from sqlalchemy.exc import ResourceClosedError
from pandas.tseries.offsets import *
from .createtable import CreateTableAs
from .util import timeit

class WRDSQuery(object):
    """Generative interface for querying WRDS tables.
    """

    def __init__(self, engine=None, limit=None):
        """Initialization logs in to DB, sets up tables."""
        if not engine:
            self.engine = sa.create_engine('postgresql://eddyhu:asdf@localhost:5432/wrds')
        else:
            self.engine = engine
        self.metadata = sa.MetaData(self.engine)
        self.metadata.reflect()
        self.tables = self.metadata.tables
        self.query = None

        # options
        self.options = {}
        self.options['limit'] = limit

    @timeit
    def read_frame(self, **kwargs):
        """Reads query results into pandas.DataFrame.

           Parameters
           ----------
           chunksize: rows to read each iteration (default: 100,000)
           as_recarray: return as records (default: False)

        """

        # copy options
        self.options.update(kwargs)

        # modify/set default options
        chunksize = kwargs.pop('chunksize', 100000)
        as_recarray = kwargs.pop('as_recarray', False)

        res = self.query.execute()
        rows = self._yield_data(res,chunksize,as_recarray,**kwargs)

        # note: using original options
        if not self.options.get('chunksize'):
            # unpack generator
            if self.options.get('as_recarray'):
                rows = list(itertools.chain.from_iterable(rows))
            else:
                rows = pd.concat(rows)

        # maybe_parse, maybe_index

        return rows

    @timeit
    def create_table(self, new_table_name, drop=True):
        if self.engine.has_table(new_table_name):
            new_table = self.tables.get(new_table_name)
            if drop:
                new_table.drop(self.engine, checkfirst=True)
                logging.debug('Old {0} table dropped.'.format(new_table_name))

        create = self.query.alias('create')
        query = CreateTableAs(create.c, new_table_name)
        logging.debug(query)
        # Execute statement and commit changes to DB.
        query.execution_options(autocommit=True).execute()
        logging.debug('Table {0} created.'.format(new_table_name))

    def _yield_data(self, res, chunksize, as_recarray, **kwargs):

        try:
            while res.returns_rows:
                rows = res.fetchmany(chunksize)
                if rows:
                    if as_recarray:
                        yield rows
                    else:
                        yield self._to_df(rows, res, **kwargs)
        except ResourceClosedError:
            logging.debug('ResultProxy empty')
            pass


    def _to_df(self, rows, res, **kwargs):
        """Makes a DataFrame from records with columns.

        Should be subclassed to do things like delay, duplicates handling,
        setting the index, etc.
        """
        return pd.DataFrame.from_records(rows,\
                    columns=res.keys(), coerce_float=True)

'''
 .o88b.  .d88b.  .88b  d88. d8888b.         .d8b.  d8b   db d8b   db
d8P  Y8 .8P  Y8. 88'YbdP`88 88  `8D        d8' `8b 888o  88 888o  88
8P      88    88 88  88  88 88oodD'        88ooo88 88V8o 88 88V8o 88
8b      88    88 88  88  88 88~~~   C8888D 88~~~88 88 V8o88 88 V8o88
Y8b  d8 `8b  d8' 88  88  88 88             88   88 88  V888 88  V888
 `Y88P'  `Y88P'  YP  YP  YP 88             YP   YP VP   V8P VP   V8P
 '''
class FUNDAQuery(WRDSQuery):
    """Generative interface for querying COMPUSTAT.FUNDA."""

    def __init__(self, engine=None,
                 be=True, me_comp=False, nsi=False,
                 tac=False, noa=False, gp=False, ag=False, ia=False,
                 roa=False, oscore=False, permno=True,
                 limit=None, all_vars=None, **kwargs):
        """Generatively create SQL query to FUNDA.

            Parameters
            ----------
            be: boolean, default True
                Book Equity = SHE + DEFTX - PS
            me_comp: boolean, default False
                Market Equity = CSHO * PRCC_F
            nsi: boolean, default False
                Net Stock Issuance = LOG( CSHO * AJEX / (LAG(CSHO)*LAG(AJEX)) )
            tac: boolean, default False
                Total Accurals = (( DIF(ACT) - DIF(CHE) ) - ( DIF(LCT) - DIF(DLC) - DIF(TXP) ) - DP) / (AT + LAG(AT))/2
            noa: boolean, default False
                Net Operating Assets = ( (AT - CHE) - (AT - DLC - DLTT - MIB - PSTK - CEQ) ) / LAG(AT)
            gp: boolean, default False
                Gross Profitability = GP/AT
            ag: boolean, default False
                Asset Growth = AT/LAG(AT) - 1
            ia: boolean, default False
                Investment to Assets = ( DIF(PPEGT) + DIF(INVT) ) / LAG(AT)
            roa: boolean, default False
                Return on Assets = IB/AT
            oscore: boolean, default False
                Ohlson's O-Score = TO-DO
            permno: boolean, default True
                LPERMNO and LPERMCO from CCMXPF_LINKTABLE

        """
        super(FUNDAQuery, self).__init__(engine, limit)
        logging.info("---- Creating a COMPUSTAT.FUNDA query session. ----")

        funda = self.tables['funda']
        ccmxpf_linktable = self.tables['ccmxpf_linktable']
        funda_vars = [funda.c.gvkey, funda.c.datadate]

        if be:
            # BE = SHE + DEFTX - PS;
            funda_vars += [(# Shareholder's Equity
                          sa.func.coalesce(funda.c.seq,
                                           funda.c.ceq + sa.func.coalesce(funda.c.pstk,0),
                                           funda.c.at - funda.c.lt
                                           )
                          +
                          # Deferred Taxes
                          sa.func.coalesce(funda.c.txditc,funda.c.txdb,0)
                          -
                          # Preferred Stock
                          sa.func.coalesce(funda.c.pstkrv,funda.c.pstkl,funda.c.pstk,0)
                          ).label('be')]
        if me_comp:
            # ME_COMP = CSHO * PRCC_F;
            funda_vars += [(funda.c.csho*funda.c.prcc_f).label('me_comp')]
        if nsi:
            # NSI = LOG( CSHO * AJEX / (LAG(CSHO)*LAG(AJEX)) );
            funda_vars += [funda.c[v.lower()] for v in
                             ('CSHO','AJEX')]
        if tac:
            # TAC = (( DIF(ACT) - DIF(CHE) ) - ( DIF(LCT) - DIF(DLC) - DIF(TXP) ) - DP) / (AT + LAG(AT))/2;
            funda_vars += [funda.c[v.lower()] for v in
                             ('ACT','CHE','LCT','DLC','TXP','DP')]
        if noa:
            # NOA = ( (AT - CHE) - (AT - DLC - DLTT - MIB - PSTK - CEQ) ) / LAG(AT);
            funda_vars += [funda.c[v.lower()] for v in
                             ('AT','CHE','DLC','DLTT','MIB','PSTK','CEQ')]
        if gp:
            # GP = GP/AT;
            funda_vars += [funda.c[v.lower()] for v in ('GP','AT')]
        if ag:
            # AG = AT/LAG(AT) - 1;
            funda_vars += [funda.c.at]
        if ia:
            # IA = ( DIF(PPEGT) + DIF(INVT) ) / LAG(AT);
            funda_vars += [funda.c[v.lower()] for v in ('PPEGT','INVT','AT')]
        if roa:
            # ROA = IB/AT;
            funda_vars += [funda.c[v.lower()] for v in ('IB','AT')]
        if oscore:
            # OSCORE;
            funda_vars += [funda.c[v.lower()] for v in
                             ('AT','DLTT','DLC','LT','LCT',
                              'NI','SEQ','WCAP','EBITDA')]
        if all_vars:
            funda_vars += funda.c

        # Get the unique set of columns/variables
        funda_vars = list(set(funda_vars))
        # Create the 'raw' select statement
        query = sa.select(funda_vars, limit=limit).\
                    where(funda.c.indfmt=='INDL').\
                    where(funda.c.datafmt=='STD').\
                    where(funda.c.popsrc=='D').\
                    where(funda.c.consol=='C')

        if permno:
            # Merge in PERMNO and PERMCO
            a = query.alias('a');b = ccmxpf_linktable.alias('b')

            # Add in PERMNO and PERMCO from CCMXPF_LINKTABLE
            query = sa.select([a, b.c.lpermno, b.c.lpermco],
                              limit=limit).\
                        where(b.c.linktype.startswith('L')).\
                        where(b.c.linkprim.in_(['P','C'])).\
                        where(b.c.usedflag==1).\
                        where((b.c.linkdt <= a.c.datadate) |
                              (b.c.linkdt == None)).\
                        where((a.c.datadate <= b.c.linkenddt) |
                              (b.c.linkenddt == None)).\
                        where(a.c.gvkey == b.c.gvkey)

        # Save the query and return ResultProxy
        logging.debug(query)
        self.query = query

    def _to_df(self, rows, res, delay=6, **kwargs):
        """Reads query results into pandas.DataFrame.

           Parameters
           ----------
           delay: how many months until accounting data becomes public

        """
        funda_df = pd.DataFrame.from_records(rows,\
                    columns=res.keys(), coerce_float=True)
        funda_df['datadate'] = pd.to_datetime(funda_df['datadate'])

        funda_df['date'] = funda_df['datadate'].copy()
        if delay:
            funda_df.set_index(['date'],inplace=True)
            funda_df = funda_df.tshift(delay,'M')
            funda_df.reset_index(inplace=True)

        funda_df.set_index(['gvkey','date'],inplace=True)
        return funda_df

'''
 .o88b.  .d88b.  .88b  d88. d8888b.         .d88b.
d8P  Y8 .8P  Y8. 88'YbdP`88 88  `8D        .8P  Y8.
8P      88    88 88  88  88 88oodD'        88    88
8b      88    88 88  88  88 88~~~   C8888D 88    88
Y8b  d8 `8b  d8' 88  88  88 88             `8P  d8'
 `Y88P'  `Y88P'  YP  YP  YP 88              `Y88'Y8
 '''
class FUNDQQuery(WRDSQuery):
    """Generative interface for querying COMPUSTAT.FUNDQ."""

    def __init__(self, engine=None, roa=True, chsdp=False,
                 permno=True,  limit=None, all_vars=None, **kwargs):
        """Generatively create SQL query to FUNDA.

            Parameters
            ----------
            roa: boolean, default False
                Return on Assets =  IBQ / ATQ
            chsdp: boolean, default False
                Campbell et. al Default Prob = TO-DO

        """
        super(FUNDQQuery, self).__init__(engine, limit)
        logging.info("---- Creating a COMPUSTAT.FUNDQ query session. ----")

        fundq = self.tables['fundq']
        ccmxpf_linktable = self.tables['ccmxpf_linktable']
        fundq_vars = [fundq.c.gvkey, fundq.c.datadate, fundq.c.rdq]

        if roa:
            # ROA = IBQ / ATQ;
            fundq_vars += [fundq.c[v.lower()] for v in ('IBQ','ATQ')]

        if chsdp:
            # CHSDP : NIQ LTQ CHEQ PSTKQ TXDITCQ SEQQ CEQQ TXDBQ;
            fundq_vars += [fundq.c[v.lower()] for v in
                             ('NIQ','LTQ','CHEQ','PSTKQ',
                              'TXDITCQ','SEQQ','CEQQ','TXDBQ')]
        if all_vars:
            fundq_vars += fundq.c

        # Get the unique set of columns/variables
        fundq_vars = list(set(fundq_vars))

        # Create the 'raw' select statement
        query = sa.select(fundq_vars, limit=limit).\
                    where(fundq.c.indfmt=='INDL').\
                    where(fundq.c.datafmt=='STD').\
                    where(fundq.c.popsrc=='D').\
                    where(fundq.c.consol=='C')

        if permno:
            # Merge in PERMNO and PERMCO
            a = query.alias('a');b = ccmxpf_linktable.alias('b')

            # Add in PERMNO and PERMCO from CCMXPF_LINKTABLE
            query = sa.select([a, b.c.lpermno, b.c.lpermco],
                              limit=limit).\
                        where(b.c.linktype.startswith('L')).\
                        where(b.c.linkprim.in_(['P','C'])).\
                        where(b.c.usedflag==1).\
                        where((b.c.linkdt <= a.c.datadate) |
                              (b.c.linkdt == None)).\
                        where((a.c.datadate <= b.c.linkenddt) |
                              (b.c.linkenddt == None)).\
                        where(a.c.gvkey == b.c.gvkey)

        # Save the query and return ResultProxy
        logging.debug(query)
        self.query = query

    def _to_df(self, rows, res, delay=3):
        """Reads query results into pandas.DataFrame.

           Parameters
           ----------
           delay: how many months until accounting data becomes public

        """

        def _nodup(data, cols=['gvkey','date']):
            # just dropping them for now
            return data.drop_duplicates(cols=cols)

        fundq_df = pd.DataFrame.from_records(rows,\
                        columns=res.keys(), coerce_float=True)
        fundq_df['datadate'] = pd.to_datetime(fundq_df['datadate'])
        fundq_df['rdq'] = pd.to_datetime(fundq_df['rdq'])

        fundq_df['date'] = fundq_df['datadate'].copy()
        if delay:
            fundq_df.set_index(['date'],inplace=True)
            fundq_df = fundq_df.tshift(delay,'M')
            fundq_df.reset_index(inplace=True)

        date_diff = fundq_df['rdq'] - fundq_df['date']
        # 0 days <= date_diff <= 6 mo
        fundq_df['date'][(date_diff > 0) \
         & (date_diff < pd.tseries.offsets.DateOffset(days=182)) ] \
            = fundq_df['rdq']

        # handle duplicates
        fundq_df = _nodup(fundq_df)

        fundq_df.set_index(['gvkey','date'],inplace=True)
        return fundq_df

'''
 .o88b. d8888b. .d8888. d8888b.        .88b  d88.
d8P  Y8 88  `8D 88'  YP 88  `8D        88'YbdP`88
8P      88oobY' `8bo.   88oodD'        88  88  88
8b      88`8b     `Y8b. 88~~~   C8888D 88  88  88
Y8b  d8 88 `88. db   8D 88             88  88  88
 `Y88P' 88   YD `8888Y' 88             YP  YP  YP
 '''
class MSFQuery(WRDSQuery):

    def __init__(self, engine=None, delist=True, vwm=6, start_date='1925-12-31', end_date='',
                limit=None, all_vars=None, **kwargs):
        """Generatively create SQL query to MSF.

            Parameters
            ----------
            delist: bool, default True
                compute ret_adj, returns adjusted for delistings
            vwm: int, default 6
                month from which annual portfolio value-weights are computed
            cei: bool, default True
                compute composite equity issuance
            start_date: str, default '1925-12-31'
                start of sample, default is beginning of CRSP
            end_date: str, default ''
            limit: int, default None
                limit the number of results in query

        """
        super(MSFQuery, self).__init__(engine, limit)
        logging.info("---- Creating a CRSP.MSF query session. ----")
        msf = self.tables['msf']
        msenames = self.tables['msenames']
        msedelist = self.tables['msedelist']

        msf_vars = [msf.c.permno, msf.c.permco, msf.c.date,
                    msf.c.prc, msf.c.shrout, msf.c.ret, msf.c.retx,
                    (sa.func.abs(msf.c.prc)*msf.c.shrout).label('me')]
        mse_vars = [msenames.c.ticker, msenames.c.ncusip,
                    msenames.c.shrcd, msenames.c.exchcd, msenames.c.hsiccd]

        if all_vars:
            msf_vars += msf.c

        # Get the unique set of columns/variables
        msf_vars = list(set(msf.c+msf_vars))

        query = sa.select(msf_vars+mse_vars, limit=limit).\
            where(msf.c.permno == msenames.c.permno).\
            where(msf.c.date >= msenames.c.namedt).\
            where(msf.c.date <= msenames.c.nameendt)

        if start_date:
            query = query.where(msf.c.date >= start_date)
        if end_date:
            query = query.where(msf.c.date <= end_date)

        if delist:
            a = query.alias('a');b = msedelist.alias('b')
            query = sa.select([a,((1+a.c.ret)*\
                              (1+sa.func.coalesce(b.c.dlret,0))-1).label('ret_adj')],
                               limit=limit)\
            .select_from(sa.join(a, b,
                sa.and_(
                    a.c.permno == b.c.permno,
                    sa.func.extract('year',a.c.date) == sa.func.extract('year',b.c.dlstdt),
                    sa.func.extract('month',a.c.date) == sa.func.extract('month',b.c.dlstdt)
                ),
                isouter=True)
            )

        if vwm:
            a = query.alias('a')
            b = sa.select([sql.fiscal_year(a.c.date,vwm,True).label('fdate'), a]).alias('b')
            c = sa.select([sql.fiscal_year(a.c.date,vwm,True).label('fdate'),
                          a.c.date, a.c.permno, a.c.me])\
                        .where(sa.func.extract('month',a.c.date) == vwm).alias('c')
            query = sa.select([b,
                              (c.c.me*sa.func.exp(
                                  sa.func.sum(sa.func.ln(1+sa.func.coalesce(b.c.ret,0)))\
                                  .over(partition_by=[b.c.permno,
                                        sa.func.extract('year',b.c.fdate)],
                                        order_by=[b.c.fdate]))).label('vweight')
                              ])\
                        .select_from(
                            sa.join(b, c,
                                sa.and_(
                                    b.c.permno == c.c.permno,
                                    sa.func.extract('year',b.c.fdate) == sa.func.extract('year',c.c.fdate)+1
                                ),
                            isouter=True)
                        )

        logging.debug(query)
        self.query = query

    def _to_df(self, rows, res, **kwargs):

        crsp_df = pd.DataFrame.from_records(rows,\
                         columns=res.keys(), coerce_float=True)
        crsp_df['date'] = pd.to_datetime(crsp_df['date']) # not needed?

        crsp_df.set_index(['permno','date'],inplace=True)
        return crsp_df

class DSFQuery(WRDSQuery):

    def __init__(self, engine=None, start_date='1925-12-31', end_date='',
                limit=None, all_vars=None, **kwargs):
        super(DSFQuery, self).__init__(engine, limit)
        logging.info("---- Creating a CRSP.DSF query session. ----")

        raise NotImplementedError('No dsf support yet')

class CCMNamesQuery(WRDSQuery):

    def __init__(self, engine=None, start_date='1925-12-31', end_date='',
                limit=None, all_vars=None, **kwargs):
        super(CCMNamesQuery, self).__init__(engine, limit)
        logging.info("---- Creating a CCM-MSENAMES query session. ----")

        msenames = self.tables['msenames']
        ccmxpf_linktable = self.tables['ccmxpf_linktable']

        id_vars = [msenames.c.permno, msenames.c.permco,
                     ccmxpf_linktable.c.gvkey, msenames.c.comnam]

        query = sa.select(id_vars+\
                        [func.min(msenames.c.namedt).label('sdate'),
                        func.max(msenames.c.nameendt).label('edate')],
                    group_by = id_vars,
                    order_by = id_vars,
                    limit= self.limit).\
            where(ccmxpf_linktable.c.linktype.startswith('L')).\
            where(ccmxpf_linktable.c.linkprim.in_(['P','C'])).\
            where(ccmxpf_linktable.c.usedflag==1).\
            where((ccmxpf_linktable.c.linkdt <= msenames.c.namedt) |
                  (ccmxpf_linktable.c.linkdt == None)).\
            where((msenames.c.nameendt <= ccmxpf_linktable.c.linkenddt) |
                  (ccmxpf_linktable.c.linkenddt == None)).\
            where(msenames.c.permno == ccmxpf_linktable.c.lpermno).\
            where(msenames.c.permco == ccmxpf_linktable.c.lpermco)

        if start_date:
            query = query.having(func.min(msenames.c.namedt) >= start_date)

        if end_date:
            query = query.having(func.max(msenames.c.nameendt) <= end_date)

        logging.debug(query)
        self.query = query


