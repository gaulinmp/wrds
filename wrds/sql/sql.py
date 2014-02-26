import sqlalchemy as sa
from sqlalchemy.sql import expression
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import DateTime

class utcnow(expression.FunctionElement):
    type = DateTime()

@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"

@compiles(utcnow, 'mssql')
def ms_utcnow(element, compiler, **kw):
    return "GETUTCDATE()"

class fiscal_year(expression.FunctionElement):
    type = DateTime()
    name = 'fiscal_year'

@compiles(fiscal_year, 'postgresql')
def pg_fiscal_year(element, compiler, **kw):
    """Registers fiscal_year postgres function call
    """
    d, m, last = list(element.clauses)
    return "fiscal_year({0}, {1}, {2})".format(
            compiler.process(d),
            compiler.process(m),
            compiler.process(last)
        )
