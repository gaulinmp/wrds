from sqlalchemy.sql import Select
from sqlalchemy.ext.compiler import compiles


class CreateTableAs(Select):
    """Create a CREATE TABLE AS SELECT ... statement."""

    def __init__(self, columns, new_table_name='', is_temporary=False,
            on_commit_delete_rows=False, on_commit_drop=False, *arg, **kw):
        """By default the table sticks around after the transaction. You can
        change this behavior using the `on_commit_delete_rows` or
        `on_commit_drop` arguments.

        :param on_commit_delete_rows: All rows in the temporary table will be
        deleted at the end of each transaction block.
        :param on_commit_drop: The temporary table will be dropped at the end
        of the transaction block.
        """
        super(CreateTableAs, self).__init__(columns, *arg, **kw)

        self.is_temporary = is_temporary
        self.new_table_name = new_table_name
        self.on_commit_delete_rows = on_commit_delete_rows
        self.on_commit_drop = on_commit_drop
        self.select = None


@compiles(CreateTableAs)
def s_create_table_as(element, compiler, **kw):
    """Compile the statement."""
    text = compiler.visit_select(element)
    # store a copy of the select statement by itself
    element.select = text
    if not element.new_table_name:
        return text

    spec = ['CREATE', 'TABLE', element.new_table_name, 'AS SELECT']

    if element.is_temporary:
        spec.insert(1, 'TEMPORARY')

    on_commit = None

    if element.on_commit_delete_rows:
        on_commit = 'ON COMMIT DELETE ROWS'
    elif element.on_commit_drop:
        on_commit = 'ON COMMIT DROP'

    if on_commit:
        spec.insert(len(spec)-1, on_commit)

    text = text.replace('SELECT', ' '.join(spec))
    return text
