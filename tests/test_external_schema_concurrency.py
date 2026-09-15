import re
from otras_fuentes.storage import OpportunityStore, SQLITE_SCHEMA


class Cursor:
    def __init__(self,missing=()):
        self.calls=[]
        self.existing={name:True for name in re.findall(r'CREATE (?:TABLE|INDEX) IF NOT EXISTS (\w+)',SQLITE_SCHEMA) if name not in missing}
    def execute(self,sql,params=()):self.calls.append(sql)
    def fetchall(self):return list(self.existing.items())
    def close(self):pass


class Connection:
    def __init__(self,cursor):self.c=cursor
    def cursor(self):return self.c
    def commit(self):pass
    def rollback(self):pass


def test_normal_postgres_open_never_requests_ddl_locks():
    cur=Cursor();OpportunityStore(Connection(cur),'postgres').ensure_schema()
    assert len(cur.calls)==1 and cur.calls[0].startswith('SELECT c.relname')


def test_migration_only_creates_missing_objects_and_serializes_migrators():
    cur=Cursor(missing={'idx_external_email_pending'})
    OpportunityStore(Connection(cur),'postgres').ensure_schema()
    assert any('pg_advisory_xact_lock' in sql for sql in cur.calls)
    ddl=[sql for sql in cur.calls if sql.startswith(('CREATE','ALTER'))]
    assert len(ddl)==1 and 'idx_external_email_pending' in ddl[0]
