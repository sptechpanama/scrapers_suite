"""Maintenance command: preserve history, back up first, never create email events."""
from __future__ import annotations
import argparse,json,sys,sqlite3
from datetime import datetime,timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
from otras_fuentes.storage import OpportunityStore,default_sqlite_path,postgres_dsn
from otras_fuentes.enrichment import DetailEnricher
from otras_fuentes.reclassify import reclassify_store


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--require-postgres',action='store_true')
    parser.add_argument('--detail-budget',type=int,default=0)
    args=parser.parse_args()
    if args.require_postgres and not postgres_dsn(): raise RuntimeError('SUPABASE_DB_URL no configurado')
    path=default_sqlite_path()
    if not path.exists(): raise RuntimeError('No existe la base local; no se creará una base vacía')
    local=OpportunityStore.sqlite(path)
    remote=OpportunityStore.postgres(postgres_dsn()) if postgres_dsn() else None
    stores=[('local',local)]+([('supabase',remote)] if remote else [])
    stamp=datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    backup=path.parent/'backups'/('quality_'+stamp)
    if args.apply:
        backup.mkdir(parents=True,exist_ok=False)
        with sqlite3.connect(backup/'before.db') as copy: local.connection.backup(copy)
        if remote:
            for table in ('external_opportunities','external_opportunity_documents'):
                c=remote.connection.cursor();c.execute('SELECT * FROM '+table)
                names=[x[0] for x in c.description]
                with (backup/(table+'.jsonl')).open('w',encoding='utf-8') as out:
                    for row in c:out.write(json.dumps(dict(zip(names,row)),ensure_ascii=False,default=str)+'\n')
                c.close()
        print('Backup:',backup,flush=True)
    enricher=DetailEnricher(path.parent/'details.db',budget=max(0,min(args.detail_budget,200)))
    try:
        reports={}
        for name,store in stores:
            reports[name]=reclassify_store(store,enricher=enricher,apply=args.apply)
            print(name,json.dumps(reports[name],ensure_ascii=True),flush=True)
        if args.apply:(backup/'result.json').write_text(json.dumps(reports,ensure_ascii=False,indent=2),encoding='utf-8')
    finally:
        enricher.close()
        for _,store in stores:store.close()
    return 0


if __name__=='__main__': raise SystemExit(main())
