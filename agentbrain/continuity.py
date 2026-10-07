"""AgentBrain v7: additive, indexed continuity. Never an authorization engine."""
from __future__ import annotations
import argparse, hashlib, json, os, re, sqlite3, sys, time, uuid
from pathlib import Path

VERSION = '7.2.0'
def hook_input(text):
    return str(text).lstrip().startswith(('<hook_prompt', '[AGENTBRAIN-CONTEXT]'))
def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()

def home():
    return Path(os.environ.get('AGENT_BRAIN_HOME', str(Path.home() / 'AgentBrain')))

def runtime():
    return home() / '00-System/Runtime/V7'

def project_identity(cwd):
    canonical=str(Path(cwd).resolve())
    registry=read(home()/'00-System/PROJECT-REGISTRY.json',{}).get('fingerprints',{})
    known=registry.get('path:'+canonical)
    return 'project:'+str(known) if known else digest(canonical)

def atomic(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name('.' + path.name + '.' + uuid.uuid4().hex)
    try:
        with temporary.open('x', encoding='utf-8') as out:
            os.chmod(temporary, 0o600)
            json.dump(value, out, ensure_ascii=False); out.flush(); os.fsync(out.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)

def read(path, default=None):
    try: return json.loads(Path(path).read_text())
    except (OSError, ValueError): return default

def redact(value):
    if isinstance(value, dict):
        return {k: '[REDACTED]' if re.search(r'(?i)^(password|api.?key|access.?token|refresh.?token|authorization|secret)$', k) else redact(v) for k,v in value.items()}
    if isinstance(value, list): return [redact(v) for v in value]
    if isinstance(value, str):
        return re.sub(r'\b(?:sk-[A-Za-z0-9_-]{12,}|gh[pousr]_[A-Za-z0-9]{12,}|Bearer\s+[A-Za-z0-9._-]{12,})', '[REDACTED]', value)
    return value

def identity(event):
    session = str(event.get('session_id') or ('unbound:' + str(event.get('event_id') or digest(event))))
    agent = event.get('agent_id') or event.get('subagent_id')
    owner = session + ('/agent/' + str(agent) if agent else '')
    return owner, session, str(agent) if agent else None

def normalize(event):
    """Normalize actual Codex hook schemas; effect classification never gates use."""
    owner, parent, agent = identity(event)
    name = str(event.get('tool_name') or '')
    args = event.get('tool_input') or {}
    canonical = 'shell' if name in ('Bash','exec_command','functions.exec_command') else name
    command = args.get('command', args.get('cmd')) if isinstance(args,dict) else None
    kind = str(event.get('hook_event_name') or 'observation')
    return {'id': digest([owner,event.get('turn_id'),kind,event.get('tool_use_id'),event.get('event_id'),name,args,event.get('tool_response',event.get('tool_result')),event.get('prompt'),event.get('last_assistant_message')]),
            'owner':owner,'session':parent,'agent':agent,'turn':event.get('turn_id'),
            'kind':kind,'tool':canonical,'native_tool':name,'command':command,
            'cwd':str(Path(event.get('cwd') or os.getcwd()).resolve()),
            'observed':time.time(),'origin':'native_hook','payload':redact(event)}

class Store:
    def __init__(self, path=None, timeout=.1):
        self.path=Path(path or home()/'00-System/workflow.sqlite3');self.timeout=timeout

    def connect(self):
        c=sqlite3.connect(str(self.path),timeout=self.timeout,isolation_level=None)
        c.row_factory=sqlite3.Row; c.execute('PRAGMA busy_timeout='+str(int(self.timeout*1000)))
        return c

    def migrate(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        with self.connect() as c:
            c.execute('PRAGMA journal_mode=WAL')
            c.executescript('''
CREATE TABLE IF NOT EXISTS continuity_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS continuity_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,owner TEXT NOT NULL,session TEXT NOT NULL,turn TEXT,kind TEXT NOT NULL,task TEXT,observed REAL NOT NULL,object TEXT NOT NULL,summary TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS continuity_events_owner ON continuity_events(owner,seq);
CREATE INDEX IF NOT EXISTS continuity_events_task ON continuity_events(task,seq);
CREATE TABLE IF NOT EXISTS continuity_tasks(id TEXT PRIMARY KEY,owner TEXT NOT NULL,project TEXT NOT NULL,parent TEXT,status TEXT NOT NULL,revision INTEGER NOT NULL,data TEXT NOT NULL,updated REAL NOT NULL);
CREATE INDEX IF NOT EXISTS continuity_tasks_owner ON continuity_tasks(owner,status);
CREATE TABLE IF NOT EXISTS continuity_heads(owner TEXT PRIMARY KEY,task TEXT,input_id TEXT,turn TEXT);
CREATE TABLE IF NOT EXISTS continuity_updates(id TEXT PRIMARY KEY,result TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS continuity_task_revisions(task TEXT,revision INTEGER,data TEXT NOT NULL,source TEXT,PRIMARY KEY(task,revision));
CREATE TABLE IF NOT EXISTS continuity_knowledge(id TEXT PRIMARY KEY,scope TEXT NOT NULL,scope_id TEXT NOT NULL,kind TEXT NOT NULL,value TEXT NOT NULL,source TEXT NOT NULL,supersedes TEXT);
CREATE TABLE IF NOT EXISTS continuity_legacy(kind TEXT,id TEXT,owner TEXT,source TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(kind,id));
CREATE TABLE IF NOT EXISTS continuity_sources(path TEXT PRIMARY KEY,inode INTEGER NOT NULL,offset INTEGER NOT NULL,current_turn TEXT);
CREATE TABLE IF NOT EXISTS continuity_native(path TEXT NOT NULL,offset INTEGER NOT NULL,owner TEXT NOT NULL,turn TEXT,kind TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(path,offset));
CREATE TABLE IF NOT EXISTS continuity_native_ids(id TEXT PRIMARY KEY,path TEXT NOT NULL,offset INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS continuity_deliveries(owner TEXT,turn TEXT,candidate_hash TEXT,source TEXT,PRIMARY KEY(owner,turn,candidate_hash));
CREATE INDEX IF NOT EXISTS continuity_native_turn ON continuity_native(owner,turn,offset);
CREATE TABLE IF NOT EXISTS continuity_gaps(id TEXT PRIMARY KEY,owner TEXT,path TEXT,detail TEXT NOT NULL,created REAL NOT NULL);
CREATE VIRTUAL TABLE IF NOT EXISTS continuity_search USING fts5(event_id UNINDEXED,owner UNINDEXED,task UNINDEXED,text);
INSERT OR REPLACE INTO continuity_meta VALUES('schema','7');
''')
            if 'generation' not in {r['name'] for r in c.execute('PRAGMA table_info(continuity_sources)')}:
                c.execute('ALTER TABLE continuity_sources ADD COLUMN generation TEXT')
            if c.execute("SELECT 1 FROM sqlite_master WHERE name='records'").fetchone():
                c.execute("INSERT OR IGNORE INTO continuity_legacy SELECT kind,id,owner,'legacy-record:'||id,data FROM records")
                for r in c.execute("SELECT id,data FROM records WHERE kind='knowledge'").fetchall():
                    d=json.loads(r['data'])
                    if d.get('scope')!='project_shared' or not d.get('project'):continue
                    value=json.dumps({'text':d.get('text'),'legacy_verification':d.get('verification'),'executable':False})
                    c.execute('INSERT OR IGNORE INTO continuity_knowledge VALUES(?,?,?,?,?,?,?)',('legacy:'+r['id'],'project','project:'+Path(d['project']).name,'interpretation',value,'legacy-record:'+r['id'],None))
            for name in ('USER.md','PREFERENCES.md'):
                path=home()/'10-Global'/name
                if path.exists():
                    value=redact({'text':path.read_text(),'legacy_verification':'unreviewed'})
                    c.execute('INSERT OR IGNORE INTO continuity_knowledge VALUES(?,?,?,?,?,?,?)',('legacy-pref:'+name,'preference','user','interpretation',json.dumps(value),str(path),None))
        # Additive: legacy tables, acceptance records and user_version untouched.
        return {'schema':7,'database':str(self.path)}

    def blob(self,value):
        value=redact(value); key=digest(value);p=self.path.parent/'Runtime/V7/objects'/key[:2]/(key+'.json')
        if not p.exists(): atomic(p,value)
        return key

    def object(self,key):
        if not re.fullmatch('[a-f0-9]{64}',str(key)):raise ValueError('invalid object ID')
        p=self.path.parent/'Runtime/V7/objects'/key[:2]/(key+'.json')
        value=json.loads(p.read_text())
        if digest(value)!=key: raise ValueError('evidence_checksum_mismatch:'+key)
        return value

    def gap(self,owner,path,detail):
        with self.connect() as c:
            c.execute('INSERT OR IGNORE INTO continuity_gaps VALUES(?,?,?,?,?)',(digest([owner,str(path),detail]),owner,str(path),detail,time.time()))

    def ingest(self,event):
        e=normalize(event); key=self.blob(e); prompt=str(event.get('prompt') or '')
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            if c.execute('SELECT 1 FROM continuity_events WHERE id=?',(e['id'],)).fetchone():return e['id']
            head=c.execute('SELECT * FROM continuity_heads WHERE owner=?',(e['owner'],)).fetchone()
            task=head['task'] if head else None
            summary=prompt or str(event.get('last_assistant_message') or e['command'] or e['tool'] or e['kind'])
            c.execute('INSERT INTO continuity_events(id,owner,session,turn,kind,task,observed,object,summary) VALUES(?,?,?,?,?,?,?,?,?)',(e['id'],e['owner'],e['session'],e['turn'],e['kind'],task,e['observed'],key,summary))
            c.execute('INSERT INTO continuity_search VALUES(?,?,?,?)',(e['id'],e['owner'],task,summary))
            if e['kind']=='UserPromptSubmit' and prompt and not hook_input(prompt):
                c.execute('INSERT INTO continuity_heads VALUES(?,?,?,?) ON CONFLICT(owner) DO UPDATE SET input_id=excluded.input_id,turn=excluded.turn',(e['owner'],task,e['id'],e['turn']))
            c.execute('COMMIT')
        return e['id']

    def register_delegation(self,parent_owner,parent_input,start_id,prompt):
        """Explicit parent attestation, never a fabricated native human message.

        Codex may encrypt delegated prompts in both hook data and transcripts.
        A parent records its assignment against the real child lifecycle event.
        This is context/provenance, not permission or a security boundary.
        """
        start=self.event(start_id);source=self.event(parent_input)
        if not start or start['kind']!='SubagentStart' or not start.get('agent'):
            raise ValueError('delegation requires an observed native child start')
        if not source or source['owner']!=parent_owner or source['session']!=start['session']:
            raise ValueError('parent input and child must belong to the same conversation')
        if source['kind'] not in ('UserPromptSubmit','AgentDelegation'):
            raise ValueError('parent request provenance is missing')
        prompt=str(prompt).strip()
        if not prompt or hook_input(prompt):raise ValueError('actual delegated assignment required')
        owner=start['owner'];ident=digest(['agent_delegation',parent_owner,parent_input,start_id,prompt])
        payload={'prompt':redact(prompt),'parent_owner':parent_owner,'parent_input':parent_input,'start_event':start_id,'provenance':'parent_agent_attestation','not_human_instruction':True}
        e=dict(start,id=ident,kind='AgentDelegation',origin='agent_declared',payload=payload,observed=time.time())
        key=self.blob(e)
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            prior=c.execute('SELECT id FROM continuity_events WHERE id=?',(ident,)).fetchone()
            if prior:return ident
            parent=c.execute('SELECT * FROM continuity_heads WHERE owner=?',(parent_owner,)).fetchone()
            if not parent or parent['input_id']!=parent_input:raise ValueError('parent input superseded; rebase delegation')
            head=c.execute('SELECT * FROM continuity_heads WHERE owner=?',(owner,)).fetchone()
            if head and head['input_id']:
                current=self.event(head['input_id'])
                if current and (current['turn']!=start['turn'] or current['kind']=='UserPromptSubmit'):
                    raise ValueError('delegation cannot overwrite a newer or human child request')
            task=head['task'] if head else None
            c.execute('INSERT INTO continuity_events(id,owner,session,turn,kind,task,observed,object,summary) VALUES(?,?,?,?,?,?,?,?,?)',(ident,owner,start['session'],start['turn'],'AgentDelegation',task,e['observed'],key,payload['prompt']))
            c.execute('INSERT INTO continuity_search VALUES(?,?,?,?)',(ident,owner,task,payload['prompt']))
            c.execute('INSERT INTO continuity_heads VALUES(?,?,?,?) ON CONFLICT(owner) DO UPDATE SET input_id=excluded.input_id,turn=excluded.turn',(owner,task,ident,start['turn']))
            c.execute('COMMIT')
        return ident

    def event(self,ident):
        with self.connect() as c:r=c.execute('SELECT * FROM continuity_events WHERE id=?',(ident,)).fetchone()
        return self.object(r['object']) if r else None

    def read_record(self,ident):
        value=self.task(ident) or self.event(ident)
        if value:return value
        with self.connect() as c:
            row=c.execute('SELECT n.data FROM continuity_native n JOIN continuity_native_ids i USING(path,offset) WHERE i.id=?',(ident,)).fetchone()
            if row:return json.loads(row[0])
            fact=c.execute('SELECT * FROM continuity_knowledge WHERE id=?',(ident,)).fetchone()
            if fact:return dict(fact,value=json.loads(fact['value']))
            row=c.execute('SELECT data FROM continuity_legacy WHERE id=?',(ident,)).fetchone()
            if row:return {'provenance':'legacy-unmodified','record':json.loads(row[0])}
        return self.object(ident)

    def task(self,ident):
        with self.connect() as c:r=c.execute('SELECT * FROM continuity_tasks WHERE id=?',(ident,)).fetchone()
        return dict(r,data=json.loads(r['data'])) if r else None

    def update_task(self,owner,input_id,action,*,task_id=None,summary=None,details=None,expected_revision=None,update_id=None):
        """Agent interpretation anchored to a human input or attributed delegation, not a grant.
        Natural language interpretation belongs to the agent, not a prefix regex.
        """
        if action not in {'continue','revise','suspend','resume','cancel','replace','start'}:raise ValueError('unknown task action')
        source=self.event(input_id)
        if not source or source['owner']!=owner or source['kind'] not in ('UserPromptSubmit','AgentDelegation'):raise ValueError('task update needs this owner native input')
        eid=update_id or digest([owner,input_id,action,task_id,summary,details])
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            prior=c.execute('SELECT result FROM continuity_updates WHERE id=?',(eid,)).fetchone()
            if prior:return json.loads(prior[0])
            head=c.execute('SELECT * FROM continuity_heads WHERE owner=?',(owner,)).fetchone()
            if head and head['input_id']!=input_id:raise ValueError('superseded input; rebase memory update on latest user input')
            target=task_id or (head['task'] if head else None)
            old=c.execute('SELECT * FROM continuity_tasks WHERE id=?',(target,)).fetchone() if target else None
            if old and old['owner']!=owner:raise ValueError('cannot update another agent task')
            if expected_revision is not None and old and old['revision']!=expected_revision:raise ValueError('revision conflict')
            if action in {'resume','cancel','suspend'} and not old:raise ValueError('task identity not found')
            if action=='resume' and head and head['task'] and head['task']!=target:
                c.execute("UPDATE continuity_tasks SET status='suspended',updated=? WHERE id=? AND status='active'",(time.time(),head['task']))
            if action in {'start','replace'} or not old:
                if old:c.execute("UPDATE continuity_tasks SET status=?,updated=? WHERE id=?",('superseded' if action=='replace' else 'suspended',time.time(),old['id']))
                target='task:'+digest([owner,input_id,eid]);data={'original_input':input_id,'original_request':source['payload'].get('prompt',''),'summary':summary or source['payload'].get('prompt',''),'instructions':[],'decisions':[],'artifacts':[],'next_actions':[], 'interpretation':'agent','request_origin':source['origin'],'request_kind':source['kind'],'parent_agent':source['session'] if source['agent'] else None,'supersedes':old['id'] if old and action=='replace' else None}
                status='active';revision=1
            else:
                data=json.loads(old['data']);revision=old['revision']+1
                status={'suspend':'suspended','cancel':'canceled','resume':'active'}.get(action,old['status'])
                if action=='revise':data.setdefault('revisions',[]).append({'source':input_id,'previous_summary':data.get('summary')})
                if summary is not None:data['summary']=summary
            data['latest_input']=input_id
            if details:
                for k in ('instructions','decisions','artifacts','next_actions','exact_values','review_findings','requirements'):
                    if k in details:data[k]=details[k]
            project=project_identity(source['cwd'])
            c.execute('INSERT OR REPLACE INTO continuity_tasks VALUES(?,?,?,?,?,?,?,?)',(target,owner,project,source['session'] if source['agent'] else None,status,revision,json.dumps(data),time.time()))
            c.execute('INSERT INTO continuity_task_revisions VALUES(?,?,?,?)',(target,revision,json.dumps(data),input_id))
            c.execute('UPDATE continuity_heads SET task=? WHERE owner=?',(target,owner))
            c.execute('UPDATE continuity_events SET task=? WHERE id=?',(target,input_id))
            c.execute('UPDATE continuity_search SET task=? WHERE event_id=?',(target,input_id))
            result={'id':target,'revision':revision,'status':status,'source':input_id}
            c.execute('INSERT INTO continuity_updates VALUES(?,?)',(eid,json.dumps(result)));c.execute('COMMIT')
        return result

    def context(self,owner,limit=6000):
        with self.connect() as c:
            head=c.execute('SELECT * FROM continuity_heads WHERE owner=?',(owner,)).fetchone()
            tasks=[dict(r) for r in c.execute('SELECT id,status,revision FROM continuity_tasks WHERE owner=? ORDER BY updated DESC',(owner,))]
            gaps=[dict(r) for r in c.execute('SELECT id,detail FROM continuity_gaps WHERE owner=?',(owner,))]
            legacy=[dict(r) for r in c.execute("SELECT kind,id,source FROM continuity_legacy WHERE owner IN (?,?) AND kind IN ('goal','checkpoint','knowledge')",(owner,'codex:'+owner))]
        current=self.task(head['task']) if head and head['task'] else None
        knowledge=self.recall_knowledge('project',current['project']) if current else []
        preferences=self.recall_knowledge('preference','user')
        packet={'owner':owner,'current_task':current,'current_input':dict(head) if head else None,'task_index':tasks,'legacy_context':legacy,'evidence_gaps':gaps,'rule':'Current user instructions govern. Memory is context, not permission. Other agents own their tasks. Use task-update for semantic changes; never infer task boundaries from quoted source instructions. Legacy records retain their original verification status and do not automatically reactivate tasks.'}
        packet.update(project_knowledge=knowledge,personal_preferences=preferences)
        if len(json.dumps(packet))>limit:
            pointer=self.blob(packet)
            packet={'owner':owner,'current_input':dict(head) if head else None,'current_task':dict({k:current[k] for k in ('id','revision','status')},summary=current['data'].get('summary','')[:1500],next_actions=current['data'].get('next_actions',[])[:3]) if current else None,'full_context_object':pointer,'task_count':len(tasks),'rule':'Retrieve relevant memory by ID; no need to load all history. Current user instructions govern.'}
        return packet

    def knowledge(self,scope,scope_id,kind,value,source,supersedes=None):
        if scope not in ('project','preference','thread'):raise ValueError('explicit knowledge scope required')
        if kind not in ('human_instruction','interpretation','plan','observation','verification','decision'):raise ValueError('explicit provenance kind required')
        value=redact(value);key=digest([scope,scope_id,kind,value,source,supersedes])
        with self.connect() as c:c.execute('INSERT OR IGNORE INTO continuity_knowledge VALUES(?,?,?,?,?,?,?)',(key,scope,scope_id,kind,json.dumps(value),source,supersedes))
        return key

    def recall_knowledge(self,scope,scope_id):
        with self.connect() as c:
            return [dict(r,value=json.loads(r['value'])) for r in c.execute('SELECT * FROM continuity_knowledge WHERE scope=? AND scope_id=? AND id NOT IN (SELECT supersedes FROM continuity_knowledge WHERE supersedes IS NOT NULL)',(scope,scope_id))]

    def search(self,owner,query,limit=20,after=0):
        with self.connect() as c:
            rows=c.execute('SELECT rowid,event_id,task,text FROM continuity_search WHERE continuity_search MATCH ? AND owner=? AND rowid>? ORDER BY rowid LIMIT ?',('"'+query.replace('"','""')+'"',owner,after,limit)).fetchall()
        return [dict(r) for r in rows]

    def index_native(self,path,owner):
        """Incremental public transcript index, no tail or message-count cutoff."""
        path=Path(path);stat=path.stat()
        with self.connect() as c:
            prior=c.execute('SELECT * FROM continuity_sources WHERE path=?',(str(path),)).fetchone()
            offset=prior['offset'] if prior and prior['inode']==stat.st_ino and stat.st_size>=prior['offset'] else 0
            current=prior['current_turn'] if offset else None
            generation=prior['generation'] if prior and offset and prior['generation'] else uuid.uuid4().hex
            with path.open('rb') as source:
                source.seek(offset);c.execute('BEGIN IMMEDIATE')
                while True:
                    start=source.tell();line=source.readline()
                    if not line:break
                    if not line.endswith(b'\n'):source.seek(start);break
                    try:row=json.loads(line)
                    except ValueError:
                        quarantine=self.blob({'path':str(path),'offset':start,'raw':line.decode('utf-8','replace')})
                        c.execute('INSERT OR IGNORE INTO continuity_gaps VALUES(?,?,?,?,?)',(digest([str(path),start,quarantine]),owner,str(path),'Malformed native record at byte '+str(start)+'; quarantine object '+quarantine,time.time()));continue
                    p=row.get('payload',{})
                    if row.get('type')=='turn_context':current=p.get('turn_id')
                    if row.get('type')=='event_msg' and p.get('type')=='task_complete' and isinstance(p.get('last_agent_message'),str):
                        c.execute('INSERT OR IGNORE INTO continuity_deliveries VALUES(?,?,?,?)',(owner,p.get('turn_id') or current,digest(p['last_agent_message']),str(path)+':'+str(start)))
                    public=row.get('type')=='turn_context' or row.get('type')=='response_item' and p.get('type')=='message' and (p.get('role')=='user' or p.get('role')=='assistant' and p.get('phase') in ('final','final_answer'))
                    if public:
                        versioned_path=str(path)+'@'+generation
                        ident='native:'+digest([versioned_path,start]);clean=json.dumps(redact(row))
                        c.execute('INSERT OR IGNORE INTO continuity_native VALUES(?,?,?,?,?,?)',(versioned_path,start,owner,p.get('internal_chat_message_metadata_passthrough',{}).get('turn_id') or current,row['type'],clean))
                        inserted=c.execute('INSERT OR IGNORE INTO continuity_native_ids VALUES(?,?,?)',(ident,versioned_path,start)).rowcount
                        if inserted:c.execute('INSERT INTO continuity_search VALUES(?,?,?,?)',(ident,owner,None,clean))
                c.execute('INSERT OR REPLACE INTO continuity_sources(path,inode,offset,current_turn,generation) VALUES(?,?,?,?,?)',(str(path),stat.st_ino,source.tell(),current,generation));c.execute('COMMIT')

    def native(self,owner,turn=None,after=0,limit=100):
        with self.connect() as c:
            sql='SELECT rowid,* FROM continuity_native WHERE owner=? AND rowid>?';args=[owner,after]
            if turn:sql+=' AND turn=?';args.append(turn)
            return [dict(r) for r in c.execute(sql+' ORDER BY rowid LIMIT ?',args+[limit])]

def main():
    p=argparse.ArgumentParser();p.add_argument('--owner',required=True)
    sub=p.add_subparsers(dest='command',required=True)
    sub.add_parser('context');sub.add_parser('migrate')
    a=sub.add_parser('read');a.add_argument('id')
    a=sub.add_parser('search');a.add_argument('query');a.add_argument('--after',type=int,default=0)
    a=sub.add_parser('index-native');a.add_argument('path')
    a=sub.add_parser('native');a.add_argument('--turn');a.add_argument('--after',type=int,default=0)
    a=sub.add_parser('knowledge-read');a.add_argument('--scope',required=True);a.add_argument('--scope-id',required=True)
    a=sub.add_parser('knowledge-write');a.add_argument('--scope',required=True);a.add_argument('--scope-id',required=True);a.add_argument('--kind',required=True);a.add_argument('--source',required=True);a.add_argument('--supersedes')
    a=sub.add_parser('register-delegation');a.add_argument('--parent-input',required=True);a.add_argument('--start-event',required=True)
    a=sub.add_parser('task-update');a.add_argument('--input-id',required=True);a.add_argument('--action',required=True);a.add_argument('--task-id');a.add_argument('--summary');a.add_argument('--revision',type=int)
    args=p.parse_args();s=Store(timeout=5)
    if args.command=='migrate':result=s.migrate()
    elif args.command=='context':result=s.context(args.owner)
    elif args.command=='search':result=s.search(args.owner,args.query,after=args.after)
    elif args.command=='read':result=s.read_record(args.id)
    elif args.command=='index-native':s.index_native(args.path,args.owner);result={'indexed':args.path}
    elif args.command=='native':result=s.native(args.owner,args.turn,after=args.after)
    elif args.command=='register-delegation':
        ident=s.register_delegation(args.owner,args.parent_input,args.start_event,sys.stdin.read());child=s.event(ident)['owner'];ctx=s.context(child)
        result=s.update_task(child,ident,'continue' if ctx.get('current_task') else 'start')
        atomic(runtime()/'heads'/(digest(child)+'.json'),s.context(child));result.update(owner=child,input_id=ident,provenance='parent_agent_attestation')
    elif args.command=='knowledge-read':result=s.recall_knowledge(args.scope,args.scope_id)
    elif args.command=='knowledge-write':result={'id':s.knowledge(args.scope,args.scope_id,args.kind,json.load(sys.stdin),args.source,args.supersedes)}
    else:
        raw=sys.stdin.read().strip() if not sys.stdin.isatty() else ''
        details=json.loads(raw) if raw else None
        result=s.update_task(args.owner,args.input_id,args.action,task_id=args.task_id,summary=args.summary,details=details,expected_revision=args.revision)
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
