import { FormEvent, CSSProperties, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity, AlertCircle, ArrowDown, ArrowLeft, ArrowRight, Bell, BookOpen, Check, CheckCircle2,
  ChevronDown, ChevronLeft, ChevronRight, CircleHelp, Clock3, Database, Download, ExternalLink,
  FileClock, Filter, History, Home, LoaderCircle, LogOut, Menu, Moon, MoreHorizontal, Pause,
  Play, Plus, Radio, RefreshCw, Search, Settings, ShieldCheck, Smartphone, Square, Sun, Trash2,
  Users, UserRound, UserRoundCheck, X, XCircle, Zap, Send, ListChecks, LayoutDashboard, PlugZap,
} from 'lucide-react';
import { api, ApiError, clearToken, getToken, openMigrationStream, setToken } from './api/client';
import type { Channel, Job, JobItem, Member, MigrationEvent, Page, TelegramAccount, User } from './types';

 type PageName = 'dashboard' | 'new' | 'migrations' | 'channels' | 'members' | 'history' | 'settings';
const SELECTED_JOB_KEY = 'tg_migrator_selected_job';
const readSelectedJobId = () => { try { return sessionStorage.getItem(SELECTED_JOB_KEY); } catch { return null; } };
const fmt = (n: number | null | undefined) => new Intl.NumberFormat().format(n || 0);
const dateFmt = (v?: string | null) => v ? new Date(v).toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' }) : '—';
const lower = (v?: string | null) => (v || '').toLowerCase().replaceAll('_', ' ');
const statusTone = (status: string) => {
  const s = status.toUpperCase();
  if (['COMPLETED', 'SUCCESS', 'ACTIVE', 'CONNECTED', 'ELIGIBLE'].includes(s)) return 'green';
  if (['RUNNING', 'ANALYZING', 'PROCESSING'].includes(s)) return 'blue';
  if (['PAUSED', 'PARTIAL', 'RATE_LIMITED', 'ALREADY_IN_DESTINATION'].includes(s)) return 'amber';
  if (['FAILED', 'CANCELLED', 'ERROR', 'PRIVACY_RESTRICTED', 'RESTRICTED'].includes(s)) return 'red';
  return 'gray';
};
function Badge({ value }: { value: string }) { return <span className={`badge ${statusTone(value)}`}>{lower(value)}</span>; }
function Metric({ title, value, sub, icon: Icon, tone = 'blue' }: {title:string;value:string|number;sub:string;icon:any;tone?:string}) {
  return <div className={`metric-card ${tone}`}><div className="metric-top"><span>{title}</span><span className="metric-icon"><Icon size={19}/></span></div><div className="metric-value">{typeof value === 'number' ? fmt(value) : value}</div><div className="metric-sub">{sub}</div></div>;
}
function ErrorBanner({ message, onClose }: {message:string;onClose?:()=>void}) { return <div className="error-banner"><AlertCircle size={18}/><span>{message}</span>{onClose && <button className="icon-btn" onClick={onClose} aria-label="Dismiss"><X size={16}/></button>}</div>; }
function EmptyState({ icon: Icon = Activity, title, description, action }: {icon?:any;title:string;description:string;action?:any}) { return <div className="empty-state"><span className="empty-icon"><Icon size={25}/></span><h3>{title}</h3><p>{description}</p>{action}</div>; }

export default function App() {
  const [token, setTokenState] = useState(getToken());
  const [user, setUser] = useState<User | null>(null);
  const [jobId, setJobId] = useState<string | null>(() => readSelectedJobId());
  const [page, setPage] = useState<PageName>(() => readSelectedJobId() ? 'migrations' : 'dashboard');
  const [accounts, setAccounts] = useState<TelegramAccount[]>([]);
  const [jobs, setJobs] = useState<Page<JobItem> | null>(null);
  const [channels, setChannels] = useState<Channel[]>([]);
  const [busy, setBusy] = useState(false);
  const [booting, setBooting] = useState(Boolean(getToken()));
  const [error, setError] = useState('');
  const [dark, setDark] = useState(false);
  const [mobileNav, setMobileNav] = useState(false);

  const loadMe = useCallback(async () => {
    if (!getToken()) { setBooting(false); return; }
    try {
      const me = await api<User>('/auth/me'); setUser(me); setTokenState(getToken());
      const [a, j] = await Promise.all([
        api<TelegramAccount[]>('/telegram/accounts'),
        api<Page<JobItem>>('/migrations?limit=100&offset=0'),
      ]);
      setAccounts(a); setJobs(j);
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) { clearToken(); setTokenState(null); setUser(null); }
      else setError(errMessage(e));
    } finally { setBooting(false); }
  }, []);

  useEffect(() => { void loadMe(); }, [loadMe]);
  useEffect(() => { document.documentElement.dataset.theme = dark ? 'dark' : 'light'; }, [dark]);
  useEffect(() => {
    try {
      if (jobId) sessionStorage.setItem(SELECTED_JOB_KEY, jobId);
      else sessionStorage.removeItem(SELECTED_JOB_KEY);
    } catch { /* Session storage may be unavailable in restricted browser contexts. */ }
  }, [jobId]);

  const refreshSummary = useCallback(async () => {
    if (!getToken()) return;
    try {
      const [a, j] = await Promise.all([api<TelegramAccount[]>('/telegram/accounts'), api<Page<JobItem>>('/migrations?limit=100&offset=0')]);
      setAccounts(a); setJobs(j);
    } catch (e) { setError(errMessage(e)); }
  }, []);

  function logout() { clearToken(); setTokenState(null); setUser(null); setAccounts([]); setJobs(null); setChannels([]); setJobId(null); setPage('dashboard'); }
  async function loginComplete(newToken: string) { setToken(newToken); setTokenState(newToken); setBooting(true); setError(''); await loadMe(); }
  function navigate(next: PageName) { setPage(next); setJobId(null); setMobileNav(false); setError(''); if (next === 'channels' || next === 'members') void refreshChannels(); }
  async function refreshChannels(accountId?: string) {
    try {
      const chosenId = accountId || accounts.find(a => a.status === 'ACTIVE')?.id;
      if (!chosenId) { setChannels([]); return; }
      const list = await api<Channel[]>(`/telegram/channels?account_id=${encodeURIComponent(chosenId)}&administered_only=true&refresh=true`);
      setChannels(list);
    } catch (e) { setError(errMessage(e)); }
  }
  function openJob(id: string) { setJobId(id); setPage('migrations'); setError(''); setMobileNav(false); }

  if (booting) return <div className="full-loader"><LoaderCircle className="spin" size={28}/><span>Loading your workspace…</span></div>;
  if (!token || !user) return <AuthScreen onSuccess={loginComplete} />;

  const themeToggle = <button className="icon-btn" aria-label="Toggle theme" onClick={() => setDark(v => !v)}>{dark ? <Sun size={18}/> : <Moon size={18}/>}</button>;
  const navItems: {id: PageName;label:string;icon:any}[] = [
    { id:'dashboard', label:'Dashboard', icon:LayoutDashboard },
    { id:'new', label:'New migration', icon:Plus },
    { id:'migrations', label:'Migrations', icon:Activity },
    { id:'channels', label:'Channels', icon:Radio },
    { id:'members', label:'Members', icon:Users },
    { id:'history', label:'History', icon:History },
    { id:'settings', label:'Settings', icon:Settings },
  ];
  return <div className="app-shell">
    <aside className={`sidebar ${mobileNav ? 'open' : ''}`}>
      <div className="brand"><span className="brand-mark"><Send size={19}/></span><span>Telegram <b>Migrator</b></span><button className="icon-btn sidebar-close" onClick={() => setMobileNav(false)} aria-label="Close menu"><X size={19}/></button></div>
      <div className="side-caption">WORKSPACE</div>
      <nav>{navItems.map(item => { const Icon = item.icon; const active = page === item.id || (item.id === 'migrations' && Boolean(jobId)); return <button key={item.id} className={`nav-item ${active ? 'active':''}`} onClick={() => navigate(item.id)}><Icon size={18}/><span>{item.label}</span>{item.id === 'migrations' && jobs?.items.some(j => j.status === 'RUNNING') && <i className="nav-dot"/>}</button>; })}</nav>
      <div className="sidebar-spacer"/>
      <div className="account-mini"><div className="account-avatar"><Send size={19}/></div><div><strong>{accounts.find(a => a.status === 'ACTIVE')?.first_name || (accounts.length ? 'Telegram connected' : 'Telegram not connected')}</strong><span>{accounts.find(a => a.status === 'ACTIVE')?.username ? `@${accounts.find(a => a.status === 'ACTIVE')?.username}` : (accounts.length ? `${accounts.length} account(s)` : 'Connect to get started')}</span></div><span className={`online-dot ${accounts.some(a => a.status === 'ACTIVE') ? '':'offline'}`}/></div>
      <div className="sidebar-footer"><span>Workspace · v1.0</span><span className="privacy-mini"><ShieldCheck size={13}/> Private by design</span></div>
    </aside>
    {mobileNav && <button className="mobile-scrim" onClick={() => setMobileNav(false)} aria-label="Close navigation"/>}
    <main className="main-area">
      <header className="topbar"><div className="topbar-left"><button className="icon-btn menu-toggle" onClick={() => setMobileNav(true)} aria-label="Open menu"><Menu size={21}/></button><div className="global-search"><Search size={17}/><input placeholder="Search channels, migrations, members…" onKeyDown={e => { if (e.key === 'Enter') { navigate('migrations'); } }}/></div></div><div className="topbar-right">{themeToggle}<button className="icon-btn notification-btn" aria-label="Notifications" onClick={() => setError('You are all caught up.')}><Bell size={18}/><i/></button><div className="top-profile"><span className="profile-avatar">{(user.email[0] || 'U').toUpperCase()}</span><div><strong>{user.email.split('@')[0]}</strong><span>{user.email}</span></div><ChevronDown size={15}/></div><button className="icon-btn logout-btn" onClick={logout} title="Sign out"><LogOut size={17}/></button></div></header>
      <div className="page-content">
        {error && <ErrorBanner message={error} onClose={() => setError('')}/>}
        {page === 'dashboard' && <Dashboard user={user} accounts={accounts} jobs={jobs} onNew={() => navigate('new')} onConnect={() => navigate('settings')} onOpenJob={openJob} onRefresh={refreshSummary} />}
        {page === 'new' && <MigrationWizard accounts={accounts} onDone={async id => { await refreshSummary(); setJobId(id); setPage('migrations'); }} onRefreshAccounts={refreshSummary} />}
        {page === 'migrations' && <MigrationsPage selectedId={jobId} onOpenJob={openJob} onRefresh={refreshSummary} onClearSelection={() => setJobId(null)} />}
        {page === 'channels' && <ChannelsPage accounts={accounts} channels={channels} onLoad={refreshChannels} />}
        {page === 'members' && <MembersPage accounts={accounts} onLoad={refreshChannels} channels={channels} />}
        {page === 'history' && <HistoryPage jobs={jobs} onOpenJob={openJob} onRefresh={refreshSummary} />}
        {page === 'settings' && <SettingsPage accounts={accounts} onChanged={refreshSummary} onConnect={refreshSummary} onError={setError} />}
      </div>
      <footer className="app-footer"><span><ShieldCheck size={14}/> Telegram rules apply. Invitations may be refused by Telegram.</span><a href="/docs" target="_blank" rel="noreferrer">API docs <ExternalLink size={12}/></a></footer>
    </main>
  </div>;
}

function errMessage(e: unknown) { return e instanceof Error ? e.message : 'Unexpected error. Please try again.'; }

function AuthScreen({ onSuccess }: {onSuccess:(token:string)=>Promise<void>}) {
  const [mode, setMode] = useState<'login'|'register'>('login');
  const [email, setEmail] = useState(''); const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false); const [error, setError] = useState('');
  async function submit(e: FormEvent) {
    e.preventDefault(); setBusy(true); setError('');
    try {
      if (mode === 'register') await api('/auth/register', { method:'POST', body:JSON.stringify({ email, password }) });
      const result = await api<{access_token:string}>('/auth/login', { method:'POST', body:JSON.stringify({ email, password }) });
      await onSuccess(result.access_token);
    } catch (err) { setError(errMessage(err)); }
    finally { setBusy(false); }
  }
  return <div className="auth-layout"><div className="auth-art"><div className="auth-art-brand"><span className="brand-mark"><Send size={20}/></span> Telegram <b>Migrator</b></div><div className="auth-art-copy"><span className="eyebrow">CHANNEL MANAGEMENT, SIMPLIFIED</span><h1>Manage your Telegram communities with clarity.</h1><p>Review eligibility, monitor every job, and keep every invitation auditable.</p><div className="auth-feature"><span><Check size={15}/></span> Real-time migration tracking</div><div className="auth-feature"><span><Check size={15}/></span> Mobile-first workspace</div><div className="auth-feature"><span><Check size={15}/></span> Built around Telegram's rules</div></div><div className="auth-art-bottom">Secure by design · Your sessions stay encrypted on the server</div></div><div className="auth-panel"><div className="auth-mobile-brand"><span className="brand-mark"><Send size={19}/></span><b>Telegram Migrator</b></div><form className="auth-form" onSubmit={submit}><div className="form-kicker">WELCOME</div><h2>{mode === 'login' ? 'Sign in to your workspace' : 'Create your workspace'}</h2><p className="muted">{mode === 'login' ? 'Use your app account to continue.' : 'Create an app account before connecting Telegram.'}</p>{error && <ErrorBanner message={error}/>}<label>Email address<input type="email" required autoComplete="email" value={email} onChange={e => setEmail(e.target.value)} placeholder="you@example.com"/></label><label>Password<input type="password" required minLength={mode === 'register' ? 12 : 1} autoComplete={mode === 'login'?'current-password':'new-password'} value={password} onChange={e => setPassword(e.target.value)} placeholder={mode === 'register'?'At least 12 characters':'Your password'}/></label><button className="btn primary full" disabled={busy}>{busy ? <LoaderCircle size={17} className="spin"/> : null}{mode === 'login' ? 'Sign in' : 'Create account'}<ArrowRight size={17}/></button><div className="auth-switch">{mode === 'login' ? 'New here?' : 'Already have an account?'} <button type="button" onClick={() => {setMode(mode === 'login'?'register':'login');setError('')}}>{mode === 'login'?'Create an account':'Sign in'}</button></div><div className="auth-safe"><ShieldCheck size={15}/> Telegram phone/code/password are never saved in the browser.</div></form></div></div>;
}

function Dashboard({ user, accounts, jobs, onNew, onConnect, onOpenJob, onRefresh }: {user:User;accounts:TelegramAccount[];jobs:Page<JobItem>|null;onNew:()=>void;onConnect:()=>void;onOpenJob:(id:string)=>void;onRefresh:()=>Promise<void>}) {
  const [refreshing, setRefreshing] = useState(false);
  const list = jobs?.items || [];
  const totals = useMemo(() => list.reduce((acc, j) => ({ total:acc.total+j.total_members, processed:acc.processed+j.processed_members, invited:acc.invited+j.successful_count, skipped:acc.skipped+j.skipped_count, failed:acc.failed+j.failed_count }), {total:0,processed:0,invited:0,skipped:0,failed:0}), [list]);
  const current = list.find(j => ['RUNNING','PAUSED','CANCELLING'].includes(j.status));
  const breakdown = [
    { title:'All analyzed', value:fmt(totals.total), icon:Users, tone:'blue', sub:'Across recent jobs' },
    { title:'Confirmed invited', value:fmt(totals.invited), icon:CheckCircle2, tone:'green', sub:'Telegram-confirmed' },
    { title:'Already processed / skipped', value:fmt(totals.skipped), icon:ListChecks, tone:'purple', sub:'Not invited' },
    { title:'Failed', value:fmt(totals.failed), icon:XCircle, tone:'red', sub:'Review job details' },
  ];
  return <>
    <PageHeader eyebrow="OVERVIEW" title="Dashboard" subtitle="Manage your Telegram channels and member migration jobs." actions={<><button className="btn subtle" onClick={async()=>{setRefreshing(true);await onRefresh();setRefreshing(false)}}><RefreshCw size={16} className={refreshing?'spin':''}/> Refresh</button><button className="btn primary" onClick={accounts.some(a=>a.status==='ACTIVE')?onNew:onConnect}><Plus size={18}/> New migration</button></>}/>
    {!accounts.some(a=>a.status==='ACTIVE') && <div className="connect-banner"><span className="connect-banner-icon"><PlugZap size={22}/></span><div><strong>Connect your Telegram account</strong><p>Connect an account you administer channels with before creating a migration.</p></div><button className="btn primary" onClick={onConnect}>Connect Telegram <ArrowRight size={16}/></button></div>}
    <div className="metric-grid">{breakdown.map(m => <Metric key={m.title} title={m.title} value={m.value} sub={m.sub} icon={m.icon} tone={m.tone}/>)}</div>
    <div className="dashboard-grid top-grid">
      <section className="panel current-panel"><PanelTitle title="Current migration" subtitle="Live summary of the active job" action={current ? <button className="text-link" onClick={()=>onOpenJob(current.id)}>Open details <ArrowRight size={14}/></button>:null}/>
        {current ? <div className="current-job"><div className="route-line"><ChannelAvatar title={current.source_title}/><div className="route-label"><strong>{current.source_title}</strong><span>Source channel</span></div><ArrowRight size={19} className="route-arrow"/><ChannelAvatar title={current.destination_title}/><div className="route-label"><strong>{current.destination_title}</strong><span>Destination</span></div><Badge value={current.status}/></div><div className="progress-row"><div className="progress-track"><span style={{width:`${Math.min(current.percent_complete,100)}%`}}/></div><strong>{Math.round(current.percent_complete)}%</strong></div><div className="progress-caption">{fmt(current.processed_members)} of {fmt(current.total_members)} processed</div><div className="mini-stats"><span><i className="dot green-dot"/> {fmt(current.successful_count)} invited</span><span><i className="dot amber-dot"/> {fmt(current.skipped_count)} skipped</span><span><i className="dot red-dot"/> {fmt(current.failed_count)} failed</span></div><button className="btn primary full" onClick={()=>onOpenJob(current.id)}>View live progress</button></div> : <EmptyState icon={Activity} title="No active migration" description="Start a new migration to analyze member eligibility and track progress." action={<button className="btn primary" onClick={onNew}><Plus size={16}/> Start a migration</button>}/>}
      </section>
      <section className="panel account-panel"><PanelTitle title="Telegram account" subtitle="Connected session status" action={<Badge value={accounts.some(a=>a.status==='ACTIVE')?'CONNECTED':'NOT CONNECTED'}/>}/>
        {accounts.filter(a=>a.status==='ACTIVE').length ? accounts.filter(a=>a.status==='ACTIVE').slice(0,2).map(a=><div className="account-row" key={a.id}><span className="profile-avatar blue-avatar"><Send size={16}/></span><div className="account-info"><strong>{a.first_name || 'Telegram user'}</strong><span>{a.username?`@${a.username}`:`ID ${a.telegram_user_id}`}</span><small>Last used {dateFmt(a.last_used_at)}</small></div><Badge value={a.status}/></div>) : <EmptyState icon={Smartphone} title="No account connected" description="Connect via Telegram's MTProto sign-in flow." action={<button className="btn secondary" onClick={onConnect}>Connect account</button>}/>}
        <div className="account-panel-foot"><span><ShieldCheck size={15}/> Session encrypted server-side</span><span><Database size={15}/> PostgreSQL</span></div>
      </section>
    </div>
    <div className="dashboard-grid lower-grid">
      <section className="panel recent-panel"><PanelTitle title="Recent migrations" subtitle="Latest jobs in your workspace" action={<button className="text-link" disabled={!list.length} onClick={()=>list[0]&&onOpenJob(list[0].id)}>View details <ArrowRight size={14}/></button>}/>
        {list.length ? <div className="table-wrap"><table><thead><tr><th>Source → Destination</th><th>Status</th><th>Processed</th><th>Invited</th><th>Created</th><th></th></tr></thead><tbody>{list.slice(0,6).map(j=><tr key={j.id}><td><strong className="route-text">{j.source_title} <span>→</span> {j.destination_title}</strong><small>{j.id.slice(0,8)}…</small></td><td><Badge value={j.status}/></td><td>{fmt(j.processed_members)} / {fmt(j.total_members)}</td><td className="green-text">{fmt(j.successful_count)}</td><td>{dateFmt(j.created_at)}</td><td><button className="icon-btn small" title="Open details" onClick={()=>onOpenJob(j.id)}><ChevronRight size={17}/></button></td></tr>)}</tbody></table></div> : <EmptyState icon={FileClock} title="No migration history yet" description="Your jobs and their outcomes will appear here." action={<button className="btn primary" onClick={onNew}><Plus size={16}/> Create first migration</button>}/>}
      </section>
      <section className="panel quick-panel"><PanelTitle title="Quick actions" subtitle="Common tasks"/><div className="quick-actions"><button onClick={onNew} className="quick-action blue"><span><Send size={19}/></span><strong>New migration</strong><small>Set up a new job</small></button><button onClick={onConnect} className="quick-action violet"><span><PlugZap size={19}/></span><strong>Telegram account</strong><small>Manage connection</small></button><button onClick={()=>list[0]&&onOpenJob(list[0].id)} disabled={!list.length} className="quick-action green"><span><FileClock size={19}/></span><strong>Last migration</strong><small>Review details</small></button><button onClick={()=>window.open('/docs','_blank','noopener,noreferrer')} className="quick-action amber"><span><BookOpen size={19}/></span><strong>API documentation</strong><small>Open FastAPI docs</small></button></div><div className="system-health"><span className="health-check"><CheckCircle2 size={16}/></span><div><strong>Application UI is ready</strong><small>Use Docker health checks and /api/health/ready to verify current backend readiness.</small></div><span className="status-pill">Monitor backend</span></div></section>
    </div>
  </>;
}

function PageHeader({eyebrow,title,subtitle,actions}: {eyebrow?:string;title:string;subtitle?:string;actions?:any}) { return <div className="page-header"><div><div className="eyebrow">{eyebrow}</div><h1>{title}</h1>{subtitle && <p>{subtitle}</p>}</div>{actions && <div className="header-actions">{actions}</div>}</div>; }
function PanelTitle({title,subtitle,action}: {title:string;subtitle?:string;action?:any}) { return <div className="panel-title"><div><h2>{title}</h2>{subtitle && <p>{subtitle}</p>}</div>{action}</div>; }
function ChannelAvatar({title}: {title:string}) { return <span className="channel-avatar">{(title || 'T').trim().slice(0,1).toUpperCase()}</span>; }

function MigrationWizard({ accounts, onDone, onRefreshAccounts }: {accounts:TelegramAccount[];onDone:(id:string)=>Promise<void>;onRefreshAccounts:()=>Promise<void>}) {
  const [step,setStep] = useState(1); const [accountId,setAccountId] = useState(accounts.find(a=>a.status==='ACTIVE')?.id || '');
  const [channels,setChannels] = useState<Channel[]>([]); const [source,setSource] = useState<Channel|null>(null); const [destination,setDestination] = useState<Channel|null>(null);
  const [memberLimit,setMemberLimit] = useState('500'); const [job,setJob] = useState<Job|null>(null); const [members,setMembers] = useState<Member[]>([]);
  const [membersTotal,setMembersTotal] = useState(0); const [memberOffset,setMemberOffset] = useState(0); const [memberSearch,setMemberSearch] = useState('');
  const [filter,setFilter] = useState('ELIGIBLE'); const [busy,setBusy] = useState(false); const [error,setError] = useState(''); const [ackPartial,setAckPartial] = useState(false);
  const analysisTimer = useRef<number|undefined>(undefined);

  const loadChannels = useCallback(async () => {
    if (!accountId) return;
    setBusy(true); setError('');
    try { setChannels(await api<Channel[]>(`/telegram/channels?account_id=${encodeURIComponent(accountId)}&administered_only=true&refresh=true`)); }
    catch(e){setError(errMessage(e));} finally {setBusy(false);}
  },[accountId]);
  useEffect(()=>{ if (accountId) void loadChannels(); },[accountId,loadChannels]);
  useEffect(()=>()=>{ if(analysisTimer.current) window.clearInterval(analysisTimer.current); },[]);
  const eligibleCount = job?.summary?.by_eligibility?.ELIGIBLE || 0;

  async function startAnalysis() {
    if (!accountId || !source || !destination) { setError('Choose both source and destination channels.'); return; }
    if (source.id === destination.id) { setError('Source and destination must be different channels.'); return; }
    setBusy(true); setError('');
    try {
      const created = await api<Job>('/migrations',{method:'POST',body:JSON.stringify({account_id:accountId,source_channel_id:source.id,destination_channel_id:destination.id,member_limit:Number(memberLimit)||500})});
      setJob(created); setStep(2);
      const queued = await api<Job>(`/migrations/${created.id}/eligibility`,{method:'POST',body:JSON.stringify({})}); setJob(queued);
      analysisTimer.current = window.setInterval(async()=>{
        try { const current = await api<Job>(`/migrations/${created.id}`); setJob(current); if (['COMPLETED','PARTIAL','FAILED'].includes(current.analysis_state)) { if (analysisTimer.current) window.clearInterval(analysisTimer.current); setBusy(false); setStep(current.analysis_state==='FAILED'?2:3); } }
        catch(e){ if(analysisTimer.current)window.clearInterval(analysisTimer.current);setBusy(false);setError(errMessage(e)); }
      },1800);
    } catch(e){setBusy(false);setError(errMessage(e));}
  }
  async function loadMembers(nextOffset=0,nextFilter=filter,search=memberSearch) {
    if (!job) return;
    setBusy(true); setError('');
    try {
      const params=new URLSearchParams({limit:'50',offset:String(nextOffset)});
      if(nextFilter && nextFilter!=='ALL') params.set('eligibility',nextFilter);
      if(search.trim()) params.set('search',search.trim());
      const result=await api<Page<Member>>(`/migrations/${job.id}/members?${params.toString()}`);
      setMembers(result.items);setMembersTotal(result.total);setMemberOffset(result.offset);setFilter(nextFilter);setMemberSearch(search);
    } catch(e){setError(errMessage(e));} finally {setBusy(false);}
  }
  useEffect(()=>{ if(step===3 && job) void loadMembers(0,filter,''); /* initial member query */ },[step,job?.id]);
  async function toggleMember(member:Member, selected:boolean) {
    if(!job) return;
    setMembers(prev=>prev.map(m=>m.id===member.id?{...m,selected}:m));
    try { await api(`/migrations/${job.id}/selection`,{method:'POST',body:JSON.stringify({telegram_user_ids:[member.telegram_user_id],selected})}); }
    catch(e){setError(errMessage(e));setMembers(prev=>prev.map(m=>m.id===member.id?{...m,selected:member.selected}:m));}
  }
  async function setAll(selected:boolean) {
    if(!job)return;
    setBusy(true); setError('');
    try { await api(`/migrations/${job.id}/selection`,{method:'POST',body:JSON.stringify({telegram_user_ids:null,selected})}); await loadMembers(memberOffset,filter,memberSearch); }
    catch(e){setError(errMessage(e));} finally{setBusy(false);}
  }
  async function goStart() {
    if(!job)return; setBusy(true);setError('');
    try { const out=await api<Job>(`/migrations/${job.id}/start`,{method:'POST',body:JSON.stringify({acknowledge_partial:job.analysis_state==='PARTIAL'&&ackPartial})}); setJob(out); await onDone(job.id); }
    catch(e){setError(errMessage(e));}finally{setBusy(false);}
  }
  return <>
    <PageHeader eyebrow="WORKFLOW" title="New migration" subtitle="Choose channels, analyze eligibility, then review before starting." actions={<span className="secure-label"><ShieldCheck size={15}/> Invitations only where permitted</span>}/>
    <div className="wizard-steps">{[{n:1,label:'Select channels'},{n:2,label:'Analyze'},{n:3,label:'Review & start'}].map((s,i)=><div key={s.n} className={`wizard-step ${step===s.n?'current':step>s.n?'done':''}`}><span>{step>s.n?<Check size={15}/>:s.n}</span><strong>{s.label}</strong>{i<2 && <i/>}</div>)}</div>
    {error&&<ErrorBanner message={error} onClose={()=>setError('')}/>}
    {step===1 && <section className="panel wizard-panel"><PanelTitle title="Select source and destination" subtitle="Only channels available to your connected account will be shown." action={<button className="btn subtle" onClick={()=>void loadChannels()} disabled={busy}><RefreshCw size={15} className={busy?'spin':''}/> Refresh</button>}/>
      {!accounts.some(a=>a.status==='ACTIVE') ? <EmptyState icon={PlugZap} title="Telegram account required" description="Connect an authorized Telegram account before starting."/> : <><label className="field-label">Connected Telegram account<select value={accountId} onChange={e=>setAccountId(e.target.value)}>{accounts.filter(a=>a.status==='ACTIVE').map(a=><option key={a.id} value={a.id}>{a.first_name || 'Telegram account'} {a.username?`(@${a.username})`:''}</option>)}</select></label><div className="channel-select-grid"><div><div className="select-heading"><span>1</span><div><strong>Source channel/group</strong><small>Members will be analyzed from this source.</small></div></div><div className="channel-list">{channels.length?channels.map(c=><button key={c.id} className={`channel-choice ${source?.id===c.id?'selected':''}`} onClick={()=>setSource(c)}><ChannelAvatar title={c.title}/><span className="channel-choice-info"><strong>{c.title}</strong><small>{c.username?`@${c.username} · `:''}{c.kind.toLowerCase()} · {c.participants_count===null?'Member count unknown':`${fmt(c.participants_count)} members`}</small><em>{c.can_invite_users?'Can invite users':c.is_admin?'Admin access':'Accessible'}</em></span><span className="select-radio">{source?.id===c.id?<Check size={15}/>:null}</span></button>):<div className="channel-empty">{busy?<LoaderCircle className="spin"/>:'No channels loaded yet. Tap Refresh.'}</div>}</div></div><div><div className="select-heading"><span>2</span><div><strong>Destination channel/group</strong><small>Invitations will be attempted here where allowed.</small></div></div><div className="channel-list">{channels.filter(c=>c.id!==source?.id).length?channels.filter(c=>c.id!==source?.id).map(c=><button key={c.id} className={`channel-choice ${destination?.id===c.id?'selected':''}`} onClick={()=>setDestination(c)}><ChannelAvatar title={c.title}/><span className="channel-choice-info"><strong>{c.title}</strong><small>{c.username?`@${c.username} · `:''}{c.kind.toLowerCase()} · {c.participants_count===null?'Member count unknown':`${fmt(c.participants_count)} members`}</small><em>{c.can_invite_users?'Can invite users':c.is_admin?'Admin access':'Accessible'}</em></span><span className="select-radio">{destination?.id===c.id?<Check size={15}/>:null}</span></button>):<div className="channel-empty">Select a different channel as destination.</div>}</div></div></div><div className="wizard-footer"><label className="field-label compact">Analysis limit<select value={memberLimit} onChange={e=>setMemberLimit(e.target.value)}><option value="100">100 members (test)</option><option value="500">500 members</option><option value="1000">1,000 members</option><option value="2000">2,000 members</option><option value="5000">5,000 members (max default cap)</option></select></label><button className="btn primary" disabled={!source||!destination||busy} onClick={()=>setStep(2)}>Analyze members <ArrowRight size={17}/></button></div></>}
    </section>}
    {step===2 && <section className="panel analysis-panel"><div className="analysis-illustration"><div><Search size={38}/></div><span className="analysis-orbit orbit-one"/><span className="analysis-orbit orbit-two"/></div><h2>{job?'Analyzing member eligibility':'Ready to analyze'}</h2><p>{job?'The background worker is checking available member data. This can take time and the result may be partial.':'No invitation is sent during analysis. You will review results before starting.'}</p>{job&&<><div className="large-progress"><span style={{width:`${job.analysis_state==='COMPLETED'||job.analysis_state==='PARTIAL'?100:Math.max(8,job.percent_complete)}%`}}/></div><div className="analysis-status"><Badge value={job.analysis_state}/><span>{fmt(job.summary.total_analyzed)} members analyzed</span></div>{job.notes?.map(n=><p className="note-line" key={n}><AlertCircle size={14}/>{n}</p>)}</>}{!job&&<div className="wizard-footer centered"><button className="btn subtle" onClick={()=>setStep(1)}><ArrowLeft size={16}/> Back</button><button className="btn primary" disabled={busy} onClick={startAnalysis}>{busy?<LoaderCircle size={17} className="spin"/>:<Search size={17}/>} Start eligibility analysis</button></div>}{job&&job.analysis_state==='FAILED'&&<div className="wizard-footer centered"><button className="btn secondary" onClick={()=>{setJob(null);setStep(1)}}>Back to channels</button><button className="btn primary" onClick={startAnalysis}>Try again</button></div>}{job&&['COMPLETED','PARTIAL'].includes(job.analysis_state)&&<div className="wizard-footer centered"><button className="btn secondary" onClick={()=>setStep(1)}><ArrowLeft size={16}/> Back</button><button className="btn primary" onClick={()=>setStep(3)}>Review eligible members <ArrowRight size={16}/></button></div>}</section>}
    {step===3 && job && <section className="panel member-review"><PanelTitle title="Review member eligibility" subtitle={`${fmt(membersTotal)} members match this filter. Eligibility is not a guarantee that Telegram will permit an invitation.`} action={<Badge value={job.analysis_state}/>}/><div className="analysis-stat-row"><div><strong>{fmt(job.summary.by_eligibility.ELIGIBLE||0)}</strong><span>Eligible (not guaranteed)</span></div><div><strong>{fmt(job.summary.by_eligibility.ALREADY_IN_DESTINATION||0)}</strong><span>Already in destination</span></div><div><strong>{fmt(job.summary.by_eligibility.PRIVACY_RESTRICTED||0)}</strong><span>Privacy restriction detected</span></div><div><strong>{fmt(job.summary.by_eligibility.UNKNOWN||0)}</strong><span>Unknown</span></div></div><div className="member-tools"><div className="search-field"><Search size={16}/><input value={memberSearch} onChange={e=>setMemberSearch(e.target.value)} onKeyDown={e=>{if(e.key==='Enter')void loadMembers(0,filter,memberSearch)}} placeholder="Search name or username"/></div><select value={filter} onChange={e=>void loadMembers(0,e.target.value,memberSearch)}><option value="ELIGIBLE">Eligible</option><option value="ALL">All eligibility states</option><option value="ALREADY_IN_DESTINATION">Already in destination</option><option value="PRIVACY_RESTRICTED">Privacy restricted</option><option value="DELETED_ACCOUNT">Deleted account</option><option value="BOT">Bot</option><option value="RESTRICTED">Restricted</option><option value="UNKNOWN">Unknown</option></select><button className="btn subtle" onClick={()=>void setAll(true)} disabled={busy}>Select all eligible</button></div><div className="table-wrap review-table"><table><thead><tr><th>Member</th><th>Role</th><th>Eligibility</th><th>Reason</th><th>Selected</th></tr></thead><tbody>{members.map(m=><tr key={m.id}><td><div className="member-person"><span className="member-avatar">{(m.display_name||'?').slice(0,1).toUpperCase()}</span><span><strong>{m.display_name}</strong><small>{m.username?`@${m.username}`:`ID ${m.telegram_user_id}`}</small></span></div></td><td>{lower(m.role)}</td><td><Badge value={m.eligibility}/></td><td>{m.reason||m.failure_reason||'—'}</td><td><input type="checkbox" aria-label={`Select ${m.display_name}`} disabled={m.eligibility!=='ELIGIBLE'} checked={Boolean(m.selected)} onChange={e=>void toggleMember(m,e.target.checked)}/></td></tr>)}</tbody></table>{!members.length&&<div className="table-empty">{busy?<LoaderCircle className="spin"/>:'No members match this filter.'}</div>}</div><div className="table-pager"><span>Showing {members.length?memberOffset+1:0}–{Math.min(memberOffset+members.length,membersTotal)} of {fmt(membersTotal)}</span><div><button className="btn subtle small-btn" disabled={memberOffset===0||busy} onClick={()=>void loadMembers(Math.max(0,memberOffset-50),filter,memberSearch)}><ChevronLeft size={15}/> Previous</button><button className="btn subtle small-btn" disabled={memberOffset+members.length>=membersTotal||busy} onClick={()=>void loadMembers(memberOffset+50,filter,memberSearch)}>Next <ChevronRight size={15}/></button></div></div><div className="compliance-note"><ShieldCheck size={18}/><span><strong>Important:</strong> Telegram may still reject an invitation based on privacy or account restrictions. No restriction will be bypassed and the report will preserve exact outcomes.</span></div>{job.analysis_state==='PARTIAL'&&<label className="ack-row"><input type="checkbox" checked={ackPartial} onChange={e=>setAckPartial(e.target.checked)}/> I understand the analysis is partial and want to continue with available results.</label>}<div className="wizard-footer"><button className="btn subtle" onClick={()=>setStep(1)}><ArrowLeft size={16}/> Start over</button><button className="btn primary" onClick={goStart} disabled={busy||(job.analysis_state==='PARTIAL'&&!ackPartial)}>{busy?<LoaderCircle size={17} className="spin"/>:<Play size={17}/>} Create and start migration <ArrowRight size={16}/></button></div></section>}
  </>;
}

function MigrationsPage({selectedId,onOpenJob,onRefresh,onClearSelection}: {selectedId:string|null;onOpenJob:(id:string)=>void;onRefresh:()=>Promise<void>;onClearSelection:()=>void}) {
  const [jobs,setJobs] = useState<Page<JobItem>|null>(null); const [selected,setSelected]=useState<Job|null>(null); const [loading,setLoading]=useState(true); const [error,setError]=useState(''); const [filter,setFilter]=useState('ALL');
  const refresh=useCallback(async()=>{setLoading(true);try{const q=filter==='ALL'?'':`&status=${filter}`;const data=await api<Page<JobItem>>(`/migrations?limit=100&offset=0${q}`);setJobs(data);}catch(e){setError(errMessage(e))}finally{setLoading(false)}},[filter]);
  useEffect(()=>{void refresh()},[refresh]);
  useEffect(()=>{if(!selectedId){setSelected(null);return}let live=true;setLoading(true);api<Job>(`/migrations/${selectedId}`).then(j=>{if(live)setSelected(j)}).catch(e=>{if(live)setError(errMessage(e))}).finally(()=>{if(live)setLoading(false)});return()=>{live=false}},[selectedId]);
  if(selectedId) return <MigrationDetail id={selectedId} initial={selected} onBack={()=>{onClearSelection();void refresh()}} onChanged={async()=>{await refresh();await onRefresh()}}/>;
  return <><PageHeader eyebrow="OPERATIONS" title="Migrations" subtitle="Inspect job state, track outcomes, and resume eligible work." actions={<button className="btn subtle" onClick={()=>void refresh()}><RefreshCw size={16}/> Refresh</button>}/>{error&&<ErrorBanner message={error} onClose={()=>setError('')}/>}<section className="panel"><div className="list-toolbar"><div className="filter-pills">{['ALL','RUNNING','PAUSED','COMPLETED','FAILED','CANCELLED'].map(s=><button key={s} className={filter===s?'selected':''} onClick={()=>setFilter(s)}>{lower(s)}</button>)}</div><span>{jobs?.total ?? 0} jobs</span></div>{loading&&!jobs?<div className="panel-loading"><LoaderCircle className="spin"/> Loading jobs…</div>:jobs?.items.length?<div className="table-wrap"><table><thead><tr><th>Migration</th><th>Status</th><th>Analysis</th><th>Progress</th><th>Invited</th><th>Created</th><th></th></tr></thead><tbody>{jobs.items.map(j=><tr key={j.id}><td><div className="job-route"><strong>{j.source_title}</strong><ArrowRight size={13}/><strong>{j.destination_title}</strong></div><small>{j.id}</small></td><td><Badge value={j.status}/></td><td><Badge value={j.analysis_state}/></td><td><div className="table-progress"><span><i style={{width:`${j.percent_complete}%`}}/></span><small>{Math.round(j.percent_complete)}%</small></div></td><td>{fmt(j.successful_count)}</td><td>{dateFmt(j.created_at)}</td><td><button className="btn subtle small-btn" onClick={()=>onOpenJob(j.id)}>Details <ChevronRight size={14}/></button></td></tr>)}</tbody></table></div>:<EmptyState icon={FileClock} title="No jobs yet" description="Create a migration to see live progress and the final report."/>}</section></>;
}

function MigrationDetail({id,initial,onBack,onChanged}: {id:string;initial:Job|null;onBack:()=>void;onChanged:()=>Promise<void>}) {
  const [job,setJob]=useState<Job|null>(initial); const [events,setEvents]=useState<MigrationEvent[]>(initial?.recent_events||[]); const [members,setMembers]=useState<Member[]>([]);const [loading,setLoading]=useState(!initial);const [actionBusy,setActionBusy]=useState(false);const [error,setError]=useState('');const [memberFilter,setMemberFilter]=useState('');
  const streamController=useRef<AbortController|null>(null);
  const [, setClockTick] = useState(0);
  useEffect(() => {
    const timer = window.setInterval(() => setClockTick(v => v + 1), 1000);
    return () => window.clearInterval(timer);
  }, []);
  
  const load=useCallback(async()=>{try{const j=await api<Job>(`/migrations/${id}`);setJob(j);setEvents(j.recent_events||[]);const ev=await api<MigrationEvent[]>(`/migrations/${id}/events?after_seq=0&limit=50`);setEvents(ev);}catch(e){setError(errMessage(e))}finally{setLoading(false)}},[id]);
  useEffect(()=>{void load();const t=window.setInterval(()=>void load(),5000);return()=>window.clearInterval(t)},[load]);
  useEffect(()=>{streamController.current?.abort();const controller=new AbortController();streamController.current=controller;openMigrationStream(id,(type,data)=>{if(type==='migration.progress'){setJob(prev=>prev?{...prev,status:data.status||prev.status,pause_reason:data.pause_reason??prev.pause_reason,pause_requested:Boolean(data.pause_requested),cancellation_requested:Boolean(data.cancellation_requested),processed_members:data.processed??prev.processed_members,total_members:data.total??prev.total_members,successful_count:data.successful??prev.successful_count,skipped_count:data.skipped??prev.skipped_count,failed_count:data.failed??prev.failed_count,rate_limited_count:data.rate_limited??prev.rate_limited_count,percent_complete:data.percent??prev.percent_complete,rate_limit_remaining_seconds:data.rate_limit_remaining_seconds??prev.rate_limit_remaining_seconds,current_member:('current_member' in data ? data.current_member : prev.current_member),error_message:data.error_message??prev.error_message}:prev);}
      if(type!=='migration.progress'){setEvents(prev=>{const seq=data.seq; if(seq && prev.some(ev=>ev.seq===seq))return prev;const item={id:String(seq||`${Date.now()}`),seq:Number(seq||0),event_type:type,message:data.message||type,status:data.status||null,reason:data.reason||null,error_code:data.error_code||null,metadata:data.metadata||null,member_id:data.member_id||null,created_at:data.created_at||new Date().toISOString()} as MigrationEvent;return [...prev,item].slice(-100)});void load();}
    },err=>{if(err instanceof ApiError&&err.status===401){setError('Session expired. Sign out and sign in again.');controller.abort();}},controller.signal);return()=>controller.abort()},[id,load]);
  async function action(name:'start'|'pause'|'resume'|'cancel'|'retry'|'delete') {
    if(!job)return;
    if(name==='cancel'&&!window.confirm('Cancel this migration? Completed operations and history will remain.'))return;
    if(name==='delete'&&!window.confirm('Permanently delete this migration and its member/event records?'))return;
    if(name==='start'&&!window.confirm('Start sending invitations to selected eligible members where Telegram permits it?'))return;
    setActionBusy(true);setError('');
    try { let result:Job|undefined; if(name==='delete'){await api(`/migrations/${id}`,{method:'DELETE'});onBack();return;} if(name==='retry')result=await api<Job>(`/migrations/${id}/retry`,{method:'POST'}); else {let body:BodyInit|undefined;if(name==='start')body=JSON.stringify({acknowledge_partial:job.analysis_state==='PARTIAL'});result=await api<Job>(`/migrations/${id}/${name}`,{method:'POST',body:body??JSON.stringify({})});}
      if(result){setJob(result);if(name==='retry'){onBack();return;}}await load();await onChanged();
    }catch(e){setError(errMessage(e));}finally{setActionBusy(false)}
  }
  useEffect(()=>{let active=true;async function loadMem(){try{const p=new URLSearchParams({limit:'50',offset:'0'});if(memberFilter)p.set('processing_status',memberFilter);const res=await api<Page<Member>>(`/migrations/${id}/members?${p}`);if(active)setMembers(res.items)}catch{} }void loadMem();return()=>{active=false}},[id,memberFilter,job?.processed_members]);
  if(loading&&!job)return <div className="panel-loading"><LoaderCircle className="spin"/> Loading migration…</div>;
  if(!job)return <><PageHeader title="Migration details" subtitle="Job not found." actions={<button className="btn subtle" onClick={onBack}><ArrowLeft size={16}/> Back</button>}/>{error&&<ErrorBanner message={error}/>}</>;
  const isRateLimited=job.status==='PAUSED'&&job.pause_reason==='RATE_LIMIT';
  const cooldownLeft = job.rate_limited_until ? Math.max(0, Math.ceil((new Date(job.rate_limited_until).getTime() - Date.now()) / 1000)) : Math.max(0, job.rate_limit_remaining_seconds);
  return <><PageHeader eyebrow="MIGRATION DETAILS" title={`${job.source.title} → ${job.destination.title}`} subtitle={`Created ${dateFmt(job.created_at)} · ID ${job.id}`} actions={<button className="btn subtle" onClick={onBack}><ArrowLeft size={16}/> All migrations</button>}/>{error&&<ErrorBanner message={error} onClose={()=>setError('')}/>}<section className="panel live-panel"><div className="live-heading"><div><div className="live-tags"><Badge value={job.status}/><Badge value={job.analysis_state}/>{job.pause_reason&&<Badge value={job.pause_reason}/>}</div><h2>{job.status==='COMPLETED'?'Migration completed':job.status==='FAILED'?'Migration failed':job.status==='PAUSED'?'Migration paused':job.status==='CANCELLED'?'Migration cancelled':'Migration progress'}</h2><p>Invitations are only attempted where Telegram permits them. Confirmed invitations are counted only after membership confirmation.</p></div><button className="icon-btn" aria-label="Refresh job" onClick={()=>void load()}><RefreshCw size={17}/></button></div><div className="live-progress-wrap"><div className="donut" style={{'--progress':`${Math.min(100,job.percent_complete)}%`} as CSSProperties}><div><strong>{Math.round(job.percent_complete)}%</strong><small>processed</small></div></div><div className="live-progress-copy"><div className="live-progress-top"><strong>{fmt(job.processed_members)} <span>/ {fmt(job.total_members)} members</span></strong><span>{fmt(job.remaining_members)} remaining</span></div><div className="progress-track big"><span style={{width:`${Math.min(100,job.percent_complete)}%`}}/></div><div className="live-stat-grid"><div><i className="dot green-dot"/><span>Confirmed invited</span><strong>{fmt(job.successful_count)}</strong></div><div><i className="dot amber-dot"/><span>Skipped</span><strong>{fmt(job.skipped_count)}</strong></div><div><i className="dot red-dot"/><span>Failed</span><strong>{fmt(job.failed_count)}</strong></div><div><i className="dot blue-dot"/><span>Rate-limit events</span><strong>{fmt(job.rate_limited_count)}</strong></div></div></div></div>
    {isRateLimited&&<div className="rate-limit-banner"><Clock3 size={20}/><div><strong>Telegram cooldown in progress</strong><p>Migration is paused according to Telegram's server-provided cooldown. The app will not bypass or shorten it.</p></div><div className="cooldown-number">{fmt(Math.floor(cooldownLeft/60)).padStart(2,'0')}:{fmt(cooldownLeft%60).padStart(2,'0')}</div></div>}
    {job.current_member&&<div className="current-member-bar"><span className="member-avatar"><UserRound size={18}/></span><div><small>Current member</small><strong>{job.current_member.label || job.current_member.id}</strong></div><Badge value={job.status==='RUNNING'?'PROCESSING':job.status}/></div>}
    {job.error_message&&<ErrorBanner message={job.error_message}/>}<div className="live-actions">{job.allowed_actions.includes('start')&&<button className="btn primary" disabled={actionBusy} onClick={()=>void action('start')}><Play size={16}/> Start</button>}{job.allowed_actions.includes('pause')&&<button className="btn warning" disabled={actionBusy} onClick={()=>void action('pause')}><Pause size={16}/> Pause</button>}{job.allowed_actions.includes('resume')&&<button className="btn primary" disabled={actionBusy||isRateLimited} onClick={()=>void action('resume')}><Play size={16}/> Resume</button>}{job.allowed_actions.includes('cancel')&&<button className="btn danger" disabled={actionBusy} onClick={()=>void action('cancel')}><Square size={15}/> Cancel</button>}{job.allowed_actions.includes('retry')&&<button className="btn secondary" disabled={actionBusy} onClick={()=>void action('retry')}><RefreshCw size={15}/> Retry unfinished members</button>}{job.allowed_actions.includes('delete')&&<button className="btn ghost-danger" disabled={actionBusy} onClick={()=>void action('delete')}><Trash2 size={15}/> Delete history</button>}{actionBusy&&<LoaderCircle className="spin" size={18}/>}</div></section>
    <div className="detail-two-col"><section className="panel"><PanelTitle title="Member results" subtitle="Recent member records" action={<select value={memberFilter} onChange={e=>setMemberFilter(e.target.value)}><option value="">All states</option><option value="SUCCESS">Success</option><option value="SKIPPED">Skipped</option><option value="FAILED">Failed</option><option value="ALREADY_MEMBER">Already member</option><option value="RATE_LIMITED">Rate limited</option></select>}/>{members.length?<div className="member-result-list">{members.slice(0,12).map(m=><div className="member-result" key={m.id}><span className="member-avatar">{(m.display_name||'?').slice(0,1).toUpperCase()}</span><div><strong>{m.display_name}</strong><small>{m.username?`@${m.username}`:`ID ${m.telegram_user_id}`}</small></div><div className="member-result-status"><Badge value={m.state}/><small>{m.failure_reason||m.reason||m.outcome||''}</small></div></div>)}</div>:<EmptyState icon={Users} title="No member records" description="Member results will appear here after analysis."/>}</section>
      <section className="panel"><PanelTitle title="Event timeline" subtitle="Persisted events and live progress" action={<button className="icon-btn" onClick={()=>void load()} aria-label="Refresh events"><RefreshCw size={15}/></button>}/><div className="event-timeline">{events.slice(-20).reverse().map((ev,i)=><div className="timeline-item" key={`${ev.seq}-${ev.id}`}><span className={`timeline-marker ${statusTone(ev.status||ev.event_type)}`}/><div><strong>{ev.message||lower(ev.event_type)}</strong><p>{dateFmt(ev.created_at)}{ev.reason?` · ${ev.reason}`:''}</p>{ev.error_code&&<small className="error-code">{ev.error_code}</small>}</div></div>)}{!events.length&&<p className="muted">No events recorded yet.</p>}</div></section></div>
    <div className="compliance-note"><ShieldCheck size={18}/><span>{job.notes?.join(' ') || 'Only operations permitted by Telegram will be attempted. The displayed eligibility status is not a guarantee of invitation success.'}</span></div>
  </>;
}

function ChannelsPage({accounts,channels,onLoad}: {accounts:TelegramAccount[];channels:Channel[];onLoad:(id?:string)=>Promise<void>}) {
  const [accountId,setAccountId]=useState(accounts.find(a=>a.status==='ACTIVE')?.id||''); const [loading,setLoading]=useState(false);
  useEffect(()=>{if(accountId){setLoading(true);void onLoad(accountId).finally(()=>setLoading(false))}},[accountId]);
  return <><PageHeader eyebrow="TELEGRAM" title="Channels" subtitle="Channels and groups available to connected Telegram accounts." actions={<button className="btn subtle" disabled={!accountId||loading} onClick={()=>{setLoading(true);void onLoad(accountId).finally(()=>setLoading(false))}}><RefreshCw size={16} className={loading?'spin':''}/> Refresh list</button>}/><section className="panel"><div className="channel-page-toolbar"><label className="field-label compact">Account<select value={accountId} onChange={e=>setAccountId(e.target.value)}><option value="">Choose account</option>{accounts.filter(a=>a.status==='ACTIVE').map(a=><option key={a.id} value={a.id}>{a.first_name || 'Telegram user'} {a.username?`(@${a.username})`:''}</option>)}</select></label><span>{channels.length} channels/groups</span></div>{!accounts.length?<EmptyState icon={PlugZap} title="Connect Telegram first" description="You need an authorized account to fetch your channels."/>:loading&&!channels.length?<div className="panel-loading"><LoaderCircle className="spin"/> Fetching channels from Telegram…</div>:channels.length?<div className="channel-grid">{channels.map(c=><div className="channel-card" key={c.id}><div className="channel-card-top"><ChannelAvatar title={c.title}/><Badge value={c.kind}/></div><h3>{c.title}</h3><p>{c.username?`@${c.username}`:'No public username'}</p><div className="channel-card-stats"><span><Users size={15}/>{c.participants_count===null?'Unknown':fmt(c.participants_count)}</span><span><ShieldCheck size={15}/>{c.can_invite_users?'Invite permitted':'No invite permission detected'}</span></div><div className="channel-card-foot"><span>Refreshed {dateFmt(c.refreshed_at)}</span><Badge value={c.is_admin?'ADMIN':'MEMBER'}/></div></div>)}</div>:<EmptyState icon={Radio} title="No channels found" description="Refresh the channel list. Telegram only returns channels available to the connected account."/>}</section></>;
}

function MembersPage({accounts,onLoad,channels}: {accounts:TelegramAccount[];onLoad:(id?:string)=>Promise<void>;channels:Channel[]}) {
  const [accountId,setAccountId]=useState(accounts.find(a=>a.status==='ACTIVE')?.id||'');const [channelId,setChannelId]=useState('');const [data,setData]=useState<any>(null);const [loading,setLoading]=useState(false);const [limit,setLimit]=useState('200');const [error,setError]=useState('');
  const thisAccountChannels=channels;
  async function loadMembers(){if(!channelId){setError('Select a channel first.');return}setLoading(true);setError('');try{const result=await api<any>(`/telegram/channels/${channelId}/members?limit=${limit}`);setData(result)}catch(e){setError(errMessage(e))}finally{setLoading(false)}}
  useEffect(()=>{if(accountId)void onLoad(accountId)},[accountId]);
  useEffect(()=>{setChannelId('');setData(null)},[accountId]);
  return <><PageHeader eyebrow="DISCOVERY" title="Members" subtitle="View member information Telegram legitimately returns for channels you administer."/><section className="panel"><div className="member-discovery-tools"><label className="field-label compact">Telegram account<select value={accountId} onChange={e=>setAccountId(e.target.value)}><option value="">Choose account</option>{accounts.filter(a=>a.status==='ACTIVE').map(a=><option key={a.id} value={a.id}>{a.first_name || 'Telegram user'} {a.username?`(@${a.username})`:''}</option>)}</select></label><label className="field-label compact">Channel<select value={channelId} onChange={e=>setChannelId(e.target.value)}><option value="">Select channel</option>{thisAccountChannels.map(c=><option key={c.id} value={c.id}>{c.title}</option>)}</select></label><label className="field-label compact">Fetch limit<select value={limit} onChange={e=>setLimit(e.target.value)}><option value="100">100</option><option value="200">200</option><option value="500">500</option><option value="1000">1,000</option></select></label><button className="btn primary" disabled={loading||!channelId} onClick={()=>void loadMembers()}>{loading?<LoaderCircle size={16} className="spin"/>:<Search size={16}/>} Fetch members</button></div>{error&&<ErrorBanner message={error}/>}{data&&<><div className="discovery-summary"><div><strong>{fmt(data.returned)}</strong><span>Members returned</span></div><div><strong>{data.reported_total===null?'Unknown':fmt(data.reported_total)}</strong><span>Telegram reported total</span></div><div><Badge value={data.complete?'COMPLETED':'PARTIAL'}/><span>Listing status</span></div></div>{data.notes?.map((n:string)=><div className="note-line" key={n}><AlertCircle size={15}/>{n}</div>)}<div className="table-wrap"><table><thead><tr><th>Member</th><th>Role</th><th>Type</th><th>Flags</th></tr></thead><tbody>{data.members.map((m:any)=><tr key={m.telegram_user_id}><td><div className="member-person"><span className="member-avatar">{(m.display_name||'?')[0]}</span><span><strong>{m.display_name}</strong><small>{m.username?`@${m.username}`:`ID ${m.telegram_user_id}`}</small></span></div></td><td>{lower(m.role)}</td><td>{m.is_bot?'Bot':m.is_deleted?'Deleted account':'User'}</td><td>{m.is_restricted?<Badge value="RESTRICTED"/>:<Badge value="UNKNOWN"/>}</td></tr>)}</tbody></table></div></>}</section></>;
}

function HistoryPage({jobs,onOpenJob,onRefresh}: {jobs:Page<JobItem>|null;onOpenJob:(id:string)=>void;onRefresh:()=>Promise<void>}) {
  useEffect(()=>{void onRefresh()},[]);
  const completed=jobs?.items.filter(j=>['COMPLETED','FAILED','CANCELLED'].includes(j.status))||[];
  return <><PageHeader eyebrow="REPORTING" title="Migration history" subtitle="Review completed, cancelled, and failed jobs." actions={<button className="btn subtle" onClick={()=>void onRefresh()}><RefreshCw size={16}/> Refresh</button>}/><div className="history-cards">{completed.length?completed.map(j=><button className="history-card" key={j.id} onClick={()=>onOpenJob(j.id)}><div className="history-route"><div><ChannelAvatar title={j.source_title}/><strong>{j.source_title}</strong></div><ArrowRight size={17}/><div><ChannelAvatar title={j.destination_title}/><strong>{j.destination_title}</strong></div></div><div className="history-meta"><Badge value={j.status}/><span>{dateFmt(j.completed_at||j.created_at)}</span></div><div className="history-progress"><span style={{width:`${j.percent_complete}%`}}/></div><div className="history-stats"><span>{fmt(j.processed_members)} / {fmt(j.total_members)} processed</span><strong className="green-text">{fmt(j.successful_count)} invited</strong></div><span className="history-open">View report <ArrowRight size={14}/></span></button>):<EmptyState icon={History} title="No completed jobs yet" description="Completed or stopped jobs will appear in this view."/>}</div></>;
}

function SettingsPage({accounts,onChanged,onConnect,onError}: {accounts:TelegramAccount[];onChanged:()=>Promise<void>;onConnect:()=>Promise<void>;onError:(s:string)=>void}) {
  const [phone,setPhone]=useState('');const [loginId,setLoginId]=useState('');const [code,setCode]=useState('');const [password,setPassword]=useState('');const [masked,setMasked]=useState('');const [phase,setPhase]=useState<'phone'|'code'|'2fa'>('phone');const [busy,setBusy]=useState(false);const [error,setError]=useState('');const [notice,setNotice]=useState('');
  async function begin(e:FormEvent){e.preventDefault();setBusy(true);setError('');setNotice('');try{const res=await api<{login_id:string;phone_masked:string;delivery:string;expires_in_seconds:number}>('/auth/telegram/start',{method:'POST',body:JSON.stringify({phone})});setLoginId(res.login_id);setMasked(res.phone_masked);setNotice(`Code requested via Telegram (${res.delivery}). It expires in ${Math.round(res.expires_in_seconds/60)} minutes.`);setPhase('code')}catch(e){setError(errMessage(e))}finally{setBusy(false)}}
  async function verify(e:FormEvent){e.preventDefault();setBusy(true);setError('');try{const res=await api<any>('/auth/telegram/verify',{method:'POST',body:JSON.stringify({login_id:loginId,code})});if(res.status==='two_factor_required'){setPhase('2fa');setCode('');setNotice('Telegram requires your account two-step verification password.');}else{setNotice(`Connected ${res.account?.username?`@${res.account.username}`:'Telegram account'} successfully.`);setPhase('phone');setPhone('');setCode('');setLoginId('');setPassword('');await onChanged();}}catch(e){setError(errMessage(e))}finally{setBusy(false)}}
  async function verify2fa(e:FormEvent){e.preventDefault();setBusy(true);setError('');try{const res=await api<any>('/auth/telegram/2fa',{method:'POST',body:JSON.stringify({login_id:loginId,password})});setNotice(`Connected ${res.account?.username?`@${res.account.username}`:'Telegram account'} successfully.`);setPhase('phone');setPhone('');setCode('');setPassword('');setLoginId('');await onChanged();}catch(e){setError(errMessage(e))}finally{setBusy(false)}}
  async function disconnect(id:string){if(!window.confirm('Disconnect this Telegram account and remove the stored session?'))return;try{await api(`/telegram/accounts/${id}`,{method:'DELETE'});await onChanged();setNotice('Telegram account disconnected.')}catch(e){setError(errMessage(e))}}
  return <><PageHeader eyebrow="PREFERENCES" title="Settings" subtitle="Manage connected Telegram accounts and workspace preferences."/><div className="settings-grid"><section className="panel"><PanelTitle title="Connected Telegram accounts" subtitle="Sessions are encrypted server-side."/>{accounts.length?accounts.map(a=><div className="settings-account" key={a.id}><span className="profile-avatar blue-avatar"><Send size={17}/></span><div><strong>{a.first_name||'Telegram user'} {a.username?`(@${a.username})`:''}</strong><span>Telegram ID {a.telegram_user_id}</span><small>Last used {dateFmt(a.last_used_at)}</small></div><Badge value={a.status}/><button className="btn ghost-danger small-btn" onClick={()=>void disconnect(a.id)}><Trash2 size={14}/> Disconnect</button></div>):<EmptyState icon={Smartphone} title="No accounts connected" description="Connect a Telegram account to fetch channels you administer."/>}
      <div className="section-divider"/><div className="connect-form-head"><h3>Connect Telegram account</h3><p>Use the official MTProto phone → code → optional 2FA flow. Credentials are sent only to the backend for verification.</p></div>{error&&<ErrorBanner message={error} onClose={()=>setError('')}/>}{notice&&<div className="success-banner"><CheckCircle2 size={17}/>{notice}</div>}
      {phase==='phone'&&<form className="stack-form" onSubmit={begin}><label>Phone number<input type="tel" autoComplete="tel" value={phone} onChange={e=>setPhone(e.target.value)} placeholder="+92 300 1234567" required/></label><p className="field-help">Telegram sends the login code to your Telegram app or another authorized delivery method. It is not sent to this website by SMS unless Telegram chooses that route.</p><button className="btn primary" disabled={busy}>{busy?<LoaderCircle className="spin" size={16}/>:<Send size={16}/>} Send login code</button></form>}
      {phase==='code'&&<form className="stack-form" onSubmit={verify}><p className="auth-code-note">Enter the code sent for <strong>{masked}</strong>.</p><label>Login code<input autoComplete="one-time-code" inputMode="numeric" value={code} onChange={e=>setCode(e.target.value)} required/></label><div className="form-actions"><button className="btn subtle" type="button" onClick={()=>setPhase('phone')}>Change phone</button><button className="btn primary" disabled={busy}>{busy?<LoaderCircle className="spin" size={16}/>:<Check size={16}/>} Verify code</button></div></form>}
      {phase==='2fa'&&<form className="stack-form" onSubmit={verify2fa}><label>Telegram two-step verification password<input type="password" autoComplete="current-password" value={password} onChange={e=>setPassword(e.target.value)} required/></label><p className="field-help">Password is sent to the backend for Telegram verification and is not saved. Do not share it with anyone.</p><div className="form-actions"><button className="btn subtle" type="button" onClick={()=>setPhase('phone')}>Cancel</button><button className="btn primary" disabled={busy}>{busy?<LoaderCircle className="spin" size={16}/>:<ShieldCheck size={16}/>} Verify and connect</button></div></form>}
    </section><section className="panel"><PanelTitle title="Security & application" subtitle="Current workspace settings"/><div className="settings-feature"><span><ShieldCheck size={18}/></span><div><strong>Encrypted Telegram sessions</strong><p>Telegram session strings are stored encrypted on the backend and are not exposed to browser storage.</p></div></div><div className="settings-feature"><span><Activity size={18}/></span><div><strong>Background processing</strong><p>Redis/ARQ workers process migration jobs. The UI receives state from the backend's event stream.</p></div></div><div className="settings-feature"><span><Clock3 size={18}/></span><div><strong>Telegram rate limits</strong><p>Cooldowns cannot be skipped. When Telegram asks for a wait, the migration must honor it.</p></div></div><div className="settings-feature"><span><Smartphone size={18}/></span><div><strong>Installable PWA</strong><p>Use your browser's “Install app” / “Add to Home Screen” action on supported devices.</p></div></div><a className="docs-link" href="/docs" target="_blank" rel="noreferrer">Open API documentation <ExternalLink size={14}/></a></section></div></>;
}
