const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const os = require('node:os');
const { URL } = require('node:url');
const { DatabaseSync } = require('node:sqlite');
const {AppError,badRequest,forbidden,notFound,conflict,tooLarge}=require('./lib/app-error');
const {safeDecodeURIComponent,parseCookies:parseCookieHeader,imageMimeFromMagic,deriveBackupKey}=require('./lib/security');
const {shaFile,listFiles,writeArchive,inspectArchive}=require('./lib/archive');
const {validateProductImage,MAX_PRODUCT_IMAGES,MAX_PRODUCT_IMAGE_BYTES}=require('./lib/product-images');
const {validatePassword,validateUsername,validateFullName,validateDepartment,normalizeOrderId,validateOrderId,strictNeedBy,needByIso,cleanText,cleanQty,cleanItem}=require('./lib/validators');
const {parsePermissionOverrides,sanitizePermissionOverrides,effectivePermissions:rbacEffectivePermissions,hasPermission:rbacHasPermission}=require('./lib/rbac');
const {runWithRequestContext,getRequestId}=require('./src/core/request-context');
const {normalizeApiPayload}=require('./src/core/api-envelope');
const {ORDER_STAGES,DEFECT_CATEGORIES,isValidStage,isEarlyStage,itemStageForOrderStage,decideStationScan}=require('./src/domain/workflow');
const {nextQuickAction,visualStage}=require('./src/domain/quick-workflow');
const {toMilliUnits,fromMilliUnits,roundDisplay,validUnit}=require('./src/domain/inventory-units');
const {createLoginGuard}=require('./src/modules/auth/login-guard');
const {createImportBatchRepository}=require('./src/modules/import/import-batches');
const {createInventoryService}=require('./src/modules/inventory/inventory-service');
const {runVersionedMigrations}=require('./src/database/migration-runner');
const chatMigration20=require('./src/database/migrations/020-chat-partners');
const {createOperationsService}=require('./src/modules/operations/operations-service');
const {createMonthSnapshotService}=require('./src/modules/sales/month-snapshots');
const {createQuickScanService}=require('./src/modules/workflow/quick-scan-service');
const {createChatService}=require('./src/modules/chat/chat-service');
const {createChatMobileService}=require('./src/modules/chat/chat-mobile-service');
const {createChatWebPush}=require('./src/modules/chat/chat-web-push');
const {createChatGroups}=require('./src/modules/chat/chat-groups');
const {createChatLifecycle}=require('./src/modules/chat/chat-lifecycle');
const {createMultiSourceIntake}=require('./src/modules/import/multi-source-intake');
const {createAloraEtsySync}=require('./src/modules/etsy-sync/alora-etsy-sync');
const {createEnterprise}=require('./src/modules/enterprise/routes');
const {computeParcel}=require('./src/modules/etsy-sync/packing');

let realtime=null;
const ROOT = __dirname;
const PUBLIC = path.join(ROOT, 'public');
const DATA = path.join(ROOT, 'data'); // legacy data location used by V14.2.1 and older local installs
const {loadConfiguration}=require('./lib/config');
const config=loadConfiguration(ROOT);
const EXTERNAL_CONFIG_PATH=config.externalConfig;
const APP_VERSION=require('./package.json').version;
const SCHEMA_VERSION=33;
const PERSIST_ROOT=config.persistRoot,DB_PATH=config.dbPath;
// Fail closed before any migration, import or SQLite creation on Windows production.
require('./lib/production-guard').enforceWindowsProduction({
  env:process.env,platform:process.platform,nodeVersion:process.versions.node,
  root:ROOT,configPath:EXTERNAL_CONFIG_PATH,dbPath:DB_PATH
});
fs.mkdirSync(DATA,{recursive:true});
fs.mkdirSync(PERSIST_ROOT,{recursive:true});
fs.mkdirSync(path.dirname(DB_PATH),{recursive:true});
const LEGACY_DATA_ROOT = path.resolve(process.env.ALORA_LEGACY_DATA_ROOT || DATA);
fs.mkdirSync(PERSIST_ROOT, { recursive: true });
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
function samePath(a,b){ try{return path.resolve(a).toLowerCase()===path.resolve(b).toLowerCase()}catch{return false} }
function copyTreeMerge(src,dst){
  if(!fs.existsSync(src))return;
  fs.mkdirSync(dst,{recursive:true});
  for(const ent of fs.readdirSync(src,{withFileTypes:true})){
    const from=path.join(src,ent.name),to=path.join(dst,ent.name);
    if(ent.isDirectory())copyTreeMerge(from,to);
    else if(ent.isFile()&&!fs.existsSync(to))fs.copyFileSync(from,to);
  }
}
function migrateLegacyPersistentData(){
  if(samePath(PERSIST_ROOT,LEGACY_DATA_ROOT))return {migrated:false,reason:'same-root'};
  const legacyDb=path.join(LEGACY_DATA_ROOT,'3alora.sqlite');
  if(fs.existsSync(DB_PATH))return {migrated:false,reason:'target-exists'};
  if(!fs.existsSync(legacyDb))return {migrated:false,reason:'no-legacy-db'};
  const stage=fs.mkdtempSync(path.join(PERSIST_ROOT,'.legacy-import-'));
  let tdb=null;
  try{
    const stagedDb=path.join(stage,'3alora.sqlite');
    // SQLite reads the committed WAL into a consistent snapshot; do not race by
    // copying live DB/WAL files separately.
    tdb=new DatabaseSync(legacyDb,{readOnly:true});
    tdb.exec('PRAGMA busy_timeout=5000');
    tdb.exec("VACUUM INTO '"+stagedDb.replace(/'/g,"''")+"'");
    tdb.close();tdb=new DatabaseSync(stagedDb);
    tdb.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA wal_checkpoint(TRUNCATE);');
    const check=tdb.prepare('PRAGMA integrity_check').get();
    const detail=String(Object.values(check||{})[0]||'');
    if(detail.toLowerCase()!=='ok')throw new Error(`Legacy database integrity_check failed: ${detail}`);
    tdb.close();tdb=null;
    const tmp=DB_PATH+`.import-${process.pid}-${Date.now()}.tmp`;
    fs.copyFileSync(stagedDb,tmp);
    copyTreeMerge(path.join(LEGACY_DATA_ROOT,'evidence'),path.join(path.dirname(DB_PATH),'evidence'));
    copyTreeMerge(path.join(LEGACY_DATA_ROOT,'backups'),path.join(path.dirname(DB_PATH),'backups'));
    copyTreeMerge(path.join(LEGACY_DATA_ROOT,'logs'),path.join(path.dirname(DB_PATH),'logs'));
    fs.renameSync(tmp,DB_PATH);
    const marker=path.join(PERSIST_ROOT,'MIGRATED_FROM_LEGACY_DATA.txt');
    fs.writeFileSync(marker,`Migrated at: ${new Date().toISOString()}\nFrom: ${LEGACY_DATA_ROOT}\nTo: ${PERSIST_ROOT}\nSource code version: ${APP_VERSION}\n`,'utf8');
    console.log(`[DATA] Migrated legacy persistent data from ${LEGACY_DATA_ROOT} to ${PERSIST_ROOT}`);
    return {migrated:true,from:LEGACY_DATA_ROOT,to:PERSIST_ROOT};
  }finally{
    try{tdb?.close()}catch{}
    try{fs.rmSync(stage,{recursive:true,force:true})}catch{}
  }
}
const LEGACY_DATA_MIGRATION = migrateLegacyPersistentData();
const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const NODE_ENV = process.env.NODE_ENV || 'development';
const IS_PROD = NODE_ENV === 'production';
const TRUST_PROXY = process.env.TRUST_PROXY === '1';
const FORCE_HTTPS = process.env.FORCE_HTTPS === '1';
const PUBLIC_BASE_URL = String(process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');
const SESSION_HOURS = Math.max(1, Number(process.env.SESSION_HOURS || 12));
const GLOBAL_RATE_LIMIT_PER_MIN = Math.max(60, Number(process.env.GLOBAL_RATE_LIMIT_PER_MIN || 300));
const MAX_SESSIONS_PER_USER = Math.max(1, Number(process.env.MAX_SESSIONS_PER_USER || 8));
const LOGIN_MAX_FAILURES = Math.max(3, Number(process.env.LOGIN_MAX_FAILURES || 10));
const LOGIN_LOCK_MINUTES = Math.max(1, Number(process.env.LOGIN_LOCK_MINUTES || 5));
const SCAN_DEBOUNCE_SECONDS = 5;
const BACKUP_DIR = process.env.BACKUP_DIR || path.join(path.dirname(DB_PATH), 'backups');
const BACKUP_KEEP = Math.max(3, Number(process.env.BACKUP_KEEP || 30));
const BACKUP_INTERVAL_HOURS = Math.min(596,Math.max(1, Number(process.env.BACKUP_INTERVAL_HOURS || 24))); // Node timers are 32-bit; clamp to avoid 1ms overflow loops.
const OFFSITE_BACKUP_DIR = String(process.env.OFFSITE_BACKUP_DIR||'').trim();
const BACKUP_ENCRYPTION_KEY = String(process.env.BACKUP_ENCRYPTION_KEY||'');
fs.mkdirSync(BACKUP_DIR, { recursive: true }); if(OFFSITE_BACKUP_DIR)fs.mkdirSync(OFFSITE_BACKUP_DIR,{recursive:true});
const LOG_DIR=process.env.LOG_DIR||path.join(path.dirname(DB_PATH),'logs');fs.mkdirSync(LOG_DIR,{recursive:true});const LOG_FILE=path.join(LOG_DIR,'server.jsonl');const LOG_MAX_BYTES=Math.max(1_000_000,Number(process.env.LOG_MAX_BYTES||10_000_000));const LOG_KEEP=Math.min(20,Math.max(2,Number(process.env.LOG_KEEP||5)));
const EVIDENCE_DIR=process.env.EVIDENCE_DIR||path.join(path.dirname(DB_PATH),'evidence');fs.mkdirSync(EVIDENCE_DIR,{recursive:true});
const EVIDENCE_MAX_FILE_BYTES=Math.max(256000,Number(process.env.EVIDENCE_MAX_FILE_BYTES||3_000_000));
const EVIDENCE_MAX_FILES_PER_ORDER=Math.max(1,Number(process.env.EVIDENCE_MAX_FILES_PER_ORDER||30));
const EVIDENCE_MAX_BYTES_PER_ORDER=Math.max(EVIDENCE_MAX_FILE_BYTES,Number(process.env.EVIDENCE_MAX_BYTES_PER_ORDER||75_000_000));
const OFFSITE_BACKUP_KEEP=Math.max(3,Number(process.env.OFFSITE_BACKUP_KEEP||BACKUP_KEEP));
const RESTORE_MARKER=path.join(path.dirname(DB_PATH),'RESTORE_ON_START.json');
let RESTORE_APPLIED=false;
function decryptBackupEarly(src){
  if(!src.endsWith('.enc'))return {file:src,cleanup:false};
  if(!BACKUP_ENCRYPTION_KEY)throw new Error('BACKUP_ENCRYPTION_KEY is required to restore encrypted backup');
  const raw=fs.readFileSync(src);
  if(raw.subarray(0,9).toString()==='ALORAENC2'){
    const salt=raw.subarray(9,25),iv=raw.subarray(25,37),tag=raw.subarray(37,53),cipher=raw.subarray(53),key=deriveBackupKey(BACKUP_ENCRYPTION_KEY,salt),dec=crypto.createDecipheriv('aes-256-gcm',key,iv);dec.setAuthTag(tag);
    const tmp=path.join(path.dirname(DB_PATH),`.restore-${process.pid}-${Date.now()}.alorabak`);fs.writeFileSync(tmp,Buffer.concat([dec.update(cipher),dec.final()]));return {file:tmp,cleanup:true};
  }
  if(raw.subarray(0,8).toString()==='ALORAENC'){
    const iv=raw.subarray(8,20),tag=raw.subarray(20,36),cipher=raw.subarray(36),key=crypto.createHash('sha256').update(BACKUP_ENCRYPTION_KEY).digest(),dec=crypto.createDecipheriv('aes-256-gcm',key,iv);dec.setAuthTag(tag);
    const tmp=path.join(path.dirname(DB_PATH),`.restore-${process.pid}-${Date.now()}.sqlite`);fs.writeFileSync(tmp,Buffer.concat([dec.update(cipher),dec.final()]));return {file:tmp,cleanup:true};
  }
  throw new Error('Invalid encrypted backup format');
}
function restoreOnStartup(){
  if(!fs.existsSync(RESTORE_MARKER))return;
  let dec=null,stage=null;
  try{
    const marker=JSON.parse(fs.readFileSync(RESTORE_MARKER,'utf8')),src=path.join(BACKUP_DIR,path.basename(String(marker.name||'')));if(!fs.existsSync(src))throw new Error('Backup restore source missing');
    dec=decryptBackupEarly(src);
    const isBundle=dec.file.endsWith('.alorabak')||String(marker.name||'').includes('.alorabak');
    if(isBundle){
      stage=fs.mkdtempSync(path.join(path.dirname(DB_PATH),'.restore-stage-')); inspectArchive(dec.file,{extractDir:stage});
      const dbSrc=path.join(stage,'database','3alora.sqlite');if(!fs.existsSync(dbSrc))throw new Error('Backup bundle không có database.');
      let tdb;try{tdb=new DatabaseSync(dbSrc,{readOnly:true});const r=tdb.prepare('PRAGMA integrity_check').get();if(String(Object.values(r||{})[0]||'').toLowerCase()!=='ok')throw new Error('Database integrity check failed');}finally{try{tdb?.close()}catch{}}
      // Stage both database and evidence first, then swap with rollback protection.
      const stamp=Date.now(),dbTmp=DB_PATH+`.restore-${stamp}.tmp`,dbOld=DB_PATH+`.restore-${stamp}.old`,evSrc=path.join(stage,'evidence'),evTmp=EVIDENCE_DIR+`.restore-${stamp}.tmp`,evOld=EVIDENCE_DIR+`.restore-${stamp}.old`;
      fs.copyFileSync(dbSrc,dbTmp); if(fs.existsSync(evSrc))fs.cpSync(evSrc,evTmp,{recursive:true});else fs.mkdirSync(evTmp,{recursive:true});
      let dbSwapped=false,evSwapped=false;
      try{
        for(const suffix of ['-wal','-shm'])try{fs.rmSync(DB_PATH+suffix,{force:true})}catch{}
        if(fs.existsSync(DB_PATH))fs.renameSync(DB_PATH,dbOld); fs.renameSync(dbTmp,DB_PATH); dbSwapped=true;
        if(fs.existsSync(EVIDENCE_DIR))fs.renameSync(EVIDENCE_DIR,evOld); fs.renameSync(evTmp,EVIDENCE_DIR); evSwapped=true;
        fs.rmSync(dbOld,{force:true});fs.rmSync(evOld,{recursive:true,force:true});
      }catch(swapError){
        try{if(evSwapped)fs.rmSync(EVIDENCE_DIR,{recursive:true,force:true});if(fs.existsSync(evOld))fs.renameSync(evOld,EVIDENCE_DIR);else fs.rmSync(evTmp,{recursive:true,force:true});}catch{}
        try{if(dbSwapped)fs.rmSync(DB_PATH,{force:true});if(fs.existsSync(dbOld))fs.renameSync(dbOld,DB_PATH);else fs.rmSync(dbTmp,{force:true});}catch{}
        throw new Error(`Restore swap failed and rollback was attempted: ${swapError.message}`);
      }
    }else{
      let tdb;try{tdb=new DatabaseSync(dec.file,{readOnly:true});const r=tdb.prepare('PRAGMA integrity_check').get();if(String(Object.values(r||{})[0]||'').toLowerCase()!=='ok')throw new Error('Database integrity check failed');}finally{try{tdb?.close()}catch{}}
      const stamp=Date.now(),dbTmp=DB_PATH+`.legacy-restore-${stamp}.tmp`,dbOld=DB_PATH+`.legacy-restore-${stamp}.old`;fs.copyFileSync(dec.file,dbTmp);
      try{for(const suffix of ['-wal','-shm'])try{fs.rmSync(DB_PATH+suffix,{force:true})}catch{};if(fs.existsSync(DB_PATH))fs.renameSync(DB_PATH,dbOld);fs.renameSync(dbTmp,DB_PATH);fs.rmSync(dbOld,{force:true});}catch(swapError){try{fs.rmSync(DB_PATH,{force:true});if(fs.existsSync(dbOld))fs.renameSync(dbOld,DB_PATH);fs.rmSync(dbTmp,{force:true})}catch{};throw swapError;}
    }
    fs.unlinkSync(RESTORE_MARKER);if(String(marker.name||'').includes('-pinned-'))fs.rmSync(src,{force:true});RESTORE_APPLIED=true;console.log(`[RESTORE] Restored safely from ${src}; sessions will be revoked.`);
  }catch(e){console.error('[RESTORE] Failed:',e.message);throw new Error('Restore failed; startup stopped: '+e.message)}finally{try{if(dec?.cleanup)fs.rmSync(dec.file,{force:true})}catch{};try{if(stage)fs.rmSync(stage,{recursive:true,force:true})}catch{}}
}
restoreOnStartup();

const db = new DatabaseSync(DB_PATH);
db.exec('PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000;');
if(RESTORE_APPLIED){for(const table of ['sessions','chat_mobile_sessions']){try{db.prepare('DELETE FROM '+table).run()}catch{}}}

const ROLE_STATIONS = {
  ADMIN: ['ORDER','PRODUCTION','QC','PACKING','SHIPPING','CSKH'],
  MANAGER: ['CSKH'],
  ORDER: ['ORDER'],
  PRODUCTION: ['PRODUCTION'],
  QC: ['QC'],
  PACKING: ['PACKING'],
  SHIPPING: ['SHIPPING'],
  WAREHOUSE: ['WAREHOUSE'],
  CSKH: ['CSKH']
};

const PERMISSION_CATALOG = Object.freeze({
  'dashboard.view': 'Xem tổng quan & chỉ số',
  'orders.view_role': 'Xem đơn thuộc phạm vi công việc',
  'orders.view_all': 'Xem toàn bộ đơn hàng',
  'orders.create': 'Tạo đơn mới',
  'orders.edit': 'Sửa toàn bộ thông tin đơn',
  'orders.delete': 'Xóa đơn vào thùng rác',
  'orders.view_deleted': 'Xem thùng rác',
  'orders.restore': 'Khôi phục đơn đã xóa',
  'orders.permanent_delete': 'Xóa vĩnh viễn đơn hàng',
  'orders.assign': 'Phân công người sản xuất',
  'orders.move_early': 'Chuyển công đoạn đầu (Đơn mới / Sẵn sàng / Đã phân công)',
  'orders.move_stage': 'Chuyển bất kỳ công đoạn',
  'orders.duplicate': 'Nhân bản đơn',
  'orders.export': 'Xuất CSV đơn hàng',
  'reports.view': 'Xem báo cáo vận hành',
  'sales.view': 'Xem báo cáo doanh số tháng, giá đơn và SKU bán chạy (nhạy cảm)',
  'sales.manage': 'Nhập và chỉnh giá bán / ngày bán cho báo cáo',
  'sales.close': 'Lưu bản chốt số liệu tháng (cần thêm sales.view)',
  'customers.view': 'Xem danh sách khách hàng',
  'audit.view': 'Xem lịch sử thao tác',
  'station.order': 'Dùng Station Kê đơn',
  'station.production': 'Dùng Station Sản xuất',
  'station.qc': 'Dùng Station Kiểm Tra',
  'station.packing': 'Dùng Station Đóng gói',
  'station.shipping': 'Dùng Station Xuất hàng',
  'qc.execute': 'Thực hiện Kiểm Tra PASS / FAIL',
  'tracking.update': 'Cập nhật Tracking',
  'import.manage': 'Nhập dữ liệu từ Google Sheets',
  'users.manage': 'Quản lý tài khoản & phân quyền',
  'users.delete': 'Xóa tài khoản (chỉ Admin)',
  'inventory.view': 'Xem kho vải & tồn kho',
  'inventory.manage': 'Tạo / sửa danh mục vải',
  'inventory.receive': 'Nhập vải vào kho',
  'inventory.issue': 'Quét Order & xuất vải theo định mức',
  'inventory.adjust': 'Điều chỉnh / hoàn vải',
  'inventory.import': 'Nhập cấu hình vải theo Order + SKU từ Google Sheets',
  'backup.manage': 'Tạo / tải / kiểm tra backup hệ thống'
});
const ADMIN_ONLY_PERMISSIONS = new Set(['users.manage','users.delete','backup.manage']);

const ROLE_PERMISSIONS = Object.freeze({
  ADMIN: Object.keys(PERMISSION_CATALOG),
  MANAGER: ['dashboard.view','orders.view_all','orders.export','reports.view','customers.view','audit.view'],
  ORDER: ['dashboard.view','orders.view_all','orders.create','orders.edit','orders.delete','orders.view_deleted','orders.restore','orders.assign','orders.move_early','orders.duplicate','orders.export','reports.view','customers.view','audit.view','station.order'],
  PRODUCTION: ['dashboard.view','orders.view_role','audit.view','station.production'],
  QC: ['dashboard.view','orders.view_role','audit.view','station.qc','qc.execute'],
  PACKING: ['dashboard.view','orders.view_role','audit.view','station.packing'],
  SHIPPING: ['dashboard.view','orders.view_role','audit.view','station.shipping','tracking.update'],
  WAREHOUSE: ['dashboard.view','orders.view_role','audit.view','inventory.view','inventory.receive','inventory.issue','inventory.import'],
  CSKH: ['dashboard.view','orders.view_all','orders.export','reports.view','customers.view','audit.view']
});

const STAGE_ORDER = ORDER_STAGES;

function nowIso(){ return new Date().toISOString(); }
function addHoursIso(h){ return new Date(Date.now()+h*3600_000).toISOString(); }
function randomToken(bytes=32){ return crypto.randomBytes(bytes).toString('hex'); }
function sha256(s){ return crypto.createHash('sha256').update(String(s)).digest('hex'); }
function makeSalt(){ return crypto.randomBytes(16).toString('hex'); }
function hashPassword(password, salt){ return crypto.scryptSync(String(password), Buffer.from(salt,'hex'), 64).toString('hex'); }
function verifyPassword(password, salt, hash){
  const a = Buffer.from(hashPassword(password,salt),'hex');
  const b = Buffer.from(hash,'hex');
  return a.length === b.length && crypto.timingSafeEqual(a,b);
}
function generatedBootstrapPassword(){ return `A!${crypto.randomBytes(18).toString('base64url')}9z`; }
function configuredAdminPassword(){ const p=String(process.env.ALORA_ADMIN_PASSWORD||'').trim(); if(p){ const err=validatePassword(p); if(err) throw new Error(`ALORA_ADMIN_PASSWORD: ${err}`); return p; } return generatedBootstrapPassword(); }
let transactionDepth=0;
function withTransaction(fn){
  const depth=transactionDepth++,savepoint='alora_nested_'+depth;
  try{db.exec(depth?'SAVEPOINT '+savepoint:'BEGIN IMMEDIATE');const out=fn();db.exec(depth?'RELEASE SAVEPOINT '+savepoint:'COMMIT');return out;}
  catch(e){try{db.exec(depth?'ROLLBACK TO SAVEPOINT '+savepoint:'ROLLBACK');if(depth)db.exec('RELEASE SAVEPOINT '+savepoint);}catch{}throw e;}
  finally{transactionDepth--;}
}
function csvSafe(v){
  let x=String(v??'');
  if(/^[\s]*[=+\-@]/.test(x)||/^[\t\r]/.test(x)) x="'"+x;
  return '"'+x.replace(/"/g,'""')+'"';
}
function vietnamToday(){
  const parts=new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Ho_Chi_Minh',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date());
  const o=Object.fromEntries(parts.map(x=>[x.type,x.value])); return `${o.year}-${o.month}-${o.day}`;
}
function vietnamDayRangeUtc(){ const [y,m,d]=vietnamToday().split('-').map(Number); const start=new Date(Date.UTC(y,m-1,d)-7*3600_000); const end=new Date(start.getTime()+86400_000); return [start.toISOString(),end.toISOString()]; }


function initSchema(){
  db.exec(`
  CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    salt TEXT NOT NULL,
    role TEXT NOT NULL,
    department TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    must_change_password INTEGER NOT NULL DEFAULT 1,
    default_station_code TEXT,
    permission_overrides TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sessions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_hash TEXT NOT NULL UNIQUE,
    csrf_token TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
  );
  CREATE TABLE IF NOT EXISTS stations(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
  );
  CREATE TABLE IF NOT EXISTS orders(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE,
    customer TEXT,
    need_by TEXT,
    priority TEXT NOT NULL DEFAULT 'NORMAL',
    current_stage TEXT NOT NULL DEFAULT 'NEW',
    tracking_number TEXT,
    assigned_worker_id INTEGER,
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(assigned_worker_id) REFERENCES users(id)
  );
  CREATE TABLE IF NOT EXISTS order_items(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_db_id INTEGER NOT NULL,
    sku TEXT,
    product_name TEXT NOT NULL,
    qty INTEGER NOT NULL DEFAULT 1,
    material TEXT,
    personalization TEXT,
    item_status TEXT NOT NULL DEFAULT 'PENDING',
    FOREIGN KEY(order_db_id) REFERENCES orders(id) ON DELETE CASCADE
  );
  CREATE TABLE IF NOT EXISTS audit_events(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    action TEXT NOT NULL,
    from_stage TEXT,
    to_stage TEXT,
    user_id INTEGER,
    username TEXT,
    full_name TEXT,
    role TEXT,
    station_code TEXT,
    detail TEXT,
    created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS qc_checks(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_db_id INTEGER NOT NULL,
    result TEXT NOT NULL,
    checklist_json TEXT NOT NULL DEFAULT '{}',
    notes TEXT,
    checked_by INTEGER,
    checked_by_name TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(order_db_id) REFERENCES orders(id) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS idx_qc_order ON qc_checks(order_db_id, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_orders_stage ON orders(current_stage);
  CREATE INDEX IF NOT EXISTS idx_orders_tracking ON orders(tracking_number);
  CREATE INDEX IF NOT EXISTS idx_audit_order ON audit_events(order_id, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_sessions_hash ON sessions(token_hash);
  `);
}


function ensureColumn(table,name,definition){
  const cols=db.prepare(`PRAGMA table_info(${table})`).all().map(x=>x.name);
  if(!cols.includes(name)) db.exec(`ALTER TABLE ${table} ADD COLUMN ${name} ${definition}`);
}
// Fail closed if a database claims schema v20 but its chat objects are missing.
// This can happen after an interrupted/manual deployment or a partially restored
// database. The version row alone is NOT sufficient proof of a usable schema.
const CHAT_REQUIRED_TABLES=Object.freeze(['chat_partner_orgs','chat_order_grants','chat_threads','chat_messages']);
const CHAT_REQUIRED_INDEXES=Object.freeze([
  'idx_chat_grants_partner','ux_chat_thread_scope','idx_chat_threads_order',
  'idx_chat_messages_thread','ux_chat_message_retry'
]);
function missingChatSchema(){
  const objects=db.prepare("SELECT name,type FROM sqlite_master WHERE type IN ('table','index')").all();
  const tables=new Set(objects.filter(r=>r.type==='table').map(r=>r.name));
  const indexes=new Set(objects.filter(r=>r.type==='index').map(r=>r.name));
  const missing=CHAT_REQUIRED_TABLES.filter(name=>!tables.has(name));
  missing.push(...CHAT_REQUIRED_INDEXES.filter(name=>!indexes.has(name)));
  if(!db.prepare('PRAGMA table_info(users)').all().some(row=>row.name==='partner_org_id'))missing.push('users.partner_org_id');
  return missing;
}
const MOBILE_REQUIRED_TABLES=Object.freeze(['chat_mobile_sessions','chat_push_devices','chat_notifications','chat_mentions','chat_routing_rules','chat_routing_scans','chat_auto_assignments']);
const PWA_REQUIRED_TABLES=Object.freeze(['chat_web_push_subscriptions']);
const INTAKE_REQUIRED_TABLES=Object.freeze(['intake_sources','intake_canonical_orders','intake_order_refs','intake_events','intake_reviews','intake_rule_sources']);
function missingIntakeSchema(){
  const tables=new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(r=>r.name));
  return INTAKE_REQUIRED_TABLES.filter(name=>!tables.has(name));
}
function missingMobileSchema(){
  const tables=new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(r=>r.name));
  return MOBILE_REQUIRED_TABLES.filter(t=>!tables.has(t));
}
function repairChatSchemaIfIncomplete(){
  const missing=missingChatSchema();
  if(!missing.length){console.log('[CHAT_SCHEMA] Verified all chat tables and indexes.');return;}
  const recorded=Number(db.prepare('SELECT COALESCE(MAX(version),0) v FROM schema_migrations').get().v||0);
  if(recorded<20||recorded>SCHEMA_VERSION)throw new Error(`[CHAT_SCHEMA] Schema version ${recorded}/${SCHEMA_VERSION}; cannot repair automatically.`);
  console.warn('[CHAT_SCHEMA] Recorded schema '+recorded+' but missing: '+missing.join(', '));
  // Never silently replace a missing table when other chat records exist:
  // that indicates potential loss of real partner conversations or grants.
  const missingTables=CHAT_REQUIRED_TABLES.filter(name=>missing.includes(name));
  if(missingTables.length){
    const existing=CHAT_REQUIRED_TABLES.filter(name=>!missing.includes(name));
    for(const table of existing){
      const count=Number(db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n||0);
      if(count>0)throw new Error(`[CHAT_SCHEMA] ${table} still has ${count} record(s); repair stopped to protect existing chat data. Restore a consistent backup instead.`);
    }
    if(!missing.includes('users.partner_org_id')){
      const users=Number(db.prepare('SELECT COUNT(*) AS n FROM users WHERE partner_org_id IS NOT NULL').get().n||0);
      if(users>0)throw new Error(`[CHAT_SCHEMA] ${users} user(s) are already linked to partner organizations; repair stopped to protect data. Restore a consistent backup.`);
    }
  }
  // A verified full backup (including evidence files) is mandatory before repair.
  const backup=createDatabaseBackup('before-chat-schema-repair');
  if(!backup)throw new Error('[CHAT_SCHEMA] Backup failed. Database has NOT been repaired; startup halted.');
  db.exec('BEGIN IMMEDIATE');
  try{
    chatMigration20.up({db,ensureColumn,nowIso}); // idempotent CREATE IF NOT EXISTS
    const stillMissing=missingChatSchema();
    if(stillMissing.length)throw new Error('Objects still missing: '+stillMissing.join(', '));
    db.exec('COMMIT');
    console.log('[CHAT_SCHEMA] Repair successful. Full backup saved: '+backup);
  }catch(error){
    try{db.exec('ROLLBACK')}catch{}
    throw new Error('[CHAT_SCHEMA] Repair rolled back: '+error.message);
  }
}
function runMigrations(){
  db.exec(`CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);`);
  const currentVersion=Number(db.prepare('SELECT COALESCE(MAX(version),0) v FROM schema_migrations').get().v||0);
  if(currentVersion>SCHEMA_VERSION)throw new Error('Database dùng schema '+currentVersion+' mới hơn bản code này ('+SCHEMA_VERSION+'). Không hạ phiên bản trực tiếp trên dữ liệu này.');
  if(currentVersion<SCHEMA_VERSION && fs.existsSync(DB_PATH) && fs.statSync(DB_PATH).size>0){
    const pre=createDatabaseBackup(`pre-migration-v${currentVersion}-to-v${SCHEMA_VERSION}`);
    if(!pre)throw new Error('Không thể tạo full backup (database + evidence) trước migration. Hệ thống dừng để bảo vệ dữ liệu.');
    console.log(`[MIGRATION] Full safety backup: ${pre}`);
  }
  if(currentVersion<25){
  ensureColumn('orders','deleted_at','TEXT');
  ensureColumn('orders','deleted_by','INTEGER');
  ensureColumn('orders','deleted_reason','TEXT');
  ensureColumn('orders','order_key','TEXT');
  ensureColumn('orders','version','INTEGER NOT NULL DEFAULT 1');
  ensureColumn('orders','need_by_iso','TEXT');
  ensureColumn('users','permission_overrides',"TEXT NOT NULL DEFAULT '{}'");
  ensureColumn('users','auth_version','INTEGER NOT NULL DEFAULT 1');
  ensureColumn('users','password_changed_at','TEXT');
  ensureColumn('users','deleted_at','TEXT');
  ensureColumn('users','deleted_by','INTEGER');
  ensureColumn('users','original_username','TEXT');
  ensureColumn('sessions','auth_version','INTEGER NOT NULL DEFAULT 1');
  ensureColumn('audit_events','order_db_id','INTEGER');
  ensureColumn('order_items','current_stage',"TEXT NOT NULL DEFAULT 'PENDING'");
  ensureColumn('order_items','item_key','TEXT');
  ensureColumn('order_items','active','INTEGER NOT NULL DEFAULT 1');
  ensureColumn('order_items','deleted_at','TEXT');
  ensureColumn('qc_checks','order_item_id','INTEGER');
  ensureColumn('qc_checks','defect_category','TEXT');
  db.exec(`CREATE TABLE IF NOT EXISTS packing_checks(
    id INTEGER PRIMARY KEY AUTOINCREMENT, order_db_id INTEGER NOT NULL, checklist_json TEXT NOT NULL DEFAULT '{}', notes TEXT, checked_by INTEGER, checked_by_name TEXT, created_at TEXT NOT NULL,
    FOREIGN KEY(order_db_id) REFERENCES orders(id) ON DELETE CASCADE
  );`);
  db.exec(`CREATE TABLE IF NOT EXISTS qc_item_results(
    id INTEGER PRIMARY KEY AUTOINCREMENT, qc_check_id INTEGER NOT NULL, order_item_id INTEGER NOT NULL, result TEXT NOT NULL, created_at TEXT NOT NULL,
    FOREIGN KEY(qc_check_id) REFERENCES qc_checks(id) ON DELETE CASCADE, FOREIGN KEY(order_item_id) REFERENCES order_items(id) ON DELETE CASCADE
  ); CREATE INDEX IF NOT EXISTS idx_qc_item_check ON qc_item_results(qc_check_id,order_item_id);`);
  ensureColumn('qc_item_results','sku_snapshot','TEXT');
  ensureColumn('qc_item_results','product_name_snapshot','TEXT');
  ensureColumn('qc_item_results','qty_snapshot','INTEGER');
  ensureColumn('qc_item_results','material_snapshot','TEXT');
  db.exec(`CREATE TABLE IF NOT EXISTS evidence_files(
    id INTEGER PRIMARY KEY AUTOINCREMENT, order_db_id INTEGER NOT NULL, order_item_id INTEGER, category TEXT NOT NULL, file_name TEXT NOT NULL, mime TEXT NOT NULL, storage_path TEXT NOT NULL, uploaded_by INTEGER, uploaded_by_name TEXT, created_at TEXT NOT NULL,
    FOREIGN KEY(order_db_id) REFERENCES orders(id) ON DELETE CASCADE, FOREIGN KEY(order_item_id) REFERENCES order_items(id) ON DELETE SET NULL
  ); CREATE INDEX IF NOT EXISTS idx_evidence_order ON evidence_files(order_db_id,created_at DESC);`);
  ensureColumn('evidence_files','size_bytes','INTEGER NOT NULL DEFAULT 0');
  ensureColumn('evidence_files','content_sha256','TEXT');
  ensureColumn('evidence_files','qc_check_id','INTEGER');
  ensureColumn('evidence_files','packing_check_id','INTEGER');
  db.exec(`CREATE TABLE IF NOT EXISTS login_failures(key TEXT PRIMARY KEY, count INTEGER NOT NULL DEFAULT 0, locked_until TEXT, updated_at TEXT NOT NULL);`);
  ensureColumn('order_items','inventory_material_id','INTEGER');
  ensureColumn('order_items','material_usage_per_unit','REAL NOT NULL DEFAULT 0');
  db.exec(`CREATE TABLE IF NOT EXISTS inventory_materials(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    color TEXT,
    unit TEXT NOT NULL DEFAULT 'M',
    current_stock REAL NOT NULL DEFAULT 0,
    min_stock REAL NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS inventory_movements(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    material_id INTEGER NOT NULL,
    movement_type TEXT NOT NULL,
    quantity REAL NOT NULL,
    balance_before REAL NOT NULL,
    balance_after REAL NOT NULL,
    order_db_id INTEGER,
    order_id_snapshot TEXT,
    order_item_id INTEGER,
    item_key_snapshot TEXT,
    reference TEXT,
    note TEXT,
    user_id INTEGER,
    user_name TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(material_id) REFERENCES inventory_materials(id),
    FOREIGN KEY(order_db_id) REFERENCES orders(id) ON DELETE SET NULL,
    FOREIGN KEY(order_item_id) REFERENCES order_items(id) ON DELETE SET NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
  );
  CREATE INDEX IF NOT EXISTS idx_inventory_movements_material ON inventory_movements(material_id,id DESC);
  CREATE INDEX IF NOT EXISTS idx_inventory_movements_order ON inventory_movements(order_db_id,id DESC);
  CREATE INDEX IF NOT EXISTS idx_order_items_inventory_material ON order_items(inventory_material_id,active);`);
  for(const e of db.prepare('SELECT id,storage_path,size_bytes,content_sha256 FROM evidence_files').all()){
    const full=path.resolve(EVIDENCE_DIR,e.storage_path||''),prefix=path.resolve(EVIDENCE_DIR)+path.sep;if(full.startsWith(prefix)&&fs.existsSync(full)){const st=fs.statSync(full);if(!e.size_bytes||!e.content_sha256)db.prepare('UPDATE evidence_files SET size_bytes=?,content_sha256=? WHERE id=?').run(st.size,shaFile(full),e.id);}
  }
  // V13 stable item identity and normalized due-date storage.
  for(const it of db.prepare('SELECT id,item_key FROM order_items').all()){ if(!it.item_key)db.prepare('UPDATE order_items SET item_key=? WHERE id=?').run(crypto.randomUUID(),it.id); }
  db.exec(`UPDATE qc_item_results SET
    sku_snapshot=COALESCE(sku_snapshot,(SELECT sku FROM order_items WHERE id=qc_item_results.order_item_id)),
    product_name_snapshot=COALESCE(product_name_snapshot,(SELECT product_name FROM order_items WHERE id=qc_item_results.order_item_id)),
    qty_snapshot=COALESCE(qty_snapshot,(SELECT qty FROM order_items WHERE id=qc_item_results.order_item_id)),
    material_snapshot=COALESCE(material_snapshot,(SELECT material FROM order_items WHERE id=qc_item_results.order_item_id))
    WHERE sku_snapshot IS NULL OR product_name_snapshot IS NULL OR qty_snapshot IS NULL OR material_snapshot IS NULL;`);
  for(const o of db.prepare('SELECT id,need_by,need_by_iso FROM orders').all()){ if(!o.need_by_iso&&o.need_by){ try{const display=strictNeedBy(o.need_by);const m=display.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);if(m)db.prepare('UPDATE orders SET need_by_iso=? WHERE id=?').run(`${m[3]}-${m[2]}-${m[1]}`,o.id);}catch{} } }
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_order_items_item_key ON order_items(item_key); CREATE INDEX IF NOT EXISTS idx_order_items_active ON order_items(order_db_id,active,id); CREATE INDEX IF NOT EXISTS idx_orders_need_by_iso ON orders(need_by_iso); CREATE TRIGGER IF NOT EXISTS trg_order_items_item_key AFTER INSERT ON order_items WHEN NEW.item_key IS NULL OR NEW.item_key='' BEGIN UPDATE order_items SET item_key=lower(hex(randomblob(16))) WHERE id=NEW.id; END;`);
  const rows=db.prepare('SELECT id,order_id,order_key FROM orders').all();
  const seen=new Map();
  for(const r of rows){
    let key=normalizeOrderId(r.order_id);
    if(seen.has(key)){ console.warn(`[MIGRATION] Duplicate normalized Order ID: ${r.order_id} conflicts with row ${seen.get(key)}. Kept unique migration key only; Admin should merge/delete duplicate.`); key=`${key}~DUP${r.id}`; }
    seen.set(key,r.id);
    if(r.order_key!==key) db.prepare('UPDATE orders SET order_key=? WHERE id=?').run(key,r.id);
  }
  try{ db.exec('CREATE UNIQUE INDEX IF NOT EXISTS ux_orders_order_key ON orders(order_key)'); }catch(e){ console.warn('Could not create normalized unique order index:',e.message); }
  db.exec('CREATE INDEX IF NOT EXISTS idx_audit_order_db_id ON audit_events(order_db_id, id DESC); CREATE INDEX IF NOT EXISTS idx_order_items_stage ON order_items(current_stage);');
  if(currentVersion<14){ db.prepare('DELETE FROM sessions').run(); console.log('[MIGRATION] Old sessions revoked after security/schema upgrade.'); }
  if(currentVersion<14)db.prepare('INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(?,?)').run(14,nowIso());
  if(currentVersion<15)db.prepare('INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(?,?)').run(15,nowIso());
  }
  runVersionedMigrations({db,ensureColumn,nowIso,targetVersion:SCHEMA_VERSION});
  repairChatSchemaIfIncomplete();
  const missingMobile=missingMobileSchema();
  if(missingMobile.length)throw new Error('[MOBILE_SCHEMA] Incomplete v21 schema: '+missingMobile.join(', ')+'. Restore a consistent backup; do not recreate missing data tables.');
  const missingIntake=missingIntakeSchema();
  if(missingIntake.length)throw new Error('[INTAKE_SCHEMA] Incomplete v22 schema: '+missingIntake.join(', ')+'. Restore a consistent backup.');
  const missingPwa=PWA_REQUIRED_TABLES.filter(name=>!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name));
  if(missingPwa.length)throw new Error('[PWA_SCHEMA] Incomplete v23 schema: '+missingPwa.join(', ')+'. Restore a consistent backup.');
}

function createUser({username,fullName,password,role,department,defaultStationCode,mustChange=1,permissionOverrides={}}){
  username=String(username??'').trim().toLowerCase();fullName=String(fullName??'').trim();department=String(department??'').trim();
  const ue=validateUsername(username),fe=validateFullName(fullName),de=validateDepartment(department),pe=validatePassword(password); if(ue||fe||de||pe)throw badRequest(ue||fe||de||pe,'USER_VALIDATION');
  if(!ROLE_STATIONS[role])throw badRequest('Role không hợp lệ.','ROLE_INVALID');
  const salt=makeSalt(); const hash=hashPassword(password,salt); const t=nowIso();
  const clean=sanitizePermissionOverrides(permissionOverrides,PERMISSION_CATALOG,ADMIN_ONLY_PERMISSIONS);
  const info=db.prepare(`INSERT INTO users(username,full_name,password_hash,salt,role,department,active,must_change_password,default_station_code,permission_overrides,created_at,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`).run(username,fullName,hash,salt,role,department||'',1,mustChange?1:0,defaultStationCode||null,JSON.stringify(clean),t,t);
  return info.lastInsertRowid;
}

function initSeed(){
  const stationCount=db.prepare('SELECT COUNT(*) c FROM stations').get().c;
  if(!stationCount){
    const ins=db.prepare('INSERT INTO stations(code,name,type,active) VALUES(?,?,?,1)');
    [['ORDER-01','Kê đơn 01','ORDER'],['PROD-01','Sản xuất 01','PRODUCTION'],['QC-01','QC 01','QC'],['PACK-01','Đóng gói 01','PACKING'],['SHIP-01','Shipping 01','SHIPPING'],['WARE-01','Kho vải 01','WAREHOUSE'],['CSKH-01','CSKH 01','CSKH']].forEach(x=>ins.run(...x));
  }
  const userCount=db.prepare('SELECT COUNT(*) c FROM users').get().c;
  if(!userCount){
    if(!process.env.ALORA_ADMIN_PASSWORD&&!process.stdout.isTTY)throw new Error('First startup requires ALORA_ADMIN_PASSWORD in private configuration.');
    const password=configuredAdminPassword();
    createUser({username:'admin',fullName:'3 ALORA Admin',password,role:'ADMIN',department:'Management',mustChange:1});
    console.log('\n============================================================');
    console.log('3ALORA FIRST RUN - ADMIN BOOTSTRAP PASSWORD');
    console.log('Username: admin');
    if(!process.env.ALORA_ADMIN_PASSWORD&&process.stdout.isTTY)console.log(`Password: ${password}`);else if(!process.env.ALORA_ADMIN_PASSWORD)throw new Error('Set ALORA_ADMIN_PASSWORD before non-interactive first startup.');
    console.log('This password is NOT written to disk. Change it immediately.');
    console.log('============================================================\n');
  }
  if(process.env.SEED_DEMO_DATA==='1'){
    const orderCount=db.prepare('SELECT COUNT(*) c FROM orders').get().c;
    if(!orderCount){
      const demo=[['4182000001','Anna Smith','05/10/2026','EXPRESS','NEW','WS-WHITE','Wreath Sash',2,'White Cotton']];
      for(const d of demo) withTransaction(()=>{
        const t=nowIso(), key=normalizeOrderId(d[0]);
        const r=db.prepare(`INSERT INTO orders(order_id,order_key,customer,need_by,need_by_iso,priority,current_stage,stage_updated_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`).run(d[0],key,d[1],d[2],needByIso(d[2]),d[3],d[4],t,t,t);
        db.prepare(`INSERT INTO order_items(order_db_id,sku,product_name,qty,material,personalization,item_status,current_stage) VALUES(?,?,?,?,?,?,?,?)`).run(r.lastInsertRowid,d[5],d[6],d[7],d[8],'','PENDING','PENDING');
      });
    }
  }
}


function safeBackupStamp(){ return new Date().toISOString().replace(/[:.]/g,'-'); }
function encryptBackupFile(src){
  if(!BACKUP_ENCRYPTION_KEY)return src;
  const salt=crypto.randomBytes(16),key=deriveBackupKey(BACKUP_ENCRYPTION_KEY,salt),iv=crypto.randomBytes(12),enc=crypto.createCipheriv('aes-256-gcm',key,iv),cipher=Buffer.concat([enc.update(fs.readFileSync(src)),enc.final()]),tag=enc.getAuthTag(),out=src+'.enc';
  fs.writeFileSync(out,Buffer.concat([Buffer.from('ALORAENC2'),salt,iv,tag,cipher])); fs.unlinkSync(src); return out;
}
function decryptBackupTemp(file){
  if(!file.endsWith('.enc'))return {file,cleanup:false};
  if(!BACKUP_ENCRYPTION_KEY)throw new Error('BACKUP_ENCRYPTION_KEY chưa được cấu hình.');
  const raw=fs.readFileSync(file); let outBuf;
  if(raw.subarray(0,9).toString()==='ALORAENC2'){
    const salt=raw.subarray(9,25),iv=raw.subarray(25,37),tag=raw.subarray(37,53),cipher=raw.subarray(53),key=deriveBackupKey(BACKUP_ENCRYPTION_KEY,salt),dec=crypto.createDecipheriv('aes-256-gcm',key,iv);dec.setAuthTag(tag);outBuf=Buffer.concat([dec.update(cipher),dec.final()]);
  }else if(raw.subarray(0,8).toString()==='ALORAENC'){
    const iv=raw.subarray(8,20),tag=raw.subarray(20,36),cipher=raw.subarray(36),key=crypto.createHash('sha256').update(BACKUP_ENCRYPTION_KEY).digest(),dec=crypto.createDecipheriv('aes-256-gcm',key,iv);dec.setAuthTag(tag);outBuf=Buffer.concat([dec.update(cipher),dec.final()]);
  }else throw new Error('Encrypted backup format không hợp lệ.');
  const ext=file.includes('.alorabak')?'.alorabak':'.sqlite',tmp=path.join(path.dirname(file),`.verify-${process.pid}-${Date.now()}${ext}`);fs.writeFileSync(tmp,outBuf);return {file:tmp,cleanup:true};
}
function verifyDatabaseFile(file){
  let tdb,tmp; try{ tmp=decryptBackupTemp(file); tdb=new DatabaseSync(tmp.file,{readOnly:true}); const r=tdb.prepare('PRAGMA integrity_check').get(); return {ok:String(Object.values(r||{})[0]||'').toLowerCase()==='ok',detail:Object.values(r||{})[0]||''}; }catch(e){return {ok:false,detail:e.message}} finally{try{tdb?.close()}catch{};try{if(tmp?.cleanup)fs.unlinkSync(tmp.file)}catch{}}
}
function verifyBackupFile(file){
  let dec=null,stage=null,tdb=null;
  try{
    const manifest=file+'.sha256'; if(fs.existsSync(manifest)){const expected=String(fs.readFileSync(manifest,'utf8')).trim().split(/\s+/)[0];const actual=shaFile(file);if(expected&&expected!==actual)return {ok:false,detail:'SHA-256 manifest không khớp.'};}
    if(file.includes('.alorabak')){
      dec=decryptBackupTemp(file); stage=fs.mkdtempSync(path.join(path.dirname(DB_PATH),'.verify-bundle-')); const info=inspectArchive(dec.file,{extractDir:stage});
      const dbFile=path.join(stage,'database','3alora.sqlite');if(!fs.existsSync(dbFile))throw new Error('Backup bundle thiếu database.');tdb=new DatabaseSync(dbFile,{readOnly:true});const r=tdb.prepare('PRAGMA integrity_check').get(),detail=String(Object.values(r||{})[0]||'');if(detail.toLowerCase()!=='ok')return {ok:false,detail};
      if(tdb.prepare("SELECT 1 FROM sqlite_master WHERE name='evidence_files'").get()){
        const entries=new Map(info.entries.map(e=>[e.path,e]));
        for(const row of tdb.prepare('SELECT storage_path FROM evidence_files').all()){
          if(!entries.has('evidence/'+String(row.storage_path).replace(/\\/g,'/')))throw new Error('Missing referenced evidence: '+row.storage_path);
        }
      }
      const evidenceCount=info.entries.filter(e=>e.path.startsWith('evidence/')).length; return {ok:true,detail:'ok',bundle:true,evidenceCount,entryCount:info.entries.length,meta:info.meta};
    }
    return verifyDatabaseFile(file);
  }catch(e){return {ok:false,detail:e.message}}
  finally{try{tdb?.close()}catch{};try{if(dec?.cleanup)fs.rmSync(dec.file,{force:true})}catch{};try{if(stage)fs.rmSync(stage,{recursive:true,force:true})}catch{}}
}
function pruneBackupDirectory(dir,keep){
  if(!dir||!fs.existsSync(dir))return;
  const files=fs.readdirSync(dir).filter(x=>!x.includes('-pinned-')&&/\.(?:alorabak|sqlite)(?:\.enc)?$/.test(x)).map(name=>({name,full:path.join(dir,name),mtime:fs.statSync(path.join(dir,name)).mtimeMs})).sort((a,b)=>b.mtime-a.mtime);
  for(const f of files.slice(keep)){try{fs.rmSync(f.full,{force:true});fs.rmSync(f.full+'.sha256',{force:true})}catch{}}
}
function createDatabaseBackup(reason='scheduled'){
  let snapshot=null,bundle=null;
  try{
    snapshot=path.join(BACKUP_DIR,`.snapshot-${process.pid}-${Date.now()}.sqlite`); const escaped=snapshot.replace(/'/g,"''"); db.exec(`VACUUM INTO '${escaped}'`);
    const integrity=verifyDatabaseFile(snapshot);if(!integrity.ok)throw new Error(`Database snapshot integrity failed: ${integrity.detail}`);
    bundle=path.join(BACKUP_DIR,`3alora-${safeBackupStamp()}-${reason}.alorabak`);
    const entries=[{full:snapshot,path:'database/3alora.sqlite',size:fs.statSync(snapshot).size,sha256:shaFile(snapshot)}];
    for(const f of listFiles(EVIDENCE_DIR)){entries.push({full:f.full,path:path.posix.join('evidence',f.rel),size:f.size,sha256:shaFile(f.full)});}
    writeArchive(bundle,entries,{version:2,appVersion:APP_VERSION,createdAt:nowIso(),reason,evidenceCount:entries.length-1,totalBytes:entries.reduce((n,e)=>n+e.size,0)});
    fs.rmSync(snapshot,{force:true});snapshot=null;
    const pre=verifyBackupFile(bundle);if(!pre.ok)throw new Error(`Backup verification failed: ${pre.detail}`);
    const finalTarget=encryptBackupFile(bundle);bundle=null; const digest=shaFile(finalTarget);fs.writeFileSync(finalTarget+'.sha256',`${digest}  ${path.basename(finalTarget)}\n`,'utf8');
    const finalCheck=verifyBackupFile(finalTarget);if(!finalCheck.ok)throw new Error(`Encrypted backup verification failed: ${finalCheck.detail}`);
    if(OFFSITE_BACKUP_DIR){fs.copyFileSync(finalTarget,path.join(OFFSITE_BACKUP_DIR,path.basename(finalTarget)));fs.copyFileSync(finalTarget+'.sha256',path.join(OFFSITE_BACKUP_DIR,path.basename(finalTarget)+'.sha256'));pruneBackupDirectory(OFFSITE_BACKUP_DIR,OFFSITE_BACKUP_KEEP);}
    pruneBackupDirectory(BACKUP_DIR,BACKUP_KEEP); console.log(`Full backup created and verified: ${finalTarget}`); return finalTarget;
  }catch(e){console.error('Full backup failed:',e.message);return null}
  finally{try{if(snapshot)fs.rmSync(snapshot,{force:true})}catch{};try{if(bundle)fs.rmSync(bundle,{force:true})}catch{}}
}


let backupJob=null;
function createRuntimeBackup(reason){
 if(backupJob)return backupJob;
 const {Worker}=require('node:worker_threads');
 backupJob=new Promise(resolve=>{
  const worker=new Worker(path.join(ROOT,'src/modules/operations/backup-worker.js'),{workerData:{DB_PATH,EVIDENCE_DIR,BACKUP_DIR,BACKUP_KEEP,OFFSITE_BACKUP_DIR,OFFSITE_BACKUP_KEEP,BACKUP_ENCRYPTION_KEY,APP_VERSION,reason}});
  let settled=false;const done=value=>{if(settled)return;settled=true;backupJob=null;resolve(value)};
  worker.on('message',message=>{if(message.error){structuredLog('error','backup_failed',{message:message.error});done(null)}else done(message.file)});
  worker.on('error',err=>{structuredLog('error','backup_failed',{message:err.message});done(null)});
  worker.on('exit',code=>{if(!settled)done(null)});
 });return backupJob;
}

function reconcileEvidence({deleteOrphans=true}={}){
  const known=new Set(db.prepare('SELECT storage_path FROM evidence_files').all().map(x=>String(x.storage_path||'').replace(/\\/g,'/')));let missing=0,orphans=0,temps=0;
  for(const row of db.prepare('SELECT id,storage_path FROM evidence_files').all()){const full=path.resolve(EVIDENCE_DIR,row.storage_path||''),prefix=path.resolve(EVIDENCE_DIR)+path.sep;if(!full.startsWith(prefix)||!fs.existsSync(full))missing++;}
  for(const f of listFiles(EVIDENCE_DIR)){const rel=String(f.rel).replace(/\\/g,'/');if(rel.endsWith('.tmp')){try{if(Date.now()-fs.statSync(f.full).mtimeMs>3600_000){fs.rmSync(f.full,{force:true});temps++;}}catch{};continue}if(!known.has(rel)){orphans++;if(deleteOrphans){try{if(Date.now()-fs.statSync(f.full).mtimeMs>24*3600_000)fs.rmSync(f.full,{force:true})}catch{}}}}
  if(missing||orphans||temps)structuredLog('warn','evidence_reconcile',{missing,orphans,tempRemoved:temps});return {missing,orphans,tempRemoved:temps};
}

function cleanupExpiredSessions(){
  try{ db.prepare('DELETE FROM chat_mobile_sessions WHERE expires_at<=?').run(nowIso());db.prepare('DELETE FROM login_failures WHERE updated_at<?').run(new Date(Date.now()-86400000).toISOString());db.prepare('DELETE FROM sessions WHERE expires_at<=?').run(nowIso()); }catch(e){ console.error('Session cleanup failed:',e.message); }
}

initSchema(); runMigrations(); initSeed();
const chatWebPush=createChatWebPush({db,nowIso});
const chatMobileService=createChatMobileService({db,nowIso,readJson,sendJson,userCanViewOrder,audit,chatWebPush});
const multiSourceIntake=createMultiSourceIntake({db,nowIso,readJson,sendJson,withTransaction,chatMobileService,audit,requestIsHttps,nodeEnv:NODE_ENV,clientIp});
const aloraEtsySync=createAloraEtsySync({db,nowIso,readJson,sendJson,withTransaction,requireAuth,requireCsrf,requestIsHttps,nodeEnv:NODE_ENV,audit,hasPermission});
const chatGroups=createChatGroups({db,nowIso,readJson,sendJson,chatMobileService,audit,userCanViewOrder});
chatMobileService.setGroupAccessChecker(chatGroups.canAccessGroup);
const chatLifecycle=createChatLifecycle({db,nowIso,readJson,sendJson,userCanViewOrder,audit});
const chatService=createChatService({db,nowIso,normalizeOrderId,userCanViewOrder,visibleOrderClause,hasPermission,readJson,sendJson,validateUsername,validateFullName,validatePassword,makeSalt,hashPassword,withTransaction,audit,chatMobileService,chatGroups,chatLifecycle});
// Background reconciliation handles orders created through Sheets/imports; only orders NEWER than
// enabled_at can be routed. No existing historical orders are distributed when rules are enabled.
setInterval(()=>{try{chatMobileService.reconcileNewOrders()}catch(e){console.error('[CHAT_ROUTING]',e.message)}},20_000).unref?.();
realtime=require('./src/modules/operations/realtime').createRealtime({getSession,nowIso});
const loginGuard=createLoginGuard({db,hash:sha256,nowIso,maxFailures:LOGIN_MAX_FAILURES,lockMinutes:LOGIN_LOCK_MINUTES});
const importBatchRepo=createImportBatchRepository({db,nowIso});
const inventoryService=createInventoryService({db,nowIso,businessDate:vietnamToday,withTransaction,audit});
const STARTUP_DATA_QUALITY=dataQualityReport();
if(!STARTUP_DATA_QUALITY.ok)console.warn(`[DATA QUALITY] ${STARTUP_DATA_QUALITY.issueCount} issue(s) across ${STARTUP_DATA_QUALITY.categories} category(ies). Check /api/admin/data-quality.`);

setTimeout(()=>createRuntimeBackup('startup'),15_000).unref?.();
setInterval(()=>createRuntimeBackup('scheduled'),BACKUP_INTERVAL_HOURS*3600_000).unref?.();
setInterval(cleanupExpiredSessions,3600_000).unref?.();
setTimeout(()=>reconcileEvidence({deleteOrphans:true}),20_000).unref?.();
setInterval(()=>reconcileEvidence({deleteOrphans:true}),24*3600_000).unref?.();

function rotateLogsIfNeeded(){try{if(!fs.existsSync(LOG_FILE)||fs.statSync(LOG_FILE).size<LOG_MAX_BYTES)return;for(let i=LOG_KEEP-1;i>=1;i--){const src=`${LOG_FILE}.${i}`,dst=`${LOG_FILE}.${i+1}`;if(fs.existsSync(src)){if(i+1>LOG_KEEP)fs.rmSync(src,{force:true});else fs.renameSync(src,dst);}}fs.renameSync(LOG_FILE,`${LOG_FILE}.1`);}catch{}}
function structuredLog(level,event,data={}){try{rotateLogsIfNeeded();fs.appendFileSync(LOG_FILE,JSON.stringify({ts:nowIso(),level,event,...data})+'\n')}catch{}}
function securityHeaders(res){
  res.setHeader('X-Content-Type-Options','nosniff');
  res.setHeader('X-Frame-Options','DENY');
  res.setHeader('Referrer-Policy','no-referrer');
  res.setHeader('Permissions-Policy','camera=(self), microphone=(), geolocation=(), payment=(), usb=(), serial=()');
  res.setHeader('Cross-Origin-Opener-Policy','same-origin');
  res.setHeader('Cross-Origin-Resource-Policy','same-origin');
  res.setHeader('X-Permitted-Cross-Domain-Policies','none');
  res.setHeader('X-Robots-Tag','noindex, nofollow, noarchive, nosnippet');
  res.setHeader('Content-Security-Policy',"default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'; manifest-src 'self'");
  if(IS_PROD || process.env.COOKIE_SECURE==='1') res.setHeader('Strict-Transport-Security','max-age=31536000; includeSubDomains');
}
function sendJson(res,status,obj){ securityHeaders(res); res.statusCode=status; res.setHeader('Cache-Control','no-store'); res.setHeader('Pragma','no-cache'); res.setHeader('Content-Type','application/json; charset=utf-8'); res.end(JSON.stringify(normalizeApiPayload(status,obj))); }
function sendText(res,status,text,type='text/plain; charset=utf-8'){ securityHeaders(res); res.statusCode=status; res.setHeader('Cache-Control','no-store'); res.setHeader('Content-Type',type); res.end(text); }
function parseCookies(req){ return parseCookieHeader(req.headers.cookie||''); }
function clientIp(req){
  if(isTrustedProxy(req)){ const x=String(req.headers['x-forwarded-for']||'').split(',')[0].trim(); if(x)return x; }
  return req.socket.remoteAddress || 'unknown';
}
function isTrustedProxy(req){const ip=String(req.socket.remoteAddress||'').replace(/^::ffff:/,'');return TRUST_PROXY&&String(process.env.TRUSTED_PROXY_IPS||'127.0.0.1,::1').split(',').map(x=>x.trim()).includes(ip);}
function requestIsHttps(req){ return !!req.socket.encrypted || (isTrustedProxy(req) && String(req.headers['x-forwarded-proto']||'').split(',')[0].trim()==='https'); }
function sessionCookieName(){ return (IS_PROD || process.env.COOKIE_SECURE==='1') ? '__Host-alora_session' : 'alora_session'; }
function setSessionCookie(req,res,token){ const secure=IS_PROD || process.env.COOKIE_SECURE==='1' || requestIsHttps(req); const name=secure?'__Host-alora_session':'alora_session'; res.setHeader('Set-Cookie',`${name}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${SESSION_HOURS*3600}${secure?'; Secure':''}`); }
function clearSessionCookie(req,res){ const secure=IS_PROD || process.env.COOKIE_SECURE==='1' || requestIsHttps(req); const name=secure?'__Host-alora_session':'alora_session'; res.setHeader('Set-Cookie',`${name}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0${secure?'; Secure':''}`); }
async function readJson(req,max=5_000_000){
  const declared=Number(req.headers['content-length']||0);
  if(declared>max)throw tooLarge('Dữ liệu gửi lên vượt quá 5 MB.');
  const chunks=[];let size=0;
  for await(const chunk of req){size+=chunk.length;if(size>max)throw tooLarge('Dữ liệu gửi lên vượt quá 5 MB.');chunks.push(chunk);}
  let value;try{value=size?JSON.parse(Buffer.concat(chunks,size).toString('utf8')):{}}catch{throw badRequest('JSON không hợp lệ.','INVALID_JSON')}
  if(!value||typeof value!=='object'||Array.isArray(value))throw badRequest('Request body phải là JSON object.','INVALID_BODY');
  return value;
}
function checkOrderVersion(order,body){if(body.version!==undefined&&Number(body.version)!==Number(order.version))throw conflict('Đơn vừa được cập nhật. Hãy tải lại.','EDIT_CONFLICT');}
function withStockTransaction(user,body,fn){
 const id=body.operationId;if(id!==undefined&&!(typeof id==='string'&&/^[A-Za-z0-9_-]{12,100}$/.test(id)))throw badRequest('Mã thao tác không hợp lệ.');
 const hash=sha256(JSON.stringify(body));
 return withTransaction(()=>{
  if(id){const old=db.prepare('SELECT * FROM api_mutation_receipts WHERE user_id=? AND operation_id=?').get(user.id,id);if(old){if(old.request_hash!==hash)throw conflict('Mã thao tác đã dùng cho dữ liệu khác.','OPERATION_CONFLICT');return {...JSON.parse(old.response_json),replayed:true}}}
  const result=fn();if(id)db.prepare('INSERT INTO api_mutation_receipts(user_id,operation_id,request_hash,status,response_json,created_at) VALUES(?,?,?,?,?,?)').run(user.id,id,hash,200,JSON.stringify(result),nowIso());return result;
 });
}

function positiveLimit(raw,fallback=150,max=500){if(raw===null||raw===undefined||raw==='')return fallback;const n=Number(raw);if(!Number.isSafeInteger(n)||n<1||n>max)throw badRequest('Giới hạn không hợp lệ.','INVALID_LIMIT');return n;}

function getUserById(id){ return db.prepare('SELECT * FROM users WHERE id=? AND deleted_at IS NULL').get(id); }
const RBAC_CONTEXT={catalog:PERMISSION_CATALOG,rolePermissions:ROLE_PERMISSIONS,adminOnly:ADMIN_ONLY_PERMISSIONS};
function effectivePermissions(u){return rbacEffectivePermissions(u,RBAC_CONTEXT);}
function hasPermission(u,perm){return rbacHasPermission(u,perm,RBAC_CONTEXT);}
function requireAdminUser(user){return !!user&&user.role==='ADMIN';}
function safeUser(u){ return u?{id:u.id,username:u.username,fullName:u.full_name,role:u.role,department:u.department,partnerOrgId:u.role==='PARTNER'?u.partner_org_id:null,active:!!u.active,mustChangePassword:!!u.must_change_password,defaultStationCode:u.default_station_code,permissions:effectivePermissions(u),permissionOverrides:parsePermissionOverrides(u)}:null; }
function audit({orderId='',orderDbId=null,action,fromStage='',toStage='',user=null,stationCode='',detail='',entityType='',entityId=''}){
  if(!orderDbId && orderId){ const r=db.prepare('SELECT id FROM orders WHERE order_id=? OR order_key=? LIMIT 1').get(orderId,normalizeOrderId(orderId)); orderDbId=r?.id||null; }
  const requestId=getRequestId()||null;
  const resolvedEntityType=entityType||((orderDbId||orderId)?'ORDER':'SYSTEM');
  const resolvedEntityId=entityId||orderId||(orderDbId?String(orderDbId):null);
  setImmediate(()=>realtime?.publish('refresh'));
  db.prepare(`INSERT INTO audit_events(order_db_id,order_id,action,from_stage,to_stage,user_id,username,full_name,role,station_code,detail,request_id,entity_type,entity_id,created_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(orderDbId,orderId||null,action,fromStage||null,toStage||null,user?.id||null,user?.username||null,user?.full_name||null,user?.role||null,stationCode||null,detail||null,requestId,resolvedEntityType,resolvedEntityId,nowIso());
}

function getSession(req){
  const bearer=String(req.headers.authorization||'').match(/^Bearer ([A-Za-z0-9_-]{40,180})$/);
  if(bearer){
    // Reject plaintext bearer credentials on non-loopback connections.
    if(!requestIsHttps(req)&&NODE_ENV!=='test'&&clientIp(req)!=='127.0.0.1'&&clientIp(req)!=='::1')return null;
    // Token accepted ONLY for mobile/chat APIs, not legacy operations/finance/admin.
    const pathname=String(req.url||'').split('?')[0];
    if(!pathname.startsWith('/api/chat/')&&!pathname.startsWith('/api/mobile/'))return null;
    const record=db.prepare('SELECT * FROM chat_mobile_sessions WHERE token_hash=?').get(sha256(bearer[1]));
    if(!record)return null;
    if(new Date(record.expires_at).getTime()<=Date.now()){db.prepare('DELETE FROM chat_mobile_sessions WHERE token_hash=?').run(record.token_hash);return null;}
    const user=getUserById(record.user_id);
    if(!user||!user.active||Number(user.auth_version||1)!==Number(record.auth_version||1))return null;
    if(user.role==='PARTNER'&&!db.prepare('SELECT 1 FROM chat_partner_orgs WHERE id=? AND active=1').get(user.partner_org_id))return null;
    return {session:record,user,bearer:true,tokenHash:record.token_hash};
  }
  const cookies=parseCookies(req); const token=cookies['__Host-alora_session'] || cookies.alora_session; if(!token) return null;
  const h=sha256(token); const s=db.prepare('SELECT * FROM sessions WHERE token_hash=?').get(h); if(!s) return null;
  if(new Date(s.expires_at).getTime()<=Date.now()){ db.prepare('DELETE FROM sessions WHERE id=?').run(s.id); return null; }
  const u=getUserById(s.user_id); if(!u||!u.active) return null;
  if(u.role==='PARTNER' && !db.prepare('SELECT 1 FROM chat_partner_orgs WHERE id=? AND active=1').get(u.partner_org_id)) return null;
  if(Number(s.auth_version||1)!==Number(u.auth_version||1)){ db.prepare('DELETE FROM sessions WHERE id=?').run(s.id); return null; }
  if(Date.now()-Date.parse(s.last_seen_at)>60000)db.prepare('UPDATE sessions SET last_seen_at=? WHERE id=?').run(nowIso(),s.id);
  return {session:s,user:u,token};
}
function requireAuth(req,res){ const x=getSession(req); if(!x){sendJson(res,401,{ok:false,error:'UNAUTHENTICATED'});return null;} return x; }
function requireCsrf(req,res,auth){ if(auth?.bearer)return true; if(!['POST','PUT','PATCH','DELETE'].includes(req.method))return true; if(req.headers['x-csrf-token']!==auth.session.csrf_token){sendJson(res,403,{ok:false,error:'BAD_CSRF'});return false;} return true; }
function roleCanStation(user,type){
  if(!user)return false;
  const map={ORDER:'station.order',PRODUCTION:'station.production',QC:'station.qc',PACKING:'station.packing',SHIPPING:'station.shipping',WAREHOUSE:'inventory.issue',CSKH:'orders.view_all'};
  return user.role==='ADMIN' || hasPermission(user,map[type]||'');
}

function aloraTeamsRequireLegacyGuard(){
  // Deny unsafe legacy APIs automatically once real multi-team data is activated.
  // This cannot be bypassed by omitting an environment variable.
  try{return !!db.prepare('SELECT 1 FROM alora_teams LIMIT 1').get() && (!!db.prepare('SELECT 1 FROM alora_accounts LIMIT 1').get() || !!db.prepare('SELECT 1 FROM alora_team_members LIMIT 1').get());}catch{return false;}
}

function visibleOrderClause(user, alias='o'){
  const p = alias ? alias + '.' : '';
  if(hasPermission(user,'orders.view_all')) return {sql:'1=1',params:[]};
  if(!hasPermission(user,'orders.view_role')) return {sql:'0=1',params:[]};
  switch(user.role){
    case 'ORDER':
      return {sql:`${p}current_stage IN ('NEW','READY_FOR_PRODUCTION','ASSIGNED')`,params:[]};
    case 'PRODUCTION':
      return {sql:`${p}current_stage IN ('NEW','READY_FOR_PRODUCTION','ASSIGNED','IN_PRODUCTION','QC_FAILED') AND (${p}assigned_worker_id IS NULL OR ${p}assigned_worker_id=?)`,params:[user.id]};
    case 'QC':
      return {sql:`${p}current_stage IN ('WAITING_QC','QC_PASSED','QC_FAILED')`,params:[]};
    case 'PACKING':
      return {sql:`${p}current_stage IN ('QC_PASSED','PACKED')`,params:[]};
    case 'SHIPPING':
      return {sql:`${p}current_stage IN ('PACKED','TRACKING_CREATED','SHIPPED')`,params:[]};
    case 'WAREHOUSE':
      return {sql:`${p}current_stage IN ('NEW','READY_FOR_PRODUCTION','ASSIGNED','IN_PRODUCTION','QC_FAILED','WAITING_QC')`,params:[]};
    case 'CSKH':
    case 'ADMIN':
    case 'MANAGER':
    default:
      return {sql:'1=1',params:[]};
  }
}

function userCanViewOrder(user, orderId, {includeDeleted=false}={}){
  const vis=visibleOrderClause(user,'o'),key=normalizeOrderId(orderId);
  return !!db.prepare(`SELECT o.id FROM orders o WHERE (o.order_id=? OR o.order_key=?) ${includeDeleted?'':'AND o.deleted_at IS NULL'} AND ${vis.sql} LIMIT 1`).get(orderId,key,...vis.params);
}

// Financial figures are only exposed by /api/sales/*; never add them to order/dashboard payloads.
const SALES_CURRENCIES=Object.freeze(['USD','VND','EUR','GBP','AUD','CAD']);
function validSaleDate(v){if(!/^\d{4}-\d{2}-\d{2}$/.test(v))return false;const d=new Date(v+'T00:00:00Z');return !Number.isNaN(d.getTime())&&d.toISOString().slice(0,10)===v;}
function priceToCents(raw,label='Giá'){
  if(raw===null||raw===undefined||raw==='')return null;
  if(typeof raw!=='string'&&typeof raw!=='number')throw badRequest(label+' phải là số tiền hợp lệ.','PRICE_FORMAT');
  const value=String(raw).trim();
  if(!/^(?:0|[1-9]\d{0,9})(?:\.\d{1,2})?$/.test(value))throw badRequest(label+' cần lớn hơn hoặc bằng 0, tối đa 2 chữ số thập phân.','PRICE_FORMAT');
  const [whole,fraction='']=value.split('.');const cents=Number(whole)*100+Number((fraction+'00').slice(0,2));
  if(!Number.isSafeInteger(cents))throw badRequest(label+' vượt giới hạn.','PRICE_FORMAT');
  return cents;
}
function findSaleOrder(orderId){return db.prepare('SELECT id,order_id,customer,created_at FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NULL LIMIT 1').get(orderId,normalizeOrderId(orderId));}
function salesPricingDetails(o){
  const f=db.prepare('SELECT sale_date saleDate,currency,order_total_cents orderTotalCents FROM order_finance WHERE order_db_id=?').get(o.id)||{};
  const items=db.prepare('SELECT oi.id,oi.sku,oi.product_name productName,oi.qty,p.unit_price_cents unitPriceCents FROM order_items oi LEFT JOIN order_item_prices p ON p.order_item_id=oi.id WHERE oi.order_db_id=? AND oi.active=1 ORDER BY oi.id').all(o.id);
  const value={orderId:o.order_id,customer:o.customer,createdAt:o.created_at,createdDateVietnam:db.prepare("SELECT DATE(?,'+7 hours') day").get(o.created_at).day,saleDate:f.saleDate||'',currency:f.currency||'USD',orderTotalCents:f.orderTotalCents??null,items};return {...value,pricingVersion:sha256(JSON.stringify(value))};
}
function safeFinanceRows(statement,args){statement.setReadBigInts(true);return statement.all(...args).map(row=>Object.fromEntries(Object.entries(row).map(([k,v])=>{if(typeof v!=='bigint')return[k,v];if(v>BigInt(Number.MAX_SAFE_INTEGER)||v<BigInt(Number.MIN_SAFE_INTEGER))throw badRequest('Tổng kỳ vượt giới hạn; chia kỳ dữ liệu để báo cáo.','REPORT_RANGE');return[k,Number(v)];})));}
function salesMonthlyReport(month,currency){
  const monthStart=month+'-01'; const nextMonth=new Date(monthStart+'T00:00:00Z');nextMonth.setUTCMonth(nextMonth.getUTCMonth()+1);const monthEnd=nextMonth.toISOString().slice(0,10);
  const scoped=`o.deleted_at IS NULL AND COALESCE(f.sale_date,DATE(o.created_at,'+7 hours'))>=? AND COALESCE(f.sale_date,DATE(o.created_at,'+7 hours'))<? AND COALESCE(f.currency,'USD')=?`;
  const rows=safeFinanceRows(db.prepare(`SELECT o.id,o.order_id orderId,o.customer,COALESCE(f.sale_date,DATE(o.created_at,'+7 hours')) saleDate,f.order_total_cents orderTotalCents,COUNT(oi.id) itemCount,COUNT(p.unit_price_cents) pricedItemCount,COALESCE(SUM(oi.qty),0) totalQty,GROUP_CONCAT(DISTINCT oi.sku) skus,COALESCE(SUM(CASE WHEN p.unit_price_cents IS NOT NULL THEN oi.qty*p.unit_price_cents ELSE 0 END),0) lineTotalCents FROM orders o LEFT JOIN order_finance f ON f.order_db_id=o.id LEFT JOIN order_items oi ON oi.order_db_id=o.id AND oi.active=1 LEFT JOIN order_item_prices p ON p.order_item_id=oi.id WHERE ${scoped} GROUP BY o.id ORDER BY saleDate DESC,o.id DESC`),[monthStart,monthEnd,currency]);
  const orders=rows.map(r=>({...r,amountCents:r.orderTotalCents!==null?r.orderTotalCents:(r.itemCount>0&&r.itemCount===r.pricedItemCount?r.lineTotalCents:null),amountSource:r.orderTotalCents!==null?'ORDER':(r.itemCount>0&&r.itemCount===r.pricedItemCount?'ITEMS':'MISSING')}));
  const totalCents=orders.reduce((n,r)=>n+(r.amountCents??0),0);if(!Number.isSafeInteger(totalCents))throw badRequest('Tổng tháng vượt giới hạn an toàn.','REPORT_RANGE');
  const topSkus=safeFinanceRows(db.prepare(`SELECT COALESCE(NULLIF(UPPER(TRIM(oi.sku)),''),'(Chưa có SKU)') sku,MAX(oi.product_name) productName,SUM(oi.qty) qty,COUNT(DISTINCT o.id) orderCount,COUNT(p.unit_price_cents) pricedLines,COUNT(oi.id) totalLines,SUM(CASE WHEN p.unit_price_cents IS NOT NULL THEN oi.qty*p.unit_price_cents ELSE 0 END) revenueCents FROM orders o JOIN order_items oi ON oi.order_db_id=o.id AND oi.active=1 LEFT JOIN order_finance f ON f.order_db_id=o.id LEFT JOIN order_item_prices p ON p.order_item_id=oi.id WHERE ${scoped} GROUP BY COALESCE(NULLIF(UPPER(TRIM(oi.sku)),''),'(Chưa có SKU)') ORDER BY qty DESC,sku ASC LIMIT 5000`),[monthStart,monthEnd,currency]);
  // 12 calendar months ending at selected month: aggregate only orders with verified totals.
  const chart=[];const base=new Date(monthStart+'T00:00:00Z');for(let offset=11;offset>=0;offset--){const d=new Date(base);d.setUTCMonth(d.getUTCMonth()-offset);const m=d.toISOString().slice(0,7),n=new Date(d);n.setUTCMonth(n.getUTCMonth()+1);const bounds=[m+'-01',n.toISOString().slice(0,10),currency];const r=safeFinanceRows(db.prepare(`SELECT o.id,f.order_total_cents orderTotalCents,COUNT(oi.id) itemCount,COUNT(p.unit_price_cents) pricedItemCount,COALESCE(SUM(oi.qty),0) totalQty,GROUP_CONCAT(DISTINCT oi.sku) skus,COALESCE(SUM(CASE WHEN p.unit_price_cents IS NOT NULL THEN oi.qty*p.unit_price_cents ELSE 0 END),0) lineTotalCents FROM orders o LEFT JOIN order_finance f ON f.order_db_id=o.id LEFT JOIN order_items oi ON oi.order_db_id=o.id AND oi.active=1 LEFT JOIN order_item_prices p ON p.order_item_id=oi.id WHERE ${scoped} GROUP BY o.id`),[...bounds]);let revenueCents=0,unpriced=0;for(const x of r){const amount=x.orderTotalCents!==null?x.orderTotalCents:(x.itemCount>0&&x.itemCount===x.pricedItemCount?x.lineTotalCents:null);if(amount===null)unpriced++;else revenueCents+=amount;}chart.push({month:m,orders:r.length,revenueCents,unpriced});}
  const dailyChart=Array.from({length:new Date(Number(month.slice(0,4)),Number(month.slice(5,7)),0).getDate()},(_,i)=>{const day=month+'-'+String(i+1).padStart(2,'0'),list=orders.filter(o=>o.saleDate===day);return {day,orders:list.length,revenueCents:list.reduce((n,o)=>n+(o.amountCents??0),0)};});
  return {dailyChart,averageCents:orders.some(o=>o.amountCents!==null)?Math.round(totalCents/orders.filter(o=>o.amountCents!==null).length):0,month,currency,totalOrders:orders.length,pricedOrders:orders.filter(x=>x.amountCents!==null).length,unpricedOrders:orders.filter(x=>x.amountCents===null).length,totalCents,quantity:orders.reduce((s,x)=>s+Number(x.totalQty||0),0),orders,topSkus,chart};
}

const quickScanService=createQuickScanService({db,hasPermission,nextQuickAction,visualStage,normalizeOrderId,cleanText,sha256,withTransaction,nowIso,syncItemWorkflowStage,audit,orderObject,DEFECT_CATEGORIES});
const operationsService=createOperationsService({db,visibleOrderClause,vietnamToday});
const monthSnapshots=createMonthSnapshotService({db,sha256,salesMonthlyReport,withTransaction,audit,nowIso,cleanText});
function salesSheetPreviewHash(text,parsed){const r=parsed||parseSalesSheet(text),ids=[...new Set(r.rows.map(x=>x.orderDbId))].sort((a,b)=>a-b);return sha256(JSON.stringify({text,versions:ids.map(id=>salesPricingDetails(db.prepare('SELECT id,order_id,customer,created_at FROM orders WHERE id=?').get(id)).pricingVersion)}));}

// TSV/CSV price import from a trusted user's pasted Google Sheets table.
// Strict validation and atomic writes; never create orders implicitly from a price sheet.
function parseSalesSheet(text){
  const matrix=parseDelimited(String(text||'').replace(/^\uFEFF/,''),detectDelimiter(text));
  if(matrix.length<2)throw badRequest('Dán bảng có tiêu đề và ít nhất một dòng dữ liệu.','SALES_IMPORT_EMPTY');
  if(matrix.length>2001)throw badRequest('Mỗi lần nhập tối đa 2.000 dòng giá.','SALES_IMPORT_LIMIT');
  const h=matrix[0].map(v=>String(v||'').trim()),cols={
    orderId:findMappedHeader(h,['order id','orderid','mã đơn','ma don']),
    sku:findMappedHeader(h,['sku','mã sku','ma sku']),
    unitPrice:findMappedHeader(h,['unit price','price per unit','sku price','giá sku','gia sku','đơn giá','don gia']),
    total:findMappedHeader(h,['order total','total price','amount','order amount','giá trị đơn','gia tri don','tổng tiền đơn','tong tien don']),
    saleDate:findMappedHeader(h,['sale date','order date','ngày bán','ngay ban','ngày đặt hàng','ngay dat hang']),
    currency:findMappedHeader(h,['currency','tiền tệ','tien te'])};
  if(cols.orderId<0|| (cols.total<0&&cols.unitPrice<0))throw badRequest('Bắt buộc có cột Order ID và ít nhất một cột Order Total hoặc Unit Price.','SALES_IMPORT_COLUMNS');
  const rows=[], errors=[],byOrder=new Map(),seen=new Map();
  for(let i=1;i<matrix.length;i++){
    const cells=matrix[i];if(!cells.some(v=>String(v||'').trim()))continue;
    const get=k=>cols[k]<0?'':String(cells[cols[k]]??'').trim();
    try{
      const orderId=cleanText(get('orderId'),120,'Order ID');if(!orderId)throw badRequest('Thiếu Order ID','SALES_IMPORT_ORDER');
      const o=findSaleOrder(orderId);if(!o)throw notFound(`Order #${orderId} chưa tồn tại. Hãy nhập đơn trước khi nhập giá.`,'SALES_IMPORT_NOT_FOUND');
      const f=db.prepare('SELECT currency,order_total_cents amount FROM order_finance WHERE order_db_id=?').get(o.id);
      const currency=(get('currency')||f?.currency||'USD').toUpperCase();
      if(!SALES_CURRENCIES.includes(currency))throw badRequest('Loại tiền không hợp lệ.','SALES_IMPORT_CURRENCY');
      if(f&&f.currency!==currency&&(f.amount!==null||db.prepare('SELECT 1 FROM order_item_prices ip JOIN order_items oi ON oi.id=ip.order_item_id WHERE oi.order_db_id=? AND ip.unit_price_cents IS NOT NULL LIMIT 1').get(o.id)))throw badRequest('Order đã có giá bằng '+f.currency+'. Hãy sửa loại tiền thủ công trước khi nhập lại bảng giá.','SALES_IMPORT_CURRENCY_CONFLICT');
      const saleDate=get('saleDate');if(saleDate&&!validSaleDate(saleDate))throw badRequest('Ngày bán phải ở định dạng YYYY-MM-DD.','SALES_IMPORT_DATE');
      const sku=get('sku');let itemId=null;
      if(sku){const matches=db.prepare('SELECT id FROM order_items WHERE order_db_id=? AND active=1 AND lower(trim(sku))=lower(trim(?))').all(o.id,sku);if(matches.length!==1)throw badRequest(`SKU ${sku} không tồn tại hoặc bị trùng trong Order.`,'SALES_IMPORT_SKU');itemId=matches[0].id;}
      if(get('unitPrice')&&!itemId)throw badRequest('Nhập giá SKU phải có cột SKU hợp lệ.','SALES_IMPORT_SKU_REQUIRED');
      const unitPriceCents=priceToCents(get('unitPrice'),'Giá SKU');if(unitPriceCents!==null&&unitPriceCents*Number(db.prepare('SELECT qty FROM order_items WHERE id=?').get(itemId).qty)>100_000_000_000_000)throw badRequest('Giá trị dòng vượt giới hạn an toàn.','PRICE_RANGE');const totalCents=priceToCents(get('total'),'Giá trị đơn');
      if(unitPriceCents===null&&totalCents===null)throw badRequest('Dòng không có giá bán.','SALES_IMPORT_PRICE_EMPTY');
      const prev=byOrder.get(o.id);
      if(prev&&(prev.currency!==currency||(saleDate&&prev.saleDate&&prev.saleDate!==saleDate)||(totalCents!==null&&prev.totalCents!==null&&prev.totalCents!==totalCents)))throw badRequest('Các dòng cùng Order có ngày bán, loại tiền hoặc tổng đơn mâu thuẫn.','SALES_IMPORT_CONFLICT');
      const key=o.id+'|'+(itemId||'ORDER');
      if(seen.has(key)&&seen.get(key)!==unitPriceCents)throw badRequest('Các dòng cùng Order + SKU có giá khác nhau.','SALES_IMPORT_DUPLICATE');
      seen.set(key,unitPriceCents);
      byOrder.set(o.id,{currency,saleDate:saleDate||prev?.saleDate||'',totalCents:totalCents??prev?.totalCents??null});
      rows.push({rowNumber:i+1,orderDbId:o.id,orderId:o.order_id,itemId,sku:sku||'',unitPriceCents,totalCents,saleDate,currency});
    }catch(e){errors.push({rowNumber:i+1,message:e.message})}
  }
  if(!rows.length&&!errors.length)throw badRequest('Không có dòng giá hợp lệ.','SALES_IMPORT_EMPTY');
  return {rows,errors};
}
function commitSalesSheet(text,user){
  const parsed=parseSalesSheet(text);if(parsed.errors.length)throw badRequest('Bảng có dòng sai. Sửa rồi Preview lại; chưa có gì được ghi.','SALES_IMPORT_INVALID');
  return withTransaction(()=>{
    const now=nowIso(),byOrder=new Map();
    for(const row of parsed.rows){let g=byOrder.get(row.orderDbId);if(!g){g={orderId:row.orderId,orderDbId:row.orderDbId,currency:row.currency,saleDate:null,totalCents:null,prices:new Map()};byOrder.set(row.orderDbId,g)}
      if(row.saleDate)g.saleDate=row.saleDate;if(row.totalCents!==null)g.totalCents=row.totalCents;
      if(row.itemId&&row.unitPriceCents!==null)g.prices.set(row.itemId,row.unitPriceCents);
    }
    for(const g of byOrder.values()){
      db.prepare(`INSERT INTO order_finance(order_db_id,sale_date,currency,order_total_cents,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(order_db_id) DO UPDATE SET sale_date=COALESCE(excluded.sale_date,order_finance.sale_date),currency=excluded.currency,order_total_cents=COALESCE(excluded.order_total_cents,order_finance.order_total_cents),updated_at=excluded.updated_at`).run(g.orderDbId,g.saleDate,g.currency,g.totalCents,now);
      for(const [id,cents] of g.prices)db.prepare('INSERT INTO order_item_prices(order_item_id,unit_price_cents) VALUES(?,?) ON CONFLICT(order_item_id) DO UPDATE SET unit_price_cents=excluded.unit_price_cents').run(id,cents);
      audit({orderId:g.orderId,orderDbId:g.orderDbId,action:'SALES_BULK_PRICING_UPDATED',user,detail:`${g.prices.size} SKU(s); currency=${g.currency}; sale date supplied=${!!g.saleDate}; total supplied=${g.totalCents!==null}`});
    }
    return {ordersUpdated:byOrder.size,rowsApplied:parsed.rows.length};
  });
}

function dashboardForUser(user){
  const c = stage => db.prepare('SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL AND current_stage=?').get(stage).c;
  const sumStages = stages => stages.reduce((n,s)=>n+c(s),0);
  const countMine = (stages) => {
    const qs=stages.map(()=>'?').join(',');
    if(user.role==='PRODUCTION') return db.prepare(`SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL AND current_stage IN (${qs}) AND (assigned_worker_id IS NULL OR assigned_worker_id=?)`).get(...stages,user.id).c;
    return db.prepare(`SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL AND current_stage IN (${qs})`).get(...stages).c;
  };
  let summary;
  if(user.role==='ORDER') summary=[
    {label:'Chờ kê đơn',value:c('NEW'),hint:'Đơn mới chưa xác nhận'},
    {label:'Sẵn sàng sản xuất',value:c('READY_FOR_PRODUCTION'),hint:'Đã kê xong'},
    {label:'Đã phân công',value:c('ASSIGNED'),hint:'Đã giao người làm'},
    {label:'Việc của bộ phận',value:countMine(['NEW','READY_FOR_PRODUCTION','ASSIGNED']),hint:'Kê đơn'}
  ];
  else if(user.role==='PRODUCTION') summary=[
    {label:'Chờ sản xuất',value:countMine(['READY_FOR_PRODUCTION','ASSIGNED','QC_FAILED']),hint:'Có thể bắt đầu'},
    {label:'Đang sản xuất',value:countMine(['IN_PRODUCTION']),hint:'Đang làm'},
    {label:'Kiểm Tra trả lại',value:countMine(['QC_FAILED']),hint:'Cần làm lại'},
    {label:'Việc của tôi',value:countMine(['READY_FOR_PRODUCTION','ASSIGNED','IN_PRODUCTION','QC_FAILED']),hint:'Production'}
  ];
  else if(user.role==='QC') summary=[
    {label:'Chờ Kiểm Tra',value:c('WAITING_QC'),hint:'Sẵn sàng kiểm'},
    {label:'Kiểm Tra PASS',value:c('QC_PASSED'),hint:'Đã đạt'},
    {label:'Kiểm Tra FAIL',value:c('QC_FAILED'),hint:'Đã trả sản xuất'},
    {label:'Việc Kiểm Tra',value:countMine(['WAITING_QC','QC_PASSED','QC_FAILED']),hint:'Quality Control'}
  ];
  else if(user.role==='PACKING') summary=[
    {label:'Chờ đóng gói',value:c('QC_PASSED'),hint:'Kiểm Tra đã đạt'},
    {label:'Đã đóng gói',value:c('PACKED'),hint:'Chờ Shipping'},
    {label:'Đã tạo Tracking',value:c('TRACKING_CREATED'),hint:'Qua bộ phận tiếp'},
    {label:'Việc Packing',value:countMine(['QC_PASSED','PACKED']),hint:'Packing'}
  ];
  else if(user.role==='SHIPPING') summary=[
    {label:'Chờ Tracking',value:c('PACKED'),hint:'Đã đóng gói'},
    {label:'Có Tracking',value:c('TRACKING_CREATED'),hint:'Chờ xuất'},
    {label:'Đã xuất',value:c('SHIPPED'),hint:'Hoàn tất Shipping'},
    {label:'Việc Shipping',value:countMine(['PACKED','TRACKING_CREATED','SHIPPED']),hint:'Shipping'}
  ];
  else if(user.role==='WAREHOUSE') summary=[
    {label:'Loại vải',value:db.prepare('SELECT COUNT(*) c FROM inventory_materials WHERE active=1').get().c,hint:'Kho vải'},
    {label:'Sắp hết',value:db.prepare('SELECT COUNT(*) c FROM inventory_materials WHERE active=1 AND current_stock<=min_stock').get().c,hint:'Cần nhập bổ sung'},
    {label:'Hết kho',value:db.prepare('SELECT COUNT(*) c FROM inventory_materials WHERE active=1 AND current_stock<=0').get().c,hint:'Tồn bằng 0'},
    {label:'Order cần vải',value:db.prepare(`SELECT COUNT(DISTINCT o.id) c FROM orders o JOIN order_items oi ON oi.order_db_id=o.id AND oi.active=1 WHERE o.deleted_at IS NULL AND o.current_stage!='SHIPPED' AND oi.inventory_material_id IS NOT NULL AND COALESCE(oi.material_usage_per_unit,0)>0`).get().c,hint:'Có định mức kho'}
  ];
  else if(user.role==='CSKH') summary=[
    {label:'Tổng đơn',value:db.prepare('SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL').get().c,hint:'Tra cứu'},
    {label:'Đang sản xuất',value:c('IN_PRODUCTION'),hint:'Khách có thể hỏi'},
    {label:'Chờ Kiểm Tra',value:c('WAITING_QC'),hint:'Chưa đóng gói'},
    {label:'Đã xuất',value:c('SHIPPED'),hint:'Có thể báo khách'}
  ];
  else summary=[
    {label:'Tổng đơn',value:db.prepare('SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL').get().c,hint:'Toàn hệ thống'},
    {label:'Đang sản xuất',value:c('IN_PRODUCTION'),hint:'Production'},
    {label:'Chờ Kiểm Tra',value:c('WAITING_QC'),hint:'Quality Control'},
    {label:'Đã xuất',value:c('SHIPPED'),hint:'Shipping'}
  ];
  const vis=visibleOrderClause(user,'o');
  const attention=db.prepare(`SELECT o.order_id orderId,o.customer,o.need_by needBy,o.priority,o.current_stage currentStage,o.tracking_number trackingNumber,o.updated_at updatedAt,u.full_name assignedWorker,(SELECT product_name FROM order_items oi WHERE oi.order_db_id=o.id AND oi.active=1 ORDER BY oi.id LIMIT 1) productName,(SELECT COUNT(*) FROM order_items oi WHERE oi.order_db_id=o.id AND oi.active=1) itemCount,(SELECT COALESCE(SUM(qty),0) FROM order_items oi WHERE oi.order_db_id=o.id AND oi.active=1) totalQty FROM orders o LEFT JOIN users u ON u.id=o.assigned_worker_id WHERE o.deleted_at IS NULL AND ${vis.sql} ORDER BY CASE WHEN COALESCE(o.need_by_iso,'')='' THEN 1 ELSE 0 END,o.need_by_iso ASC,CASE o.priority WHEN 'EXPRESS' THEN 0 WHEN 'HIGH' THEN 1 ELSE 2 END,o.id DESC LIMIT 50`).all(...vis.params);

  // V13 dashboard analytics: aggregate indicators are calculated directly from active orders.
  const totalOrders=db.prepare('SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL').get().c;
  const [vnStart,vnEnd]=vietnamDayRangeUtc(); const todayCreated=db.prepare(`SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL AND created_at>=? AND created_at<?`).get(vnStart,vnEnd).c;
  const stageCounts={};
  for(const stage of STAGE_ORDER) stageCounts[stage]=c(stage);
  const completed=stageCounts.SHIPPED||0;
  const completionRate=totalOrders?Math.round((completed/totalOrders)*100):0;
  const today=vietnamToday(),end5=(()=>{const d=new Date(today+'T00:00:00Z');d.setUTCDate(d.getUTCDate()+5);return d.toISOString().slice(0,10)})();
  const dueSoon5=db.prepare(`SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL AND current_stage!='SHIPPED' AND COALESCE(need_by_iso,'')<>'' AND need_by_iso>=? AND need_by_iso<=?`).get(today,end5).c;
  const overdue=db.prepare(`SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL AND current_stage!='SHIPPED' AND COALESCE(need_by_iso,'')<>'' AND need_by_iso<?`).get(today).c;
  const overview={
    totalOrders,
    todayCreated,
    inProduction:stageCounts.IN_PRODUCTION||0,
    waitingQc:stageCounts.WAITING_QC||0,
    packed:stageCounts.PACKED||0,
    shipped:stageCounts.SHIPPED||0,
    qcFailed:stageCounts.QC_FAILED||0,
    dueSoon5,
    overdue,
    completionRate
  };
  const upcoming=db.prepare(`SELECT o.order_id orderId,o.customer,o.need_by needBy FROM orders o WHERE o.deleted_at IS NULL AND ${vis.sql} AND o.current_stage!='SHIPPED' AND o.need_by_iso>=? AND o.need_by_iso<=? ORDER BY o.need_by_iso,CASE o.priority WHEN 'EXPRESS' THEN 0 WHEN 'HIGH' THEN 1 ELSE 2 END,o.id LIMIT 4`).all(...vis.params,today,end5);
  return {summary,attention,upcoming,overview,stageCounts,scope:'COMPANY',attentionLimit:50};
}


const IMPORT_HEADER_MAP = {
  orderId: ['order id','order','order number','orderid','mã đơn','ma don'],
  customer: ['customer','customer name','buyer','buyer name','name','khách hàng','khach hang'],
  productName: ['product','product name','item','title','sản phẩm','san pham'],
  qty: ['qty','quantity','sl','số lượng','so luong'],
  sku: ['sku'],
  needBy: ['need by','need date','wedding date','date needed','ngày cần','ngay can'],
  material: ['material','color / material','color','fabric','chất liệu','chat lieu'],
  worker: ['worker','assigned to','người làm','nguoi lam','nhân viên','nhan vien'],
  personalization: ['personalization','notes','note','ghi chú','ghi chu'],
  priority: ['priority','ưu tiên','uu tien'],
  currentStage: ['current stage','status','trạng thái','trang thai'],
  trackingNumber: ['tracking number','tracking','tracking code','mã vận chuyển','ma van chuyen']
};

function dataQualityReport(){
  const issues=[];
  const add=(code,count,sample=[])=>{if(Number(count)>0)issues.push({code,count:Number(count),sample});};
  let r=db.prepare(`SELECT COUNT(*) c FROM orders WHERE priority NOT IN ('NORMAL','HIGH','EXPRESS') OR priority IS NULL`).get();add('ORDER_PRIORITY_INVALID',r.c,db.prepare(`SELECT order_id orderId,priority FROM orders WHERE priority NOT IN ('NORMAL','HIGH','EXPRESS') OR priority IS NULL LIMIT 20`).all());
  r=db.prepare(`SELECT COUNT(*) c FROM orders WHERE current_stage NOT IN ('NEW','READY_FOR_PRODUCTION','ASSIGNED','IN_PRODUCTION','WAITING_QC','QC_FAILED','QC_PASSED','PACKED','TRACKING_CREATED','SHIPPED') OR current_stage IS NULL`).get();add('ORDER_STAGE_INVALID',r.c,db.prepare(`SELECT order_id orderId,current_stage currentStage FROM orders WHERE current_stage NOT IN ('NEW','READY_FOR_PRODUCTION','ASSIGNED','IN_PRODUCTION','WAITING_QC','QC_FAILED','QC_PASSED','PACKED','TRACKING_CREATED','SHIPPED') OR current_stage IS NULL LIMIT 20`).all());
  r=db.prepare(`SELECT COUNT(*) c FROM orders WHERE order_key IS NULL OR TRIM(order_key)=''`).get();add('ORDER_KEY_MISSING',r.c);
  r=db.prepare(`SELECT COUNT(*) c FROM order_items WHERE active=1 AND (item_key IS NULL OR TRIM(item_key)='')`).get();add('ITEM_KEY_MISSING',r.c);
  r=db.prepare(`SELECT COUNT(*) c FROM inventory_materials WHERE current_stock_milli IS NULL OR min_stock_milli IS NULL OR ABS(ROUND(current_stock*1000)-current_stock_milli)>0 OR ABS(ROUND(min_stock*1000)-min_stock_milli)>0`).get();add('INVENTORY_MILLI_MISMATCH',r.c,db.prepare(`SELECT id,code,current_stock currentStock,current_stock_milli currentStockMilli FROM inventory_materials WHERE current_stock_milli IS NULL OR min_stock_milli IS NULL OR ABS(ROUND(current_stock*1000)-current_stock_milli)>0 OR ABS(ROUND(min_stock*1000)-min_stock_milli)>0 LIMIT 20`).all());
  r=db.prepare(`SELECT COUNT(*) c FROM users WHERE role NOT IN ('ADMIN','MANAGER','ORDER','PRODUCTION','QC','PACKING','SHIPPING','WAREHOUSE','CSKH','PARTNER') AND deleted_at IS NULL`).get();add('USER_ROLE_INVALID',r.c);
  return {ok:issues.length===0,issueCount:issues.reduce((a,x)=>a+x.count,0),categories:issues.length,issues};
}

function normHeader(v){
  return String(v||'').trim().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/đ/g,'d').replace(/\s+/g,' ');
}
function findMappedHeader(headers, candidates){
  const norm=headers.map(normHeader);
  for(const c of candidates){
    const idx=norm.indexOf(normHeader(c));
    if(idx>=0)return idx;
  }
  return -1;
}
function parseDelimited(text, delimiter=','){
  text=String(text||'').replace(/^\uFEFF/,'');
  const rows=[]; let row=[], field='', inQuotes=false;
  for(let i=0;i<text.length;i++){
    const ch=text[i];
    if(inQuotes){
      if(ch==='"' && text[i+1]==='"'){ field+='"'; i++; }
      else if(ch==='"') inQuotes=false;
      else field+=ch;
    }else{
      if(ch==='"') inQuotes=true;
      else if(ch===delimiter){ row.push(field); field=''; }
      else if(ch==='\n'){ row.push(field.replace(/\r$/,'')); rows.push(row); row=[]; field=''; }
      else field+=ch;
    }
  }
  if(field.length || row.length){ row.push(field.replace(/\r$/,'')); rows.push(row); }
  return rows.filter(r=>r.some(v=>String(v||'').trim()!==''));
}
function detectDelimiter(text){
  const first=String(text||'').split(/\r?\n/)[0]||'';
  const tabs=(first.match(/\t/g)||[]).length;
  const commas=(first.match(/,/g)||[]).length;
  return tabs>commas?'\t':',';
}
function normalizeImportedRows(text){
  const delim=detectDelimiter(text);
  const matrix=parseDelimited(text,delim);
  if(matrix.length<2)throw badRequest('Không tìm thấy dữ liệu. Hãy dán ít nhất 2 dòng: 1 dòng tiêu đề + 1 dòng dữ liệu.','IMPORT_EMPTY');
  const headers=matrix[0].map(v=>String(v||'').trim());
  const idx={};
  for(const [key,cands] of Object.entries(IMPORT_HEADER_MAP))idx[key]=findMappedHeader(headers,cands);
  if(idx.orderId<0)throw badRequest('Không tìm thấy cột Order ID. Hãy dán trực tiếp bảng từ Google Sheets hoặc dùng CSV có dấu phẩy.','IMPORT_ORDER_HEADER');

  const rows=[];
  for(let r=1;r<matrix.length;r++){
    const src=matrix[r];
    const get=k=>idx[k]>=0?String(src[idx[k]]??'').trim():'';
    const orderId=get('orderId');
    if(!orderId)continue;
    const qtyRaw=get('qty');
    const stageText=normHeader(get('currentStage'));
    const stageAliases={
      '':'NEW','new':'NEW','moi':'NEW','mới':'NEW',
      'ready_for_production':'READY_FOR_PRODUCTION','ready for production':'READY_FOR_PRODUCTION','da ke don':'READY_FOR_PRODUCTION','đã kê đơn':'READY_FOR_PRODUCTION','san sang san xuat':'READY_FOR_PRODUCTION','sẵn sàng sản xuất':'READY_FOR_PRODUCTION',
      'assigned':'ASSIGNED','da phan':'ASSIGNED','đã phân':'ASSIGNED','da phan cong':'ASSIGNED','đã phân công':'ASSIGNED',
      'in_production':'IN_PRODUCTION','in production':'IN_PRODUCTION','dang san xuat':'IN_PRODUCTION','đang sản xuất':'IN_PRODUCTION','dang lam':'IN_PRODUCTION','đang làm':'IN_PRODUCTION',
      'waiting_qc':'WAITING_QC','waiting qc':'WAITING_QC','cho qc':'WAITING_QC','chờ qc':'WAITING_QC',
      'qc_failed':'QC_FAILED','qc failed':'QC_FAILED','qc fail':'QC_FAILED','qc loi':'QC_FAILED','qc lỗi':'QC_FAILED',
      'qc_passed':'QC_PASSED','qc passed':'QC_PASSED','qc pass':'QC_PASSED','qc dat':'QC_PASSED','qc đạt':'QC_PASSED',
      'packed':'PACKED','da dong goi':'PACKED','đã đóng gói':'PACKED',
      'tracking_created':'TRACKING_CREATED','tracking created':'TRACKING_CREATED','da co tracking':'TRACKING_CREATED','đã có tracking':'TRACKING_CREATED',
      'shipped':'SHIPPED','da xuat':'SHIPPED','đã xuất':'SHIPPED','da xuat hang':'SHIPPED','đã xuất hàng':'SHIPPED'
    };
    const importedStage=stageText?stageAliases[stageText]:undefined;if(stageText&&!importedStage)throw badRequest('Trạng thái không hợp lệ ở dòng '+(r+1),'IMPORT_STAGE');
    const priorityText=normHeader(get('priority'));
    const importedPriority=priorityText?(['express','gap','urgent'].includes(priorityText)?'EXPRESS':['high','cao'].includes(priorityText)?'HIGH':['normal','binh thuong'].includes(priorityText)?'NORMAL':null):undefined;if(importedPriority===null)throw badRequest('Ưu tiên không hợp lệ ở dòng '+(r+1),'IMPORT_PRIORITY');
    rows.push({
      rowNumber:r+1,
      orderId,
      customer:get('customer'),
      productName:get('productName')||'Sản phẩm',
      qty:qtyRaw===''?1:Number(qtyRaw),
      sku:get('sku'),
      needBy:get('needBy'),
      material:get('material'),
      worker:get('worker'),
      personalization:get('personalization'),
      priority:importedPriority,
      currentStage:importedStage,
      trackingNumber:get('trackingNumber')
    });
  }
  return {headers,rows};
}
function extractGoogleSheetCsvUrl(input){
  const raw=String(input||'').trim();
  if(!raw)throw new Error('Chưa nhập link Google Sheets.');
  let parsed;
  try{ parsed=new URL(raw); }catch{ throw new Error('Link Google Sheets không hợp lệ.'); }
  if(parsed.protocol!=='https:' || parsed.hostname!=='docs.google.com') throw new Error('Chỉ chấp nhận link Google Sheets chính thức từ https://docs.google.com.');
  const m=parsed.pathname.match(/\/spreadsheets\/d\/([^/]+)/);
  if(!m)throw new Error('Link Google Sheets không hợp lệ.');
  const gid=parsed.searchParams.get('gid') || (raw.match(/[?#&]gid=(\d+)/)||[])[1] || '0';
  return `https://docs.google.com/spreadsheets/d/${encodeURIComponent(m[1])}/export?format=csv&gid=${encodeURIComponent(gid)}`;
}
async function fetchSheetText(sheetUrl){
  if(process.env.ALLOW_PUBLIC_SHEET_IMPORT!=='1')throw new Error('Import bằng link Google Sheets public đang tắt để bảo vệ dữ liệu. Hãy dùng Copy/Paste hoặc bật ALLOW_PUBLIC_SHEET_IMPORT=1 sau khi hiểu rủi ro.');
  const url=extractGoogleSheetCsvUrl(sheetUrl);
  const r=await fetch(url,{redirect:'follow',signal:AbortSignal.timeout(10000)});
  let finalHost='';
  try{ finalHost=new URL(r.url).hostname; }catch{}
  const googleHost = finalHost==='docs.google.com' || finalHost==='googleusercontent.com' || finalHost.endsWith('.googleusercontent.com');
  if(!googleHost) throw new Error('Google Sheets redirect không hợp lệ.');
  if(!r.ok)throw new Error(`Không đọc được Google Sheets (${r.status}). Hãy đặt quyền "Bất kỳ ai có liên kết - Người xem" hoặc dùng cách Copy/Paste.`);
  const len=Number(r.headers.get('content-length')||0);
  if(len>10_000_000)throw new Error('Google Sheets quá lớn. Giới hạn tải là 10 MB.');
  const text=await r.text();
  if(text.length>10_000_000)throw new Error('Google Sheets quá lớn. Giới hạn tải là 10 MB.');
  if(/<html/i.test(text.slice(0,500)))throw new Error('Google trả về trang đăng nhập thay vì dữ liệu. Hãy chia sẻ Sheet cho "Bất kỳ ai có liên kết - Người xem".');
  return text;
}

const INVENTORY_IMPORT_HEADER_MAP = {
  orderId: ['order id','order','order number','orderid','mã đơn','ma don'],
  sku: ['sku','mã sku','ma sku','sku đơn','sku don'],
  materialCode: ['fabric code','material code','mã vải','ma vai','sku vải','sku vai','fabric sku'],
  materialName: ['fabric name','material name','tên vải','ten vai','loại vải','loai vai'],
  color: ['fabric color','material color','màu vải','mau vai','color','màu','mau'],
  unit: ['unit','đơn vị','don vi'],
  usagePerUnit: ['usage per unit','fabric per unit','material per unit','định mức','dinh muc','định mức/sp','dinh muc/sp','m/sp','m / sp'],
  totalRequired: ['fabric qty','fabric quantity','total fabric','required fabric','tổng vải cần','tong vai can','tổng vải','tong vai','số vải cần','so vai can','required qty'],
  note: ['note','notes','ghi chú','ghi chu']
};
function normalizeInventoryImportRows(text){
  const delim=detectDelimiter(text),matrix=parseDelimited(text,delim);
  if(matrix.length<2)throw badRequest('Không tìm thấy dữ liệu Kho vải. Hàng đầu tiên phải là header.','INVENTORY_IMPORT_EMPTY');
  const headers=matrix[0].map(v=>String(v||'').trim()),idx={};
  for(const [key,cands] of Object.entries(INVENTORY_IMPORT_HEADER_MAP))idx[key]=findMappedHeader(headers,cands);
  if(idx.orderId<0)throw badRequest('Thiếu cột Order ID.','INVENTORY_IMPORT_ORDER_HEADER');
  if(idx.sku<0)throw badRequest('Thiếu cột SKU. Mỗi dòng phải xác định đúng SKU của Order.','INVENTORY_IMPORT_SKU_HEADER');
  if(idx.materialCode<0)throw badRequest('Thiếu cột Mã vải / Fabric Code.','INVENTORY_IMPORT_MATERIAL_HEADER');
  if(idx.usagePerUnit<0&&idx.totalRequired<0)throw badRequest('Cần có cột Định mức/SP hoặc Tổng vải cần.','INVENTORY_IMPORT_USAGE_HEADER');
  const rows=[];
  for(let r=1;r<matrix.length;r++){
    const src=matrix[r],get=k=>idx[k]>=0?String(src[idx[k]]??'').trim():'';
    const orderId=get('orderId'),sku=get('sku'),materialCode=get('materialCode');
    if(!orderId&&!sku&&!materialCode)continue;
    rows.push({rowNumber:r+1,orderId,sku,materialCode,materialName:get('materialName'),color:get('color'),unit:get('unit')||'M',usagePerUnit:get('usagePerUnit'),totalRequired:get('totalRequired'),note:get('note')});
  }
  if(!rows.length)throw badRequest('Không có dòng dữ liệu Kho vải để import.','INVENTORY_IMPORT_EMPTY');
  return {headers,rows};
}
function inventoryImportPreviewRows(rows){
  return rows.map(raw=>{
    const out={...raw,status:'READY',errors:[],warnings:[]};
    try{
      const orderKey=normalizeOrderId(raw.orderId);const idErr=validateOrderId(orderKey);if(idErr)throw badRequest(idErr,'INVENTORY_IMPORT_ORDER_ID');
      const sku=cleanText(raw.sku||'',120,'SKU');if(!sku)throw badRequest('SKU không được trống.','INVENTORY_IMPORT_SKU');
      const materialCode=normalizeMaterialCode(raw.materialCode);const unit=cleanMaterialUnit(raw.unit||'M');
      const order=db.prepare('SELECT id,order_id,customer,current_stage,version FROM orders WHERE order_key=? AND deleted_at IS NULL').get(orderKey);if(!order)throw notFound(`Không tìm thấy Order ${raw.orderId}.`,'INVENTORY_IMPORT_ORDER_NOT_FOUND');
      const matchingItems=db.prepare('SELECT id,item_key,sku,product_name,qty,inventory_material_id,material_usage_per_unit,material_usage_milli FROM order_items WHERE order_db_id=? AND active=1 AND lower(sku)=lower(?)').all(order.id,sku);if(matchingItems.length>1)throw conflict('SKU trùng trong đơn; không thể chọn dòng an toàn.','INVENTORY_AMBIGUOUS_SKU');const item=matchingItems[0];if(!item)throw notFound(`Order #${order.order_id} không có SKU ${sku}.`,'INVENTORY_IMPORT_SKU_NOT_FOUND');
      let usage=null,total=null;
      if(String(raw.usagePerUnit||'').trim()!=='')usage=cleanInventoryNumber(raw.usagePerUnit,{allowZero:false,name:'Định mức/SP'});
      if(String(raw.totalRequired||'').trim()!=='')total=cleanInventoryNumber(raw.totalRequired,{allowZero:false,name:'Tổng vải cần'});
      if(usage===null&&total===null)throw badRequest('Dòng phải có Định mức/SP hoặc Tổng vải cần.','INVENTORY_IMPORT_USAGE_REQUIRED');
      if(usage===null){if(Number(item.qty)<=0)throw badRequest('Số lượng sản phẩm không hợp lệ để tính định mức.','INVENTORY_IMPORT_ITEM_QTY');const milli=toMilliUnits(total);if(milli%Number(item.qty)!==0)throw badRequest('Tổng vải không chia hết theo độ chính xác 0.001; hãy nhập định mức/SP rõ ràng.','INVENTORY_PRECISION_INVALID');usage=fromMilliUnits(milli/Number(item.qty));}
      const calculatedTotal=Math.round(Number(item.qty)*usage*1e6)/1e6;
      if(total!==null&&Math.abs(calculatedTotal-total)>0.001)out.warnings.push(`Tổng theo định mức là ${calculatedTotal} nhưng Sheet ghi ${total}. Hệ thống dùng Định mức/SP.`);
      const material=db.prepare('SELECT id,code,name,color,unit,active,current_stock currentStock FROM inventory_materials WHERE code=?').get(materialCode);
      const currentMaterial=item.inventory_material_id?getInventoryMaterial(item.inventory_material_id):null;
      const issued=currentMaterial?itemNetIssued(item.id,currentMaterial.id):0;
      if(issued>1e-9&&(!material||Number(material.id)!==Number(currentMaterial?.id)))throw conflict(`SKU ${sku} đã xuất ${issued} ${currentMaterial?.unit||''}; phải hoàn vải trước khi đổi mã vải.`,'INVENTORY_ALREADY_ISSUED');
      if(issued>calculatedTotal+1e-9)throw conflict(`SKU ${sku} đã xuất ${issued}, lớn hơn tổng định mức mới ${calculatedTotal}.`,'INVENTORY_USAGE_BELOW_ISSUED');
      if(['PCS','ROLL'].includes(material?.unit||unit)&&!Number.isInteger(usage))throw badRequest(`SKU ${sku} dùng vật tư dạng tấm/chiếc: định mức trên 1 sản phẩm phải là số nguyên.`, 'STOCK_USAGE_INTEGER');
      if(!material)out.warnings.push('Mã vải chưa có trong danh mục; có thể tự tạo khi Import.');
      else if(!material.active)throw conflict(`Mã vải ${materialCode} đang ngừng dùng.`,'MATERIAL_INACTIVE');
      if(raw.expectedVersion!==undefined&&Number(raw.expectedVersion)!==order.version)throw conflict('Đơn thay đổi sau xem trước; hãy kiểm tra lại.','INVENTORY_IMPORT_STALE');out.expectedVersion=order.version;out.orderId=order.order_id;out.orderDbId=order.id;out.customer=order.customer;out.itemId=item.id;out.productName=item.product_name;out.itemQty=Number(item.qty);out.sku=item.sku;out.materialCode=materialCode;out.materialName=cleanText(raw.materialName||material?.name||materialCode,160,'Tên vải');out.color=cleanText(raw.color||material?.color||'',120,'Màu');out.unit=material?.unit||unit;out.usagePerUnit=usage;out.totalRequired=calculatedTotal;out.materialId=material?.id||null;out.materialExists=!!material;out.currentMaterialCode=currentMaterial?.code||'';out.currentUsagePerUnit=fromMilliUnits(Number(item.material_usage_milli??toMilliUnits(item.material_usage_per_unit||0)));out.issuedQty=issued;
    }catch(e){out.status='ERROR';out.errors.push(e.message||'Dòng không hợp lệ.');}
    return out;
  });
}
function commitInventoryImport(rows,user,{createMissingMaterials=true}={}){
  if(!Array.isArray(rows)||!rows.length)throw badRequest('Không có dữ liệu Kho vải để import.','INVENTORY_IMPORT_EMPTY');
  if(rows.length>2000)throw badRequest('Mỗi lần chỉ import tối đa 2000 dòng.','INVENTORY_IMPORT_TOO_LARGE');
  return withTransaction(()=>{
    const preview=inventoryImportPreviewRows(rows);const bad=preview.filter(r=>r.status==='ERROR');if(bad.length)throw badRequest(`Có ${bad.length} dòng lỗi. Hãy sửa dữ liệu trước khi import.`,'INVENTORY_IMPORT_INVALID',{rows:bad.slice(0,50)});
    const seen=new Map();let updated=0,createdMaterials=0;const touchedOrders=new Map();
    for(const row of preview){
      const key=`${row.orderDbId}|${String(row.sku).toLowerCase()}`;const sig=`${row.materialCode}|${row.usagePerUnit}`;
      if(seen.has(key)){if(seen.get(key)!==sig)throw conflict(`Trùng Order + SKU nhưng cấu hình vải khác nhau: #${row.orderId} / ${row.sku}.`,'INVENTORY_IMPORT_DUPLICATE_SKU');continue;}seen.set(key,sig);
      const currentId=db.prepare('SELECT id FROM inventory_materials WHERE code=?').get(row.materialCode)?.id;let material=currentId?getInventoryMaterial(currentId):null;
      if(!material){
        if(!createMissingMaterials)throw conflict(`Mã vải ${row.materialCode} chưa tồn tại.`,'MATERIAL_NOT_FOUND');
        const categoryId=stockCategoryId();const t=nowIso();const rr=db.prepare('INSERT INTO inventory_materials(code,name,color,unit,current_stock,min_stock,current_stock_milli,min_stock_milli,active,notes,created_at,updated_at,category_id) VALUES(?,?,?,?,0,0,0,0,1,?,?,?,?)').run(row.materialCode,row.materialName||row.materialCode,row.color||'',row.unit||'M',`Tạo tự động từ Sheet · ${row.note||''}`.slice(0,1000),t,t,categoryId);material=getInventoryMaterial(rr.lastInsertRowid);createdMaterials++;
      }
      const item=db.prepare('SELECT * FROM order_items WHERE id=? AND order_db_id=? AND active=1').get(row.itemId,row.orderDbId);if(!item)throw conflict(`SKU ${row.sku} vừa thay đổi. Hãy Preview lại.`,'INVENTORY_IMPORT_STALE');
      const oldMid=Number(item.inventory_material_id||0),issued=oldMid?itemNetIssued(item.id,oldMid):0;
      if(issued>1e-9&&oldMid!==Number(material.id))throw conflict(`SKU ${row.sku} đã xuất vải; không thể đổi mã vải.`,'INVENTORY_ALREADY_ISSUED');
      if(issued>Number(row.totalRequired)+1e-9)throw conflict(`SKU ${row.sku} đã xuất nhiều hơn định mức Sheet.`,'INVENTORY_USAGE_BELOW_ISSUED');
      db.prepare('UPDATE order_items SET inventory_material_id=?,material_usage_per_unit=?,material_usage_milli=? WHERE id=?').run(material.id,row.usagePerUnit,toMilliUnits(row.usagePerUnit),item.id);updated++;touchedOrders.set(row.orderDbId,row.orderId);
    }
    for(const [orderDbId,orderId] of touchedOrders){db.prepare('UPDATE orders SET version=version+1,updated_at=? WHERE id=?').run(nowIso(),orderDbId);audit({orderId,orderDbId,action:'INVENTORY_SHEET_MAPPING_IMPORTED',user,detail:`Import cấu hình vải theo SKU từ Sheets`});}
    audit({action:'INVENTORY_IMPORT_BATCH',user,detail:`${updated} SKU · ${touchedOrders.size} Order · tạo ${createdMaterials} mã vải`});
    return {updated,orders:touchedOrders.size,createdMaterials};
  });
}


// STOCK SKU V15.1: A SKU denotes one fixed-size item. Business date != immutable audit timestamp.
function stockDate(raw){
  const value=String(raw||'').trim();
  if(!value)return vietnamToday();
  const vi=value.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$/);
  const iso=vi?`${vi[3]}-${vi[2].padStart(2,'0')}-${vi[1].padStart(2,'0')}`:value;
  if(!/^\d{4}-\d{2}-\d{2}$/.test(iso)||Number.isNaN(Date.parse(iso))||new Date(`${iso}T00:00:00Z`).toISOString().slice(0,10)!==iso)throw badRequest('Ngày nhập/xuất phải là ngày hợp lệ (YYYY-MM-DD hoặc DD/MM/YYYY).','STOCK_DATE_INVALID');
  return iso;
}
function stockDimensions(body,old={}){
  const width=body.widthCm===undefined?Number(old.widthCm||0):Number(body.widthCm||0),length=body.lengthCm===undefined?Number(old.lengthCm||0):Number(body.lengthCm||0);
  if(!Number.isFinite(width)||!Number.isFinite(length)||width<0||length<0||width>100000||length>100000||(!!width!==!!length))throw badRequest('Kích thước phải gồm chiều rộng và dài hợp lệ (cm), hoặc để trống cả hai.','STOCK_SIZE_INVALID');
  const sizeLabel=body.sizeLabel===undefined?(old.sizeLabel||''):cleanText(body.sizeLabel||'',100,'Kích thước khác');
  return {widthCm:Math.round(width*100)/100,lengthCm:Math.round(length*100)/100,sizeLabel};
}
function stockCategoryId(id){
  const n=Number(id||db.prepare("SELECT id FROM inventory_categories WHERE name='Vải'").get()?.id);
  const cat=Number.isSafeInteger(n)&&n>0?db.prepare('SELECT id,name FROM inventory_categories WHERE id=?').get(n):null;
  if(!cat)throw badRequest('Nhóm sản phẩm không tồn tại; hãy tạo nhóm trước.','STOCK_CATEGORY_INVALID');
  return cat.id;
}
function requireWholePieces(m,qty){
  if(['PCS','ROLL'].includes(m.unit)&&!Number.isInteger(qty))throw badRequest(`SKU ${m.code}: đơn vị ${m.unit} phải nhập số lượng nguyên.`, 'STOCK_PIECES_INTEGER');
}
function requireWholeMinStock(unit,minStock){
  if(['PCS','ROLL'].includes(unit)&&!Number.isInteger(minStock))
    throw badRequest(`Tồn tối thiểu theo ${unit} phải là số nguyên.`, 'STOCK_MIN_INTEGER');
}
function stockImportPreview(text){
  if(String(text).length>1500000)throw badRequest('Bảng nhập kho vượt dung lượng cho phép.','STOCK_IMPORT_TOO_LARGE');
  const matrix=parseDelimited(text,detectDelimiter(text));
  if(matrix.length<2)throw badRequest('Dán ít nhất hàng tiêu đề và một dòng dữ liệu.','STOCK_IMPORT_EMPTY');
  const headers=matrix[0].map(x=>String(x||'').trim());
  const fieldMap={
    code:['Mã vải','Mã sản phẩm','Mã SKU','SKU','Code','Material Code'],
    qty:['Số lượng nhập','Số lượng','SL nhập','SL','Quantity','Qty'],
    name:['Tên vải','Tên sản phẩm','Tên','Name','Product'],
    category:['Nhóm sản phẩm','Loại sản phẩm','Nhóm','Category'],
    color:['Màu','Color'],unit:['Đơn vị','Unit'],
    widthCm:['Rộng (cm)','Chiều rộng (cm)','Rộng','Width cm','Width'],
    lengthCm:['Dài (cm)','Chiều dài (cm)','Dài','Length cm','Length'],
    sizeLabel:['Kích thước','Size','Size label'],
    date:['Ngày nhập','Ngày','Date','Received date'],
    lot:['Mã lô','Lot','Lot code'],supplier:['Nhà cung cấp','Supplier'],note:['Ghi chú','Note']
  };
  const col=Object.fromEntries(Object.entries(fieldMap).map(([k,c])=>[k,findMappedHeader(headers,c)]));
  if(col.code<0||col.qty<0)throw badRequest('Sheet cần có cột Mã vải/SKU và Số lượng nhập.','STOCK_IMPORT_HEADERS');
  if(matrix.length>2001)throw badRequest('Tối đa 2000 dòng mỗi lần.','STOCK_IMPORT_TOO_MANY');
  const rows=matrix.slice(1).map((row,i)=>{
    const get=k=>col[k]<0?'':String(row[col[k]]??'').trim();
    const out={rowNumber:i+2,code:get('code'),qty:get('qty'),name:get('name'),category:get('category'),color:get('color'),unit:get('unit'),widthCm:get('widthCm'),lengthCm:get('lengthCm'),sizeLabel:get('sizeLabel'),date:get('date'),lot:get('lot'),supplier:get('supplier'),note:get('note'),status:'READY',errors:[]};
    try{
      out.code=normalizeMaterialCode(out.code);
      out.qty=cleanInventoryNumber(out.qty,{allowZero:false,name:'Số lượng nhập'});
      out.date=stockDate(out.date);
      const material=db.prepare('SELECT * FROM inventory_materials WHERE code=?').get(out.code);
      const catName=out.category||(material?db.prepare('SELECT name FROM inventory_categories WHERE id=?').get(material.category_id)?.name:'Vải')||'Vải';
      const category=db.prepare('SELECT id,name FROM inventory_categories WHERE name=? COLLATE NOCASE').get(catName);
      if(!category)throw badRequest(`Nhóm "${catName}" chưa có. Tạo nhóm trước khi import.`,'STOCK_CATEGORY_INVALID');
      out.categoryId=category.id;out.category=category.name;
      if(material){
        if(!material.active)throw badRequest(`SKU ${out.code} đã ngừng dùng.`,'STOCK_INACTIVE');
        const existing=stockDimensions({},{widthCm:material.width_cm,lengthCm:material.length_cm,sizeLabel:material.size_label});
        if(out.widthCm!==''||out.lengthCm!==''){
          const incoming=stockDimensions({widthCm:out.widthCm,lengthCm:out.lengthCm});
          if(incoming.widthCm!==existing.widthCm||incoming.lengthCm!==existing.lengthCm)throw badRequest(`Kích thước SKU ${out.code} khác danh mục; phải tạo SKU khác.`,'STOCK_FIXED_SIZE_CONFLICT');
        }
        if(out.unit&&cleanMaterialUnit(out.unit)!==material.unit)throw badRequest(`Đơn vị SKU ${out.code} đã cố định là ${material.unit}.`,'STOCK_UNIT_CONFLICT');
        if(out.category&&Number(material.category_id)!==category.id)throw badRequest(`SKU ${out.code} thuộc nhóm khác (${material.category_id}).`,'STOCK_CATEGORY_CONFLICT');
        out.unit=material.unit;out.existing=true;out.name=material.name;out.color=material.color||'';
        out.widthCm=existing.widthCm;out.lengthCm=existing.lengthCm;out.sizeLabel=existing.sizeLabel;
      }else{
        if(!out.name)throw badRequest(`SKU mới ${out.code} cần cột Tên sản phẩm.`,'STOCK_NAME_REQUIRED');
        out.name=cleanText(out.name,160,'Tên sản phẩm');out.color=cleanText(out.color,120,'Màu');
        out.unit=cleanMaterialUnit(out.unit||'PCS');
        const dims=stockDimensions(out);Object.assign(out,dims);out.existing=false;
        if((out.widthCm||out.lengthCm)&&out.unit!=='PCS')throw badRequest('Hàng kích thước cố định phải dùng đơn vị PCS (tấm/chiếc).','STOCK_SIZE_UNIT');
      }
      requireWholePieces({code:out.code,unit:out.unit},out.qty);
      out.lot=cleanText(out.lot,120,'Mã lô');out.supplier=cleanText(out.supplier,160,'Nhà cung cấp');out.note=cleanText(out.note,1000,'Ghi chú');
    }catch(e){out.status='ERROR';out.errors.push(e.message||'Dòng lỗi');}
    return out;
  });
  if(!rows.length)throw badRequest('Không có dòng dữ liệu.','STOCK_IMPORT_EMPTY');
  const definitions=new Map();
  for(const r of rows.filter(r=>r.status==='READY'&&!r.existing)){
    const sig=JSON.stringify([r.name,r.categoryId,r.unit,r.widthCm,r.lengthCm,r.sizeLabel,r.color]);
    if(definitions.has(r.code)&&definitions.get(r.code)!==sig){r.status='ERROR';r.errors.push('Cùng SKU mới nhưng thông số khác nhau trong Sheet.');}
    definitions.set(r.code,sig);
  }
  return {headers,rows,summary:{total:rows.length,ready:rows.filter(r=>r.status==='READY').length,errors:rows.filter(r=>r.status==='ERROR').length,newSkus:new Set(rows.filter(r=>r.status==='READY'&&!r.existing).map(r=>r.code)).size,totalQty:roundDisplay(rows.filter(r=>r.status==='READY').reduce((a,r)=>a+r.qty,0))}};
}
// Deduplicate the *business receipt*, not only its raw CSV/TSV formatting.
// Keep the legacy raw hash lookup for receipts already recorded by V15.1.
function stockReceiptFingerprint(rows){
  const normalized=rows.map(r=>[r.code,r.qty,r.name,r.categoryId,r.color,r.unit,r.widthCm,r.lengthCm,r.sizeLabel,r.date,r.lot,r.supplier,r.note]);
  normalized.sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
  return sha256(JSON.stringify(normalized));
}
function stockImportCommit(text,user,expectedReceiptHash=''){
  const parsed=stockImportPreview(text);
  if(parsed.summary.errors)throw badRequest(`Có ${parsed.summary.errors} dòng lỗi. Vui lòng xem trước và sửa.`, 'STOCK_IMPORT_INVALID',{rows:parsed.rows.filter(r=>r.status==='ERROR').slice(0,30)});
  const legacyHash=sha256(String(text).trim().replace(/\r\n/g,'\n'));
  const hash=stockReceiptFingerprint(parsed.rows);
  if(expectedReceiptHash&&hash!==expectedReceiptHash)throw conflict('Dữ liệu nhập kho đã thay đổi sau Preview (hoặc đã sang ngày mới). Hãy kiểm tra lại.','STOCK_SHEET_CHANGED');
  return withTransaction(()=>{
    const previous=db.prepare('SELECT id FROM stock_import_receipts WHERE source_hash=? OR source_hash=? LIMIT 1').get(hash,legacyHash);
    if(previous)throw conflict(`Sheet này đã nhập ở lần #${previous.id}; không thể cộng kho trùng.`, 'STOCK_IMPORT_DUPLICATE');
    const created=new Set();
    for(const row of parsed.rows){
      let m=db.prepare('SELECT id FROM inventory_materials WHERE code=?').get(row.code);
      if(!m){const t=nowIso();const r=db.prepare(`INSERT INTO inventory_materials(code,name,color,unit,current_stock,min_stock,current_stock_milli,min_stock_milli,active,notes,created_at,updated_at,category_id,width_cm,length_cm,size_label) VALUES(?,?,?,?,0,0,0,0,1,'',?,?,?,?,?,?)`).run(row.code,row.name,row.color,row.unit,t,t,row.categoryId,row.widthCm||null,row.lengthCm||null,row.sizeLabel||'');m={id:Number(r.lastInsertRowid)};created.add(row.code);}
      const fresh=getInventoryMaterial(m.id),beforeMilli=Number(fresh.currentStockMilli),add=toMilliUnits(row.qty),afterMilli=beforeMilli+add;
      writeMaterialStock(m.id,afterMilli);
      inventoryMovement({materialId:m.id,type:'RECEIVE',quantity:row.qty,before:fromMilliUnits(beforeMilli),after:fromMilliUnits(afterMilli),effectiveDate:row.date,reference:row.lot||'SHEETS',note:[row.supplier&&`NCC: ${row.supplier}`,row.note,'Nhập tồn kho từ Sheets'].filter(Boolean).join(' · '),user});
    }
    const receipt=db.prepare('INSERT INTO stock_import_receipts(source_hash,row_count,created_count,received_quantity,created_by,created_at) VALUES(?,?,?,?,?,?)').run(hash,parsed.rows.length,created.size,parsed.summary.totalQty,user.id,nowIso());
    audit({action:'STOCK_SHEETS_RECEIVED',user,detail:`Batch #${receipt.lastInsertRowid} · ${parsed.rows.length} dòng · tạo ${created.size} SKU`});
    return {receiptId:Number(receipt.lastInsertRowid),count:parsed.rows.length,created:created.size,totalQty:parsed.summary.totalQty};
  });
}

function findProductionWorkerId(workerText){
  const w=String(workerText||'').trim();
  if(!w)return null;
  const n=w.toLowerCase();
  const u=db.prepare(`SELECT id FROM users WHERE active=1 AND deleted_at IS NULL AND role='PRODUCTION' AND (lower(username)=? OR lower(full_name)=?) LIMIT 1`).get(n,n);
  return u?u.id:null;
}
function validateImportedRow(row){
  if(!row||typeof row!=='object'||Array.isArray(row))throw badRequest('Dòng nhập không hợp lệ.');
  const id=normalizeOrderId(row.orderId),idErr=validateOrderId(id);if(idErr)throw badRequest(idErr,'IMPORT_ORDER_ID');
  cleanText(row.customer||'',200,'Khách hàng'); if(row.needBy)strictNeedBy(row.needBy); cleanText(row.trackingNumber||'',200,'Tracking'); cleanText(row.personalization||'',5000,'Ghi chú');
  const priority=String(row.priority||'NORMAL').toUpperCase();if(!['NORMAL','HIGH','EXPRESS'].includes(priority))throw badRequest('Priority không hợp lệ.','IMPORT_PRIORITY');
  const stage=String(row.currentStage||'NEW').toUpperCase();if(!STAGE_ORDER.includes(stage))throw badRequest('Stage không hợp lệ.','IMPORT_STAGE');
  cleanItem({sku:row.sku,productName:row.productName||'Sản phẩm',qty:row.qty===undefined?1:row.qty,material:row.material,personalization:row.personalization});return true;
}
function upsertImportedOrder(row, mode, user, context={}){
  const orderId=normalizeOrderId(row.orderId); const idErr=validateOrderId(orderId); if(idErr)throw new Error(idErr);
  const orderKey=normalizeOrderId(orderId); const existing=db.prepare('SELECT * FROM orders WHERE order_key=?').get(orderKey);
  if(!existing&&row.currentStage&&row.currentStage!=='NEW'&&!hasPermission(user,'orders.move_stage'))throw forbidden('Không có quyền đặt công đoạn khi nhập đơn.');
  if(existing&&row.currentStage&&row.currentStage!==existing.current_stage&&!hasPermission(user,'orders.move_stage'))throw forbidden('Không có quyền đổi công đoạn qua import.');
  const t=nowIso(); const assignedWorkerId=findProductionWorkerId(row.worker);
  let needBy=''; if(row.needBy)needBy=strictNeedBy(row.needBy);
  const priority=String(row.priority||existing?.priority||'NORMAL').toUpperCase(); if(!['NORMAL','HIGH','EXPRESS'].includes(priority))throw new Error('Priority không hợp lệ.');
  const stage=String(row.currentStage||existing?.current_stage||'NEW').toUpperCase(); if(!STAGE_ORDER.includes(stage))throw new Error('Stage không hợp lệ.');
  const item=cleanItem({sku:row.sku,productName:row.productName||'Sản phẩm',qty:row.qty??1,material:row.material,personalization:row.personalization});
  if(existing?.deleted_at)throw conflict('Đơn ở thùng rác; khôi phục trước khi nhập.');
  if(existing && mode!=='UPDATE' && context.preexistingKeys?.has(orderKey)) return {status:'SKIPPED',orderId,message:'Order đã tồn tại trước batch'};
  if(!existing){
    const r=db.prepare(`INSERT INTO orders(order_id,order_key,customer,need_by,need_by_iso,priority,current_stage,stage_updated_at,tracking_number,assigned_worker_id,notes,version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,1,?,?)`).run(
      orderId,orderKey,cleanText(row.customer||'',200,'Khách hàng'),needBy,needByIso(needBy),priority,stage,t,cleanText(row.trackingNumber||'',200,'Tracking')||null,assignedWorkerId,cleanText(row.personalization||'',5000,'Ghi chú'),t,t
    );
    db.prepare(`INSERT INTO order_items(order_db_id,sku,product_name,qty,material,personalization,item_status,current_stage) VALUES(?,?,?,?,?,?,?,?)`).run(r.lastInsertRowid,item.sku,item.productName,item.qty,item.material,item.personalization,'PENDING','PENDING');
    syncItemWorkflowStage(r.lastInsertRowid,stage);
    audit({orderId,orderDbId:r.lastInsertRowid,action:'ORDER_IMPORTED',user,detail:'Imported from Sheets / pasted data'});
    context.createdKeys?.add(orderKey); return {status:'CREATED',orderId};
  }
  const customer=row.customer?cleanText(row.customer,200,'Khách hàng'):existing.customer||'';
  const tracking=row.trackingNumber?cleanText(row.trackingNumber,200,'Tracking'):existing.tracking_number||null;
  const workerId=assignedWorkerId||existing.assigned_worker_id||null;
  const notes=row.personalization?cleanText(row.personalization,5000,'Ghi chú'):existing.notes||'';
  db.prepare(`UPDATE orders SET customer=?,need_by=?,need_by_iso=?,priority=?,current_stage=?,stage_updated_at=CASE WHEN current_stage<>? THEN ? ELSE stage_updated_at END,tracking_number=?,assigned_worker_id=?,notes=?,version=version+1,updated_at=? WHERE id=?`).run(customer,needBy||existing.need_by||'',needByIso(needBy||existing.need_by||''),priority,stage,stage,t,tracking,workerId,notes,t,existing.id);
  let currentItem=null;
  if(item.sku){const matches=db.prepare('SELECT * FROM order_items WHERE order_db_id=? AND active=1 AND lower(sku)=lower(?)').all(existing.id,item.sku);if(matches.length>1)throw conflict('SKU trùng; cần sửa từng dòng theo itemKey.','IMPORT_AMBIGUOUS_ITEM');currentItem=matches[0]||null;}
  if(!item.sku){const found=db.prepare("SELECT * FROM order_items WHERE order_db_id=? AND active=1 AND COALESCE(sku,'')='' AND lower(product_name)=lower(?)").all(existing.id,item.productName);if(found.length>1)throw conflict('Tên sản phẩm trùng; cần SKU hoặc itemKey.','IMPORT_AMBIGUOUS_ITEM');currentItem=found[0]||null;}
  if(currentItem){
    const issued=currentItem.inventory_material_id?itemNetIssued(currentItem.id,currentItem.inventory_material_id):0;
    if(issued>item.qty*Number(currentItem.material_usage_per_unit||0)+1e-9)throw conflict('Phải hoàn vải trước khi giảm số lượng đã xuất.','INVENTORY_ALREADY_ISSUED');
    if(issued>0&&item.sku!==currentItem.sku)throw conflict('Phải hoàn vải trước khi đổi SKU đã xuất.','INVENTORY_ALREADY_ISSUED');
    db.prepare(`UPDATE order_items SET sku=?,product_name=?,qty=?,material=?,personalization=? WHERE id=?`).run(item.sku,item.productName,item.qty,item.material,item.personalization,currentItem.id);
    if(String(currentItem.sku||'').trim().toUpperCase()!==String(item.sku||'').trim().toUpperCase())db.prepare('UPDATE order_item_prices SET unit_price_cents=NULL WHERE order_item_id=?').run(currentItem.id);
  }else{
    if(db.prepare('SELECT COUNT(*) n FROM order_items WHERE order_db_id=? AND active=1').get(existing.id).n>=50)throw badRequest('Đơn vượt 50 dòng sản phẩm.','ITEM_LIMIT');
    db.prepare(`INSERT INTO order_items(order_db_id,sku,product_name,qty,material,personalization,item_status,current_stage) VALUES(?,?,?,?,?,?,?,?)`).run(existing.id,item.sku,item.productName,item.qty,item.material,item.personalization,'PENDING','PENDING');
  }
  syncItemWorkflowStage(existing.id,stage);
  audit({orderId:existing.order_id,orderDbId:existing.id,action:'ORDER_IMPORT_UPDATED',user,detail:`Updated/imported SKU ${item.sku||item.productName}`});
  return {status:'UPDATED',orderId:existing.order_id};
}


const requestBuckets=new Map();
function requestAllowed(ip){
  const now=Date.now(), minute=Math.floor(now/60_000);
  const key=`${ip}|${minute}`;
  const count=(requestBuckets.get(key)||0)+1; requestBuckets.set(key,count);
  if(requestBuckets.size>5000){ for(const k of requestBuckets.keys()){ const m=Number(k.slice(k.lastIndexOf('|')+1)); if(m<minute-2) requestBuckets.delete(k); } }
  return count<=GLOBAL_RATE_LIMIT_PER_MIN;
}

function loginAllowed(raw){return loginGuard.state(raw).allowed;}
function loginFail(raw){return loginGuard.fail(raw);}
function loginSuccess(raw){return loginGuard.success(raw);}

function evidenceUsage(orderDbId){return db.prepare('SELECT COUNT(*) count,COALESCE(SUM(size_bytes),0) bytes FROM evidence_files WHERE order_db_id=?').get(orderDbId);}
function prepareEvidence(payload,o,user,category){
  if(!payload||!payload.dataUrl)return null;
  if(!userCanViewOrder(user,o.order_id)&&!hasPermission(user,'orders.view_all'))throw forbidden('Bạn không có quyền thêm bằng chứng cho Order này.','EVIDENCE_SCOPE');
  const itemId=Number(payload.itemId)||null;if(itemId&&!db.prepare('SELECT id FROM order_items WHERE id=? AND order_db_id=? AND active=1').get(itemId,o.id))throw badRequest('Item không thuộc Order hoặc đã được lưu lịch sử.','EVIDENCE_ITEM');
  const m=String(payload.dataUrl||'').match(/^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=]+)$/);if(!m)throw badRequest('Chỉ nhận ảnh PNG/JPEG/WEBP.','EVIDENCE_FORMAT');
  let buf;try{buf=Buffer.from(m[2],'base64')}catch{throw badRequest('Ảnh base64 không hợp lệ.','EVIDENCE_BASE64')}
  if(!buf.length||buf.length>EVIDENCE_MAX_FILE_BYTES)throw badRequest(`Ảnh phải nhỏ hơn ${Math.round(EVIDENCE_MAX_FILE_BYTES/1_000_000)} MB.`,'EVIDENCE_SIZE');
  const actual=imageMimeFromMagic(buf);if(!actual||actual!==m[1])throw badRequest('Nội dung file không khớp định dạng ảnh khai báo.','EVIDENCE_MAGIC');
  if(category==='PRODUCT'){
    if(buf.length>MAX_PRODUCT_IMAGE_BYTES)throw badRequest('Mỗi ảnh tối đa 3 MB.','EVIDENCE_SIZE');
    const count=db.prepare("SELECT COUNT(*) n FROM evidence_files WHERE order_db_id=? AND category='PRODUCT'").get(o.id).n;
    if(count>=MAX_PRODUCT_IMAGES)throw new AppError(409,'PRODUCT_IMAGE_QUOTA','Mỗi đơn hàng tối đa 6 ảnh sản phẩm.');
  }
  const usage=evidenceUsage(o.id);if(Number(usage.count)>=EVIDENCE_MAX_FILES_PER_ORDER)throw new AppError(409,'EVIDENCE_QUOTA',`Order đã đạt giới hạn ${EVIDENCE_MAX_FILES_PER_ORDER} ảnh.`);if(Number(usage.bytes)+buf.length>EVIDENCE_MAX_BYTES_PER_ORDER)throw new AppError(409,'EVIDENCE_QUOTA','Order đã đạt giới hạn dung lượng ảnh bằng chứng.');
  const ext=actual==='image/png'?'.png':actual==='image/webp'?'.webp':'.jpg',fileName=cleanText(payload.fileName||`evidence${ext}`,180,'Tên file'),dir=String(o.id),rel=path.join(dir,`${Date.now()}-${crypto.randomUUID()}${ext}`),fullDir=path.join(EVIDENCE_DIR,dir),finalFull=path.join(EVIDENCE_DIR,rel),tmpFull=finalFull+'.tmp';fs.mkdirSync(fullDir,{recursive:true});fs.writeFileSync(tmpFull,buf);
  return {tmpFull,finalFull,rel,itemId,category,fileName,mime:actual,sizeBytes:buf.length,sha256:crypto.createHash('sha256').update(buf).digest('hex')};
}
function commitPreparedEvidence(prep,o,user,{qcCheckId=null,packingCheckId=null}={}){
  if(!prep)return null;fs.renameSync(prep.tmpFull,prep.finalFull);
  try{const r=db.prepare('INSERT INTO evidence_files(order_db_id,order_item_id,category,file_name,mime,storage_path,size_bytes,content_sha256,qc_check_id,packing_check_id,uploaded_by,uploaded_by_name,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run(o.id,prep.itemId,prep.category,prep.fileName,prep.mime,prep.rel,prep.sizeBytes,prep.sha256,qcCheckId,packingCheckId,user.id,user.full_name,nowIso());return r.lastInsertRowid;}catch(e){try{fs.rmSync(prep.finalFull,{force:true})}catch{};throw e}
}
function cleanupPreparedEvidence(prep){if(!prep)return;try{fs.rmSync(prep.tmpFull,{force:true})}catch{};try{if(fs.existsSync(prep.finalFull))fs.rmSync(prep.finalFull,{force:true})}catch{}}

function validateItemsLength(items){if(items.length>50)throw badRequest('Mỗi đơn tối đa 50 dòng sản phẩm.','ITEM_LIMIT');return items;}
function syncOrderItems(orderDbId,items){
  const existing=db.prepare('SELECT * FROM order_items WHERE order_db_id=? AND active=1 ORDER BY id').all(orderDbId),byId=new Map(existing.map(x=>[Number(x.id),x])),seen=new Set();
  const upd=db.prepare(`UPDATE order_items SET sku=?,product_name=?,qty=?,material=?,personalization=?,inventory_material_id=?,material_usage_per_unit=?,material_usage_milli=? WHERE id=? AND order_db_id=? AND active=1`);
  const ins=db.prepare(`INSERT INTO order_items(order_db_id,item_key,sku,product_name,qty,material,personalization,item_status,current_stage,inventory_material_id,material_usage_per_unit,material_usage_milli,active) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1)`);
  for(const it of items){
    if(it.inventoryMaterialId&&!getInventoryMaterial(it.inventoryMaterialId))throw badRequest('Vải kho được chọn không tồn tại.','MATERIAL_NOT_FOUND');
    if(it.id){
      const old=byId.get(Number(it.id));if(!old)throw badRequest(`Sản phẩm ID ${it.id} không thuộc Order hoặc đã được lưu lịch sử.`,'ITEM_ID_INVALID');if(seen.has(Number(it.id)))throw badRequest('Trùng ID sản phẩm trong form.','ITEM_DUPLICATE');
      const oldMaterialId=Number(old.inventory_material_id||0),newMaterialId=Number(it.inventoryMaterialId||0);const issued=oldMaterialId?itemNetIssued(old.id,oldMaterialId):0;
      if(issued>1e-9&&oldMaterialId!==newMaterialId)throw conflict('Dòng sản phẩm đã xuất vải. Hãy hoàn vải trước khi đổi loại vải kho.','INVENTORY_ALREADY_ISSUED');
      const newRequired=Number(it.qty||0)*Number(it.materialUsagePerUnit||0);if(issued>newRequired+1e-9)throw conflict(`Dòng sản phẩm đã xuất ${issued}. Không thể giảm định mức/số lượng xuống dưới lượng đã xuất. Hãy hoàn vải trước.`,'INVENTORY_ALREADY_ISSUED');
      seen.add(Number(it.id));upd.run(it.sku,it.productName,it.qty,it.material,it.personalization,it.inventoryMaterialId,it.materialUsagePerUnit,toMilliUnits(it.materialUsagePerUnit||0),it.id,orderDbId);
      // SKU has changed: its previous unit price must not silently become the new SKU's sale price.
      if(String(old.sku||'').trim().toUpperCase()!==String(it.sku||'').trim().toUpperCase())db.prepare('UPDATE order_item_prices SET unit_price_cents=NULL WHERE order_item_id=?').run(it.id);
    }else{
      const key=it.itemKey||crypto.randomUUID();const r=ins.run(orderDbId,key,it.sku,it.productName,it.qty,it.material,it.personalization,'PENDING','PENDING',it.inventoryMaterialId,it.materialUsagePerUnit,toMilliUnits(it.materialUsagePerUnit||0));seen.add(Number(r.lastInsertRowid));
    }
  }
  const t=nowIso();for(const old of existing){if(!seen.has(Number(old.id))){const mid=Number(old.inventory_material_id||0),issued=mid?itemNetIssued(old.id,mid):0;if(issued>1e-9)throw conflict(`Không thể xóa dòng sản phẩm ${old.product_name} vì đã xuất ${issued} vải. Hãy hoàn vải trước.`,'INVENTORY_ALREADY_ISSUED');db.prepare('UPDATE order_items SET active=0,deleted_at=? WHERE id=?').run(t,old.id);}}
}

function orderObject(orderId, includeDeleted=false){
  const key=normalizeOrderId(orderId); const o=db.prepare(`SELECT o.*,u.full_name assigned_worker FROM orders o LEFT JOIN users u ON u.id=o.assigned_worker_id WHERE (o.order_id=? OR o.order_key=?) ${includeDeleted?'':'AND o.deleted_at IS NULL'} ORDER BY CASE WHEN o.order_id=? THEN 0 ELSE 1 END LIMIT 1`).get(orderId,key,orderId);
  if(!o)return null;
  const items=db.prepare(`SELECT oi.id,oi.item_key itemKey,oi.sku,oi.product_name productName,oi.qty,oi.material,oi.personalization,oi.item_status itemStatus,oi.current_stage currentStage,oi.inventory_material_id inventoryMaterialId,COALESCE(oi.material_usage_milli,ROUND(COALESCE(oi.material_usage_per_unit,0)*1000)) materialUsageMilli,COALESCE(oi.material_usage_milli,ROUND(COALESCE(oi.material_usage_per_unit,0)*1000))/1000.0 materialUsagePerUnit,im.code inventoryMaterialCode,im.name inventoryMaterialName,im.color inventoryMaterialColor,im.unit inventoryMaterialUnit,COALESCE(im.current_stock_milli,ROUND(COALESCE(im.current_stock,0)*1000))/1000.0 inventoryMaterialStock FROM order_items oi LEFT JOIN inventory_materials im ON im.id=oi.inventory_material_id WHERE oi.order_db_id=? AND oi.active=1 ORDER BY oi.id`).all(o.id).map(it=>{const requiredMilli=Number(it.qty||0)*Number(it.materialUsageMilli||0),issuedMilli=it.inventoryMaterialId?itemNetIssuedMilli(it.id,it.inventoryMaterialId):0;return {...it,materialRequiredQty:fromMilliUnits(requiredMilli),materialIssuedQty:fromMilliUnits(issuedMilli),materialPendingQty:fromMilliUnits(Math.max(0,requiredMilli-issuedMilli))};});
  const timeline=db.prepare(`SELECT action,from_stage fromStage,to_stage toStage,full_name fullName,username,station_code stationCode,detail,request_id requestId,entity_type entityType,entity_id entityId,created_at createdAt FROM audit_events WHERE order_db_id=? OR (order_db_id IS NULL AND order_id=?) ORDER BY id DESC LIMIT 500`).all(o.id,o.order_id);
  const qcChecks=db.prepare(`SELECT id,result,checklist_json checklistJson,notes,checked_by_name checkedByName,defect_category defectCategory,created_at createdAt FROM qc_checks WHERE order_db_id=? ORDER BY id DESC LIMIT 100`).all(o.id).map(x=>{const itemResultDetails=db.prepare('SELECT order_item_id itemId,result,sku_snapshot sku,product_name_snapshot productName,qty_snapshot qty,material_snapshot material FROM qc_item_results WHERE qc_check_id=? ORDER BY id').all(x.id);return {...x,checklist:(()=>{try{return JSON.parse(x.checklistJson||'{}')}catch{return {}}})(),itemResults:Object.fromEntries(itemResultDetails.map(r=>[String(r.itemId),r.result])),itemResultDetails};});
  const packingChecks=db.prepare(`SELECT id,checklist_json checklistJson,notes,checked_by_name checkedByName,created_at createdAt FROM packing_checks WHERE order_db_id=? ORDER BY id DESC LIMIT 100`).all(o.id).map(x=>({...x,checklist:(()=>{try{return JSON.parse(x.checklistJson||'{}')}catch{return {}}})()}));
  const packingMetrics=db.prepare('SELECT actual_grams actualGrams,volume_cm3 volumeCm3,volumetric_grams volumetricGrams,billable_grams billableGrams,length_mm lengthMm,width_mm widthMm,height_mm heightMm,divisor_cm3_per_kg divisorCm3PerKg,created_at createdAt FROM order_packing_metrics WHERE order_db_id=? ORDER BY id DESC LIMIT 1').get(o.id)||null;
  const evidence=db.prepare(`SELECT id,order_item_id orderItemId,qc_check_id qcCheckId,packing_check_id packingCheckId,category,file_name fileName,mime,size_bytes sizeBytes,uploaded_by_name uploadedByName,created_at createdAt FROM evidence_files WHERE order_db_id=? ORDER BY id DESC LIMIT 200`).all(o.id).map(x=>({...x,url:`/api/evidence/${x.id}`}));
  const deletedByUser=o.deleted_by?db.prepare('SELECT full_name FROM users WHERE id=?').get(o.deleted_by):null;
  return {id:o.id,orderId:o.order_id,version:Number(o.version||1),customer:o.customer,needBy:o.need_by,priority:o.priority,currentStage:o.current_stage,stageUpdatedAt:o.stage_updated_at||o.updated_at,trackingNumber:o.tracking_number,assignedWorkerId:o.assigned_worker_id||null,assignedWorker:o.assigned_worker||'',notes:o.notes||'',createdAt:o.created_at,updatedAt:o.updated_at,deletedAt:o.deleted_at||null,deletedReason:o.deleted_reason||'',deletedBy:deletedByUser?.full_name||'',items,timeline,qcChecks,packingChecks,packingMetrics,evidence};
}

function cleanInventoryNumber(v,opts){return inventoryService.cleanNumber(v,opts);}
function normalizeMaterialCode(v){return inventoryService.normalizeCode(v);}
function cleanMaterialUnit(v){return inventoryService.cleanUnit(v);}
function getInventoryMaterial(id){return inventoryService.getMaterial(id);}
function materialCatalogRows(activeOnly=true){return inventoryService.catalog(activeOnly);}
function writeMaterialStock(id,milli,updatedAt){return inventoryService.writeStock(id,milli,updatedAt);}
function writeMaterialMinStock(id,milli,updatedAt){return inventoryService.writeMinStock(id,milli,updatedAt);}
function inventoryMovement(args){return inventoryService.movement(args);}
function validateInventoryItemReferences(items){return inventoryService.validateItemRefs(items);}
function itemNetIssuedMilli(itemId,materialId){return inventoryService.netIssuedMilli(itemId,materialId);}
function itemNetIssued(itemId,materialId){return inventoryService.netIssued(itemId,materialId);}
function orderMaterialRequirements(orderDbId){return inventoryService.requirements(orderDbId);}
function inventoryOrderPreview(orderId){return inventoryService.orderPreview(orderId);}
function issueOrderMaterials(orderId,user,opts){return inventoryService.issueOrder(orderId,user,opts);}
function returnOrderMaterial(orderId,user,args){return inventoryService.returnOrder(orderId,user,args);}

function lastDuplicate(orderId,stationCode){
  const row=db.prepare(`SELECT created_at FROM audit_events WHERE order_id=? AND station_code=? AND action LIKE 'SCAN_%' AND COALESCE(from_stage,'') <> COALESCE(to_stage,'') ORDER BY id DESC LIMIT 1`).get(orderId,stationCode);
  if(!row)return false; return Date.now()-new Date(row.created_at).getTime()<SCAN_DEBOUNCE_SECONDS*1000;
}

function autoScan(order,user,station){ return decideStationScan(order,user,station); }

function syncItemWorkflowStage(orderDbId,stage){
  const v=itemStageForOrderStage(stage);
  if(v)db.prepare('UPDATE order_items SET current_stage=?,item_status=? WHERE order_db_id=? AND active=1').run(v[0],v[1],orderDbId);
}

function applyScan(orderId,user,station,source){
  source=String(source||'SCANNER').toUpperCase();
  const key=normalizeOrderId(orderId); const o=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NULL LIMIT 1').get(orderId,key); if(!o)return {ok:false,error:'NOT_FOUND'};
  const r=autoScan(o,user,station);
  if(r.mutate){
    const t=nowIso();
    if(r.assignSelf && !o.assigned_worker_id && user.role==='PRODUCTION') db.prepare('UPDATE orders SET current_stage=?,stage_updated_at=?,assigned_worker_id=?,version=version+1,updated_at=? WHERE id=?').run(r.toStage,t,user.id,t,o.id);
    else db.prepare('UPDATE orders SET current_stage=?,stage_updated_at=?,version=version+1,updated_at=? WHERE id=?').run(r.toStage,t,t,o.id);
    syncItemWorkflowStage(o.id,r.toStage);
    audit({orderId:o.order_id,orderDbId:o.id,action:'SCAN_'+r.action,fromStage:o.current_stage,toStage:r.toStage,user,stationCode:station.code,detail:`${source} · ${r.message}`});
  } else {
    audit({orderId:o.order_id,orderDbId:o.id,action:'SCAN_'+r.kind,fromStage:o.current_stage,toStage:o.current_stage,user,stationCode:station.code,detail:`${source} · ${r.message}`});
  }
  return {ok:true,result:r,order:orderObject(o.order_id)};
}

function contentType(file){ const ext=path.extname(file).toLowerCase(); return ({'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.svg':'image/svg+xml','.ico':'image/x-icon','.woff2':'font/woff2','.webmanifest':'application/manifest+json','.txt':'text/plain; charset=utf-8' }[ext]||'application/octet-stream'); }
function serveStatic(req,res,url){
  if(!['GET','HEAD'].includes(req.method)) return sendText(res,405,'Method not allowed');
  let pathname;
  try{ pathname=safeDecodeURIComponent(url.pathname); }catch{ return sendText(res,400,'Bad request'); }
  if(pathname==='/') pathname='/enterprise.html';

  // V25 P0: legacy Etsy pages render financial labels before JS can authorize.
  // Restrict the entire sensitive legacy bundle to an authenticated FINANCE grant
  // (or root). Staff continue using the finance-free Enterprise workspace.
  const legacyFinancialAssets=new Set([
    '/alora-finance.html','/alora-finance.js',
    '/alora-orders.html','/alora-orders.js',
    '/alora-admin.html','/alora-admin.js','/alora-price-editor.js'
  ]);
  if(legacyFinancialAssets.has(pathname.toLowerCase())){
    const session=getSession(req);
    if(!session || (session.user.role!=='ADMIN' && !db.prepare(
      'SELECT 1 FROM enterprise_grants WHERE user_id=? AND (role=? OR can_finance=1) LIMIT 1'
    ).get(session.user.id,'FINANCE'))) return sendText(res,404,'Not found');
  }

  // Internal application bundle is never exposed before a valid authenticated session exists.
  // Modern browsers also have to request it as a script, which blocks normal address-bar/source navigation.
  if(pathname.toLowerCase()==='/app.js'){
    if(!getSession(req)) return sendText(res,404,'Not found');
    const dest=String(req.headers['sec-fetch-dest']||'');
    if(dest && dest!=='script') return sendText(res,404,'Not found');
  }

  // Never serve source maps, hidden files, backups or accidental secret/config files from public/.
  const lower=pathname.toLowerCase();
  const denied = lower.includes('/.') || /\.(?:map|env|ini|log|sql|sqlite|sqlite3|bak|old|orig|swp|key|pem|crt|p12|pfx|config|lock)$/i.test(lower);
  if(denied) return sendText(res,404,'Not found');

  const file=path.resolve(PUBLIC,'.'+pathname);
  const publicPrefix=PUBLIC.endsWith(path.sep)?PUBLIC:PUBLIC+path.sep;
  if(file!==PUBLIC && !file.startsWith(publicPrefix)) return sendText(res,403,'Forbidden');
  if(!fs.existsSync(file)||fs.statSync(file).isDirectory()) return sendText(res,404,'Not found');

  securityHeaders(res);
  // Only the three authenticated Etsy UI documents may be framed by this same origin.
  // Everything else keeps frame-ancestors 'none' and X-Frame-Options: DENY.
  if(['/alora-admin.html','/alora-orders.html','/alora-finance.html'].includes(pathname) && url.searchParams.get('embed')==='1'){
    res.setHeader('X-Frame-Options','SAMEORIGIN');
    res.setHeader('Content-Security-Policy',
      "default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'self'; form-action 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'; manifest-src 'self'");
  }
  res.statusCode=200;
  res.setHeader('Content-Type',contentType(file));
  const ext=path.extname(file).toLowerCase();
  if(['.png','.ico','.webmanifest'].includes(ext)) res.setHeader('Cache-Control','public, max-age=3600, must-revalidate');
  else { res.setHeader('Cache-Control','no-store, max-age=0'); res.setHeader('Pragma','no-cache'); }
  if(req.method==='HEAD') return res.end();
  fs.createReadStream(file).pipe(res);
}

const enterprise=createEnterprise({db,nowIso,readJson,sendJson,withTransaction,requireAuth,requireCsrf,sync:aloraEtsySync,evidenceDir:EVIDENCE_DIR,chatWebPush,parseDelimited,createUser,hasPermission,normalizeOrderId,orderObject,
  evidenceLimits:{maxFileBytes:EVIDENCE_MAX_FILE_BYTES,maxFiles:EVIDENCE_MAX_FILES_PER_ORDER,maxOrderBytes:EVIDENCE_MAX_BYTES_PER_ORDER},commitPreparedEvidence,cleanupPreparedEvidence,
  makeQuickScanner: permission=>createQuickScanService({db,hasPermission:permission,nextQuickAction,visualStage,normalizeOrderId,cleanText,sha256,withTransaction,nowIso,syncItemWorkflowStage,audit,orderObject,DEFECT_CATEGORIES})});

async function api(req,res,url){
  try{
    if(req.method==='GET'&&url.pathname==='/api/pwa/push/config'){
      // Public VAPID key is not a secret; no login required for feature detection.
      return sendJson(res,200,{ok:true,enabled:chatWebPush.enabled,publicKey:chatWebPush.publicKey});
    }
    if(url.pathname==='/api/pwa/push/subscriptions' && ['POST','DELETE'].includes(req.method)){
      const a=requireAuth(req,res);if(!a)return;
      if(a.bearer)return sendJson(res,403,{ok:false,error:'COOKIE_LOGIN_REQUIRED'});
      if(!requireCsrf(req,res,a))return;
      if(!requestIsHttps(req)&&!['127.0.0.1','::1'].includes(clientIp(req))&&NODE_ENV!=='test')return sendJson(res,403,{ok:false,error:'HTTPS_REQUIRED'});
      if(a.user.must_change_password)return sendJson(res,428,{ok:false,error:'PASSWORD_CHANGE_REQUIRED'});
      const b=await readJson(req);
      if(req.method==='POST'){
        const result=chatWebPush.register(a.user.id,b);
        return sendJson(res,result.ok?200:400,result);
      }
      return sendJson(res,200,{ok:chatWebPush.unregister(a.user.id,b.endpoint)});
    }
    if(req.method==='GET'&&url.pathname==='/api/health'){
      try{ db.prepare('SELECT 1 ok').get(); return sendJson(res,200,{ok:true,status:'healthy',version:APP_VERSION}); }
      catch{ return sendJson(res,503,{ok:false,status:'unhealthy'}); }
    }
    if(url.pathname.startsWith('/api/enterprise/'))return await enterprise.handle(req,res,url);
    if(url.pathname.startsWith('/api/alora/'))return await aloraEtsySync.handle(req,res,url);
    if(url.pathname.startsWith('/api/intake/receive/')){const handled=await multiSourceIntake.incoming(req,res,url);if(handled===false)return sendJson(res,404,{ok:false,error:'NOT_FOUND',message:'Nguồn không hợp lệ.'});return handled;}
    if(req.method==='POST'&&url.pathname==='/api/mobile/auth/login'){
      // Mobile credentials and bearer tokens must not cross a public plaintext connection.
      if(!requestIsHttps(req)&&NODE_ENV!=='test'&&clientIp(req)!=='127.0.0.1'&&clientIp(req)!=='::1')
        return sendJson(res,403,{ok:false,error:'HTTPS_REQUIRED',message:'App điện thoại cần kết nối máy chủ HTTPS.'});
      const b=await readJson(req),username=String(b.username||'').trim().toLowerCase(),password=String(b.password||'');
      const key=`${clientIp(req)}|${username}`;
      if(!loginAllowed(key))return sendJson(res,429,{ok:false,message:'Đăng nhập tạm khóa vì thử sai quá nhiều lần.'});
      const u=db.prepare('SELECT * FROM users WHERE username=? AND deleted_at IS NULL').get(username);
      const orgActive=u?.role!=='PARTNER'||!!db.prepare('SELECT 1 FROM chat_partner_orgs WHERE id=? AND active=1').get(u.partner_org_id);
      if(!u||!u.active||!orgActive||!verifyPassword(password,u.salt,u.password_hash)){
        loginFail(key);return sendJson(res,401,{ok:false,message:'Tài khoản hoặc mật khẩu không đúng.'});
      }
      loginSuccess(key);
      const token=randomToken(),t=nowIso();
      db.prepare('INSERT INTO chat_mobile_sessions(token_hash,user_id,auth_version,created_at,expires_at) VALUES(?,?,?,?,?)').run(sha256(token),u.id,Number(u.auth_version||1),t,addHoursIso(24*30));
      audit({action:'CHAT_MOBILE_LOGIN',user:u});
      return sendJson(res,200,{ok:true,token,user:safeUser(u),expiresAt:addHoursIso(24*30)});
    }
    if(req.method==='POST'&&url.pathname==='/api/auth/login'){
      const body=await readJson(req); const username=String(body.username||'').trim().toLowerCase(); const password=String(body.password||'');
      const key=`${clientIp(req)}|${username}`; if(!loginAllowed(key))return sendJson(res,429,{ok:false,message:`Đã nhập sai quá ${LOGIN_MAX_FAILURES} lần liên tiếp. Tạm khóa đăng nhập ${LOGIN_LOCK_MINUTES} phút.`});
      const u=db.prepare('SELECT * FROM users WHERE username=? AND deleted_at IS NULL').get(username);
      if(!u||!u.active||(u.role==='PARTNER'&&!db.prepare('SELECT 1 FROM chat_partner_orgs WHERE id=? AND active=1').get(u.partner_org_id))||!verifyPassword(password,u.salt,u.password_hash)){loginFail(key); audit({action:'LOGIN_FAILED',detail:`username=${username}`}); return sendJson(res,401,{ok:false,message:'Tài khoản hoặc mật khẩu không đúng.'});}
      loginSuccess(key); const token=randomToken(); const csrf=randomToken(18); const t=nowIso();
      db.prepare('INSERT INTO sessions(token_hash,csrf_token,user_id,expires_at,created_at,last_seen_at,auth_version) VALUES(?,?,?,?,?,?,?)').run(sha256(token),csrf,u.id,addHoursIso(SESSION_HOURS),t,t,Number(u.auth_version||1));
      const oldSessions=db.prepare('SELECT id FROM sessions WHERE user_id=? ORDER BY id DESC LIMIT -1 OFFSET ?').all(u.id,MAX_SESSIONS_PER_USER);
      for(const sess of oldSessions) db.prepare('DELETE FROM sessions WHERE id=?').run(sess.id);
      setSessionCookie(req,res,token); audit({action:'LOGIN',user:u,detail:'Login success'}); return sendJson(res,200,{ok:true,user:safeUser(u),csrfToken:csrf});
    }
    if(url.pathname==='/api/mobile/auth/me'&&req.method==='GET'){
      const a=requireAuth(req,res);if(!a||!a.bearer)return a?sendJson(res,403,{ok:false,error:'BEARER_REQUIRED'}):undefined;
      return sendJson(res,200,{ok:true,user:safeUser(a.user)});
    }
    if(url.pathname==='/api/mobile/auth/logout'&&req.method==='POST'){
      const a=requireAuth(req,res);if(!a||!a.bearer)return a?sendJson(res,403,{ok:false,error:'BEARER_REQUIRED'}):undefined;
      const b=await readJson(req);if(b.expoToken)db.prepare('DELETE FROM chat_push_devices WHERE expo_token=? AND user_id=?').run(String(b.expoToken),a.user.id);
      db.prepare('DELETE FROM chat_mobile_sessions WHERE token_hash=?').run(a.tokenHash);
      return sendJson(res,200,{ok:true});
    }
    if(url.pathname==='/api/mobile/auth/change-password'&&req.method==='POST'){
      const a=requireAuth(req,res);if(!a||!a.bearer)return a?sendJson(res,403,{ok:false,error:'BEARER_REQUIRED'}):undefined;
      const b=await readJson(req);
      if(!verifyPassword(String(b.currentPassword||''),a.user.salt,a.user.password_hash))return sendJson(res,400,{ok:false,message:'Mật khẩu hiện tại không đúng.'});
      const err=validatePassword(b.newPassword);if(err)return sendJson(res,400,{ok:false,message:err});
      if(b.currentPassword===b.newPassword)return sendJson(res,400,{ok:false,message:'Mật khẩu mới phải khác mật khẩu cũ.'});
      const salt=makeSalt(),t=nowIso();
      withTransaction(()=>{
        db.prepare('UPDATE users SET password_hash=?,salt=?,must_change_password=0,password_changed_at=?,auth_version=auth_version+1,updated_at=? WHERE id=?').run(hashPassword(b.newPassword,salt),salt,t,t,a.user.id);
        db.prepare('DELETE FROM chat_mobile_sessions WHERE user_id=? AND token_hash<>?').run(a.user.id,a.tokenHash);
        db.prepare('DELETE FROM sessions WHERE user_id=?').run(a.user.id);
        db.prepare('UPDATE chat_mobile_sessions SET auth_version=? WHERE token_hash=?').run(Number(getUserById(a.user.id).auth_version||1),a.tokenHash);
      });
      return sendJson(res,200,{ok:true,user:safeUser(getUserById(a.user.id))});
    }
    if(req.method==='GET'&&url.pathname==='/api/me'){
      const a=requireAuth(req,res); if(!a)return; return sendJson(res,200,{ok:true,user:safeUser(a.user),csrfToken:a.session.csrf_token});
    }
    if(req.method==='GET'&&url.pathname==='/api/system/meta'){
      const a=requireAuth(req,res);if(!a)return;
      return sendJson(res,200,{ok:true,appVersion:APP_VERSION,schemaVersion:Number(db.prepare('SELECT COALESCE(MAX(version),0) v FROM schema_migrations').get().v||0),dataContractVersion:1,workflowStages:STAGE_ORDER,inventoryPrecision:0.001,loginPolicy:{maxFailures:LOGIN_MAX_FAILURES,lockMinutes:LOGIN_LOCK_MINUTES}});
    }
    if(req.method==='POST'&&url.pathname==='/api/auth/logout'){
      const a=requireAuth(req,res); if(!a)return; if(!requireCsrf(req,res,a))return; db.prepare('DELETE FROM sessions WHERE id=?').run(a.session.id); clearSessionCookie(req,res); audit({action:'LOGOUT',user:a.user}); return sendJson(res,200,{ok:true});
    }
    if(req.method==='POST'&&url.pathname==='/api/auth/change-password'){
      const a=requireAuth(req,res); if(!a)return;if(!requireCsrf(req,res,a))return;
      const b=await readJson(req);
      if(!verifyPassword(String(b.currentPassword||''),a.user.salt,a.user.password_hash))return sendJson(res,400,{ok:false,message:'Mật khẩu hiện tại không đúng.'});
      const err=validatePassword(b.newPassword); if(err)return sendJson(res,400,{ok:false,message:err});
      if(String(b.newPassword)===String(b.currentPassword))return sendJson(res,400,{ok:false,message:'Mật khẩu mới phải khác mật khẩu hiện tại.'});
      const salt=makeSalt(),hash=hashPassword(b.newPassword,salt),t=nowIso();
      withTransaction(()=>{
        db.prepare('UPDATE users SET password_hash=?,salt=?,must_change_password=0,password_changed_at=?,auth_version=auth_version+1,updated_at=? WHERE id=?').run(hash,salt,t,t,a.user.id);
        db.prepare('DELETE FROM sessions WHERE user_id=? AND id<>?').run(a.user.id,a.session.id);
        db.prepare('DELETE FROM chat_mobile_sessions WHERE user_id=?').run(a.user.id);
        const u=getUserById(a.user.id);
        db.prepare('UPDATE sessions SET auth_version=? WHERE id=?').run(Number(u.auth_version||1),a.session.id);
      });
      audit({action:'PASSWORD_CHANGED',user:getUserById(a.user.id),detail:'Other sessions revoked'});
      return sendJson(res,200,{ok:true});
    }

    const a=requireAuth(req,res); if(!a)return;
    if(['POST','PUT','PATCH','DELETE'].includes(req.method)&&!requireCsrf(req,res,a))return;
    if(a.user.must_change_password){ return sendJson(res,428,{ok:false,error:'PASSWORD_CHANGE_REQUIRED',message:'Bạn phải đổi mật khẩu trước khi sử dụng hệ thống.'}); }
    // Partner role is never permitted to reach legacy OPS, inventory, finance, reports or admin routes.
    // Safety barrier: legacy APIs have no Team scoping; deny rather than leak between teams.
    // Keep disabled on existing installations until ALL legacy modules have been migrated.
    if((process.env.ALORA_TEAM_GUARD==='1' || aloraTeamsRequireLegacyGuard()) && a.user.role!=='ADMIN')return sendJson(res,403,{ok:false,error:'TEAM_LEGACY_API_NOT_READY',message:'Legacy module not yet team-scoped; access denied.'});
    if(['POST','PATCH','PUT','DELETE'].includes(req.method)){
      const legacyLinked=url.pathname.match(/^\/api\/(?:orders|sales\/orders|inventory\/orders)\/([^/]+)/);
      if(legacyLinked){const orderKey=safeDecodeURIComponent(legacyLinked[1]);if(db.prepare('SELECT 1 FROM alora_order_links l JOIN orders o ON o.id=l.order_db_id WHERE o.order_id=? OR o.order_key=?').get(orderKey,normalizeOrderId(orderKey)))return sendJson(res,409,{ok:false,error:'ENTERPRISE_ORDER_REQUIRED',message:'Đơn đã vào Workspace mới. Cập nhật tại /enterprise.html để giữ quyền và khóa sổ.'});}
    }
    if(url.pathname==='/api/realtime/events'&&req.method==='GET')return realtime.connect(req,res,a);
    if(url.pathname.startsWith('/api/chat/'))return await chatService.handle(req,res,url,a);
    if(url.pathname.startsWith('/api/intake/'))return await multiSourceIntake.admin(req,res,url,a);
    if(a.user.role==='PARTNER')return sendJson(res,403,{ok:false,error:'PARTNER_PORTAL_ONLY',message:'Đối tác chỉ được dùng mục trao đổi của đơn được cấp quyền.'});

    if(req.method==='POST'&&url.pathname==='/api/admin/backup'){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được quản lý backup.'});
      const out=await createRuntimeBackup('manual');
      if(!out)return sendJson(res,500,{ok:false,message:'Không thể tạo backup.'});
      audit({action:'DATABASE_BACKUP',user:a.user,detail:path.basename(out)});
      return sendJson(res,200,{ok:true,file:path.basename(out)});
    }

    if(req.method==='GET'&&url.pathname==='/api/admin/backups'){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được quản lý backup.'});
      const files=fs.readdirSync(BACKUP_DIR).filter(x=>/\.(?:alorabak|sqlite)(?:\.enc)?$/.test(x)&&!x.startsWith('.')).map(name=>{ const st=fs.statSync(path.join(BACKUP_DIR,name)); return {name,size:st.size,createdAt:st.mtime.toISOString(),fullBackup:name.includes('.alorabak')}; }).sort((x,y)=>y.createdAt.localeCompare(x.createdAt));
      return sendJson(res,200,{ok:true,backups:files.slice(0,100)});
    }
    const backupDownload=url.pathname.match(/^\/api\/admin\/backups\/([^/]+)\/download$/);
    if(req.method==='GET'&&backupDownload){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được quản lý backup.'});
      const name=path.basename(safeDecodeURIComponent(backupDownload[1]));
      if(!/^3alora-[A-Za-z0-9_.-]+\.(?:alorabak|sqlite)(?:\.enc)?$/.test(name))return sendJson(res,400,{ok:false,message:'Tên backup không hợp lệ.'});
      const full=path.join(BACKUP_DIR,name); if(!fs.existsSync(full))return sendJson(res,404,{ok:false,message:'Backup không tồn tại.'});
      securityHeaders(res); res.statusCode=200; res.setHeader('Cache-Control','no-store'); res.setHeader('Content-Type','application/octet-stream'); res.setHeader('Content-Disposition',`attachment; filename="${name}"`); return fs.createReadStream(full).pipe(res);
    }

    const backupVerify=url.pathname.match(/^\/api\/admin\/backups\/([^/]+)\/verify$/);
    if(req.method==='POST'&&backupVerify){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được quản lý backup.'});
      const name=path.basename(safeDecodeURIComponent(backupVerify[1])),full=path.join(BACKUP_DIR,name); if(!fs.existsSync(full))return sendJson(res,404,{ok:false,message:'Backup không tồn tại.'});
      const result=verifyBackupFile(full); audit({action:'BACKUP_VERIFIED',user:a.user,detail:`${name}; ${JSON.stringify(result)}`}); return sendJson(res,result.ok?200:422,{ok:result.ok,...result});
    }
    const backupRestore=url.pathname.match(/^\/api\/admin\/backups\/([^/]+)\/restore$/);
    if(req.method==='POST'&&backupRestore){
      if(a.user.role!=='ADMIN'||!hasPermission(a.user,'backup.manage'))return sendJson(res,403,{ok:false,message:'Chỉ Admin được lên lịch khôi phục database.'});
      const name=path.basename(safeDecodeURIComponent(backupRestore[1])),full=path.join(BACKUP_DIR,name); if(!fs.existsSync(full))return sendJson(res,404,{ok:false,message:'Backup không tồn tại.'}); const check=verifyBackupFile(full); if(!check.ok)return sendJson(res,422,{ok:false,message:'Backup không đạt kiểm tra toàn vẹn.',detail:check.detail});
      const b=await readJson(req); if(String(b.confirm||'')!=='RESTORE')return sendJson(res,400,{ok:false,message:'Nhập RESTORE để xác nhận.'});
      const pinnedName='3alora-pinned-'+crypto.randomUUID()+(name.includes('.alorabak')?'.alorabak':'.sqlite')+(name.endsWith('.enc')?'.enc':'');fs.copyFileSync(full,path.join(BACKUP_DIR,pinnedName));
      const pre=await createRuntimeBackup('pre-restore');if(!pre)return sendJson(res,500,{ok:false,message:'Không thể tạo full backup trước restore nên hệ thống đã hủy thao tác.'});
      fs.writeFileSync(RESTORE_MARKER,JSON.stringify({name:pinnedName,requestedAt:nowIso(),requestedBy:a.user.username,preRestore:path.basename(pre)})); audit({action:'DATABASE_RESTORE_SCHEDULED',user:a.user,detail:`restore=${name}; preRestore=${path.basename(pre)}`}); return sendJson(res,202,{ok:true,message:'Đã kiểm tra backup, tạo full pre-restore và lên lịch khôi phục. Restart server để áp dụng; mọi session cũ sẽ bị thu hồi.'});
    }
    if(req.method==='GET'&&url.pathname==='/api/admin/data-quality'){
      if(a.user.role!=='ADMIN')return sendJson(res,403,{ok:false,message:'Chỉ Admin.'});
      return sendJson(res,200,{ok:true,report:dataQualityReport()});
    }
    if(req.method==='GET'&&url.pathname==='/api/admin/diagnostics'){
      if(a.user.role!=='ADMIN')return sendJson(res,403,{ok:false,message:'Chỉ Admin.'}); const st=fs.statSync(DB_PATH),disk=fs.statfsSync(path.dirname(DB_PATH)); return sendJson(res,200,{ok:true,version:APP_VERSION,persistRoot:PERSIST_ROOT,dbPath:DB_PATH,evidenceDir:EVIDENCE_DIR,dbBytes:st.size,freeBytes:Number(disk.bavail)*Number(disk.bsize),schemaVersion:db.prepare('SELECT MAX(version) v FROM schema_migrations').get().v||0,wal:true,backupDir:BACKUP_DIR,offsiteConfigured:!!OFFSITE_BACKUP_DIR,backupEncrypted:!!BACKUP_ENCRYPTION_KEY,externalConfigPath:EXTERNAL_CONFIG_PATH||null,legacyDataMigrated:!!LEGACY_DATA_MIGRATION?.migrated,dataQuality:dataQualityReport()});
    }

    const evidenceGet=url.pathname.match(/^\/api\/evidence\/(\d+)$/);
    if(req.method==='GET'&&evidenceGet){
      const e=db.prepare(`SELECT e.*,o.order_id,o.deleted_at FROM evidence_files e JOIN orders o ON o.id=e.order_db_id WHERE e.id=?`).get(Number(evidenceGet[1])); if(!e)return sendText(res,404,'Not found');
      if(e.deleted_at && !hasPermission(a.user,'orders.view_deleted'))return sendText(res,404,'Not found');
      if(!hasPermission(a.user,'orders.view_all')&&!userCanViewOrder(a.user,e.order_id,{includeDeleted:!!e.deleted_at}))return sendText(res,404,'Not found');
      const full=path.resolve(EVIDENCE_DIR,e.storage_path),prefix=path.resolve(EVIDENCE_DIR)+path.sep;if(!full.startsWith(prefix)||!fs.existsSync(full))return sendText(res,404,'Not found');
      if(e.content_sha256&&shaFile(full)!==e.content_sha256)return sendText(res,409,'Evidence integrity check failed');
      securityHeaders(res);res.statusCode=200;res.setHeader('Cache-Control','private, no-store');res.setHeader('Content-Type',e.mime);return fs.createReadStream(full).pipe(res);
    }
    const evidencePost=url.pathname.match(/^\/api\/orders\/([^/]+)\/evidence$/);
    if(req.method==='POST'&&evidencePost){
      const orderId=safeDecodeURIComponent(evidencePost[1]),key=normalizeOrderId(orderId),o=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NULL LIMIT 1').get(orderId,key);if(!o)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});
      if(!userCanViewOrder(a.user,o.order_id)&&!hasPermission(a.user,'orders.view_all'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền thêm bằng chứng cho Order này.'});
      const b=await readJson(req),category=String(b.category||'').toUpperCase();if(!['PRODUCT','QC_PASS','QC_FAIL','PACKING'].includes(category))return sendJson(res,400,{ok:false,message:'Loại bằng chứng không hợp lệ.'});
      if(category==='PRODUCT'){
        if(!hasPermission(a.user,'orders.edit'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền sửa đơn để thêm ảnh sản phẩm.'});
        await validateProductImage(b.dataUrl);
        // Decoding is asynchronous: recheck the live order and permission after it completes.
        const current=db.prepare('SELECT deleted_at FROM orders WHERE id=?').get(o.id);
        if(!current||current.deleted_at)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});
        const uploader=db.prepare('SELECT * FROM users WHERE id=? AND active=1 AND deleted_at IS NULL').get(a.user.id);
        if(!uploader||!hasPermission(uploader,'orders.edit')||(!hasPermission(uploader,'orders.view_all')&&!userCanViewOrder(uploader,o.order_id)))return sendJson(res,403,{ok:false,message:'Quyền sửa đơn đã thay đổi.'});
      }
      if(category.startsWith('QC')&&!hasPermission(a.user,'qc.execute'))return sendJson(res,403,{ok:false,message:'Không có quyền upload ảnh Kiểm Tra.'});if(category==='PACKING'&&!hasPermission(a.user,'station.packing'))return sendJson(res,403,{ok:false,message:'Không có quyền upload ảnh đóng gói.'});
      let prep;try{prep=prepareEvidence(b,o,a.user,category);if(!prep)throw badRequest('Thiếu ảnh bằng chứng.','EVIDENCE_REQUIRED');const id=withTransaction(()=>{const id=commitPreparedEvidence(prep,o,a.user);audit({orderId:o.order_id,orderDbId:o.id,action:'EVIDENCE_UPLOADED',user:a.user,detail:`${category}; evidenceId=${id}`});return id;});return sendJson(res,201,{ok:true,id,url:`/api/evidence/${id}`});}catch(e){cleanupPreparedEvidence(prep);throw e}
    }



    // ===== STOCK PRODUCT CATEGORIES, SIZED SKU & STOCK SHEETS =====
    if(req.method==='GET'&&url.pathname==='/api/inventory/categories'){
      if(!['inventory.view','inventory.receive','inventory.issue','inventory.manage','orders.create','orders.edit'].some(p=>hasPermission(a.user,p)))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xem nhóm hàng.'});
      return sendJson(res,200,{ok:true,categories:db.prepare('SELECT id,name FROM inventory_categories ORDER BY id').all()});
    }
    if(req.method==='POST'&&url.pathname==='/api/inventory/categories'){
      if(!hasPermission(a.user,'inventory.manage'))return sendJson(res,403,{ok:false,message:'Chỉ người có quyền quản lý kho mới được thêm nhóm.'});
      const b=await readJson(req),name=cleanText(b.name||'',80,'Nhóm sản phẩm').trim();if(!name)throw badRequest('Tên nhóm không được trống.','STOCK_CATEGORY_EMPTY');
      const found=db.prepare('SELECT id,name FROM inventory_categories WHERE name=? COLLATE NOCASE').get(name);
      if(found)return sendJson(res,200,{ok:true,category:found,existing:true});
      const id=db.prepare('INSERT INTO inventory_categories(name,created_at) VALUES(?,?)').run(name,nowIso()).lastInsertRowid;
      audit({action:'STOCK_CATEGORY_CREATED',user:a.user,detail:name});return sendJson(res,201,{ok:true,category:{id:Number(id),name}});
    }
    if(req.method==='POST'&&url.pathname==='/api/inventory/stock-import/preview'){
      if(!hasPermission(a.user,'inventory.receive'))return sendJson(res,403,{ok:false,message:'Không có quyền nhập kho từ Sheets.'});
      const b=await readJson(req);let text=String(b.text||'');if(String(b.sheetUrl||'').trim())text=await fetchSheetText(b.sheetUrl);
      const parsed=stockImportPreview(text);
      return sendJson(res,200,{ok:true,...parsed,sourceHash:sha256(text.trim().replace(/\r\n/g,'\n')),receiptHash:stockReceiptFingerprint(parsed.rows)});
    }
    if(req.method==='POST'&&url.pathname==='/api/inventory/stock-import/commit'){
      if(!hasPermission(a.user,'inventory.receive'))return sendJson(res,403,{ok:false,message:'Không có quyền nhập kho từ Sheets.'});
      const b=await readJson(req);let text=String(b.text||'');if(String(b.sheetUrl||'').trim())text=await fetchSheetText(b.sheetUrl);
      if(b.expectedHash&&crypto.createHash('sha256').update(text.trim().replace(/\r\n/g,'\n')).digest('hex')!==b.expectedHash)throw conflict('Sheet đã thay đổi sau khi xem trước; hãy Preview lại trước khi nhập.','STOCK_SHEET_CHANGED');
      return sendJson(res,200,{ok:true,...stockImportCommit(text,a.user,String(b.expectedReceiptHash||''))});
    }

    // ===== FABRIC INVENTORY =====
    if(req.method==='GET'&&url.pathname==='/api/inventory/materials'){
      const canCatalog=hasPermission(a.user,'inventory.view')||hasPermission(a.user,'inventory.receive')||hasPermission(a.user,'inventory.issue')||hasPermission(a.user,'inventory.manage')||hasPermission(a.user,'orders.create')||hasPermission(a.user,'orders.edit');
      if(!canCatalog)return sendJson(res,403,{ok:false,message:'Bạn không có quyền xem danh mục vải.'});
      const includeInactive=(url.searchParams.get('all')==='1')&&(a.user.role==='ADMIN'||hasPermission(a.user,'inventory.manage'));
      return sendJson(res,200,{ok:true,materials:materialCatalogRows(!includeInactive)});
    }
    if(req.method==='POST'&&url.pathname==='/api/inventory/materials'){
      if(!hasPermission(a.user,'inventory.manage'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền tạo danh mục vải.'});
      const b=await readJson(req),code=normalizeMaterialCode(b.code),name=cleanText(b.name||'',160,'Tên vải');if(!name)return sendJson(res,400,{ok:false,message:'Tên vải không được trống.'});
      const categoryId=stockCategoryId(b.categoryId),dims=stockDimensions(b);if(dims.widthCm&&cleanMaterialUnit(b.unit)!=='PCS')throw badRequest('Hàng kích thước cố định dùng PCS (tấm/chiếc).','STOCK_SIZE_UNIT');const color=cleanText(b.color||'',120,'Màu'),unit=cleanMaterialUnit(b.unit),minStock=cleanInventoryNumber(b.minStock||0,{name:'Tồn tối thiểu'}),notes=cleanText(b.notes||'',1000,'Ghi chú'),t=nowIso();
      requireWholeMinStock(unit,minStock);
      try{const minMilli=toMilliUnits(minStock);const r=db.prepare(`INSERT INTO inventory_materials(code,name,color,unit,current_stock,min_stock,current_stock_milli,min_stock_milli,active,notes,created_at,updated_at,category_id,width_cm,length_cm,size_label) VALUES(?,?,?,?,0,?,0,?,1,?,?,?,?,?,?,?)`).run(code,name,color,unit,minStock,minMilli,notes,t,t,categoryId,dims.widthCm||null,dims.lengthCm||null,dims.sizeLabel);audit({action:'INVENTORY_MATERIAL_CREATED',user:a.user,detail:`${code} · ${name}`});return sendJson(res,201,{ok:true,material:getInventoryMaterial(r.lastInsertRowid)});}catch(e){if(String(e.message).includes('UNIQUE'))return sendJson(res,409,{ok:false,message:'Mã vải đã tồn tại.'});throw e}
    }
    let invMat=url.pathname.match(/^\/api\/inventory\/materials\/(\d+)$/);
    if(req.method==='PATCH'&&invMat){
      if(!hasPermission(a.user,'inventory.manage'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền sửa danh mục vải.'});
      const id=Number(invMat[1]),old=getInventoryMaterial(id);if(!old)return sendJson(res,404,{ok:false,message:'Không tìm thấy vải.'});const b=await readJson(req),code=b.code===undefined?old.code:normalizeMaterialCode(b.code),name=b.name===undefined?old.name:cleanText(b.name,160,'Tên vải'),color=b.color===undefined?old.color:cleanText(b.color,120,'Màu'),unit=b.unit===undefined?old.unit:cleanMaterialUnit(b.unit),minStock=b.minStock===undefined?old.minStock:cleanInventoryNumber(b.minStock,{name:'Tồn tối thiểu'}),active=b.active===undefined?!!old.active:!!b.active,notes=b.notes===undefined?old.notes:cleanText(b.notes,1000,'Ghi chú'),categoryId=stockCategoryId(b.categoryId===undefined?old.categoryId:b.categoryId),dims=stockDimensions(b,old);if(dims.widthCm&&unit!=='PCS')throw badRequest('Hàng kích thước cố định dùng PCS.','STOCK_SIZE_UNIT');if(!name)throw badRequest('Tên vật tư không được trống.');requireWholeMinStock(unit,minStock);const hasMovement=Number(old.currentStock)>0||!!db.prepare('SELECT 1 FROM inventory_movements WHERE material_id=? LIMIT 1').get(id);if(hasMovement&&(code!==old.code||unit!==old.unit||dims.widthCm!==old.widthCm||dims.lengthCm!==old.lengthCm))throw conflict('SKU đã có lịch sử kho: không được sửa mã, đơn vị hoặc kích thước. Tạo SKU mới thay thế.','STOCK_SKU_IMMUTABLE');
      try{db.prepare('UPDATE inventory_materials SET code=?,name=?,color=?,unit=?,min_stock=?,min_stock_milli=?,active=?,notes=?,updated_at=?,category_id=?,width_cm=?,length_cm=?,size_label=? WHERE id=?').run(code,name,color,unit,minStock,toMilliUnits(minStock),active?1:0,notes,nowIso(),categoryId,dims.widthCm||null,dims.lengthCm||null,dims.sizeLabel,id);audit({action:'INVENTORY_MATERIAL_EDITED',user:a.user,detail:`${old.code} → ${code}`});return sendJson(res,200,{ok:true,material:getInventoryMaterial(id)});}catch(e){if(String(e.message).includes('UNIQUE'))return sendJson(res,409,{ok:false,message:'Mã vải đã tồn tại.'});throw e}
    }
    let invReceive=url.pathname.match(/^\/api\/inventory\/materials\/(\d+)\/receive$/);
    if(req.method==='POST'&&invReceive){
      if(!hasPermission(a.user,'inventory.receive'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền nhập kho.'});
      const id=Number(invReceive[1]),m=getInventoryMaterial(id);if(!m||!m.active)return sendJson(res,404,{ok:false,message:'Không tìm thấy vải đang hoạt động.'});const b=await readJson(req),qty=cleanInventoryNumber(b.qty,{allowZero:false,name:'Số lượng nhập'}),lot=cleanText(b.lotCode||'',120,'Mã lô'),supplier=cleanText(b.supplier||'',160,'Nhà cung cấp'),note=cleanText(b.note||'',1000,'Ghi chú'),effectiveDate=stockDate(b.date);requireWholePieces(m,qty);
      const result=withStockTransaction(a.user,{...b,__scope:req.url},()=>{const fresh=getInventoryMaterial(id),beforeMilli=Number(fresh.currentStockMilli),qtyMilli=toMilliUnits(qty),afterMilli=beforeMilli+qtyMilli;writeMaterialStock(id,afterMilli);const before=fromMilliUnits(beforeMilli),after=fromMilliUnits(afterMilli);const movementId=inventoryMovement({materialId:id,type:'RECEIVE',quantity:qty,before,after,effectiveDate,reference:lot||'RECEIVE',note:[supplier&&`NCC: ${supplier}`,note].filter(Boolean).join(' · '),user:a.user});audit({action:'INVENTORY_RECEIVED',user:a.user,entityType:'INVENTORY_MATERIAL',entityId:String(id),detail:`${fresh.code} +${qty}${fresh.unit}${lot?` · lot=${lot}`:''}`});return {movementId,before,after};});return sendJson(res,200,{ok:true,material:getInventoryMaterial(id),...result});
    }

    let invManualIssue=url.pathname.match(/^\/api\/inventory\/materials\/(\d+)\/issue$/);
    if(req.method==='POST'&&invManualIssue){
      if(!hasPermission(a.user,'inventory.issue'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xuất kho.'});
      const id=Number(invManualIssue[1]),m=getInventoryMaterial(id);if(!m||!m.active)return sendJson(res,404,{ok:false,message:'Không tìm thấy SKU hoạt động.'});
      const b=await readJson(req),qty=cleanInventoryNumber(b.qty,{allowZero:false,name:'Số lượng xuất'}),reference=cleanText(b.reference||'',120,'Phiếu xuất'),note=cleanText(b.note||'',1000,'Lý do xuất'),effectiveDate=stockDate(b.date);
      if(!reference&&!note)throw badRequest('Xuất tay cần mã phiếu hoặc lý do để đối soát.','STOCK_ISSUE_REASON');requireWholePieces(m,qty);
      const result=withStockTransaction(a.user,{...b,__scope:req.url},()=>{const fresh=getInventoryMaterial(id),beforeMilli=Number(fresh.currentStockMilli),afterMilli=beforeMilli-toMilliUnits(qty);if(afterMilli<0)throw conflict(`SKU ${fresh.code} chỉ còn ${fresh.currentStock} ${fresh.unit}, không đủ xuất ${qty}.`,'INVENTORY_SHORTAGE');writeMaterialStock(id,afterMilli);const movementId=inventoryMovement({materialId:id,type:'ISSUE',quantity:-qty,before:fresh.currentStock,after:fromMilliUnits(afterMilli),reference:reference||'XUAT-TAY',note,effectiveDate,user:a.user});audit({action:'STOCK_MANUAL_ISSUE',user:a.user,entityType:'INVENTORY_MATERIAL',entityId:String(id),detail:`${fresh.code} -${qty} ${fresh.unit} · ${reference||note}`});return {movementId,before:fresh.currentStock,after:fromMilliUnits(afterMilli)};});
      return sendJson(res,200,{ok:true,material:getInventoryMaterial(id),...result});
    }

    let invAdjust=url.pathname.match(/^\/api\/inventory\/materials\/(\d+)\/adjust$/);
    if(req.method==='POST'&&invAdjust){
      if(!hasPermission(a.user,'inventory.adjust'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền điều chỉnh tồn kho.'});
      const id=Number(invAdjust[1]),m=getInventoryMaterial(id);if(!m)return sendJson(res,404,{ok:false,message:'Không tìm thấy vải.'});const b=await readJson(req),delta=Number(b.delta);if(['PCS','ROLL'].includes(m.unit)&&!Number.isInteger(delta))throw badRequest('Điều chỉnh theo tấm/chiếc cần số nguyên.','STOCK_ADJUST_INTEGER');if(!Number.isFinite(delta)||Math.abs(delta)<1e-9||Math.abs(delta)>1_000_000_000)return sendJson(res,400,{ok:false,message:'Số lượng điều chỉnh không hợp lệ.'});if(Math.abs(delta*1000-Math.round(delta*1000))>1e-7)throw badRequest('Số lượng điều chỉnh chỉ được tối đa 3 chữ số thập phân.','INVENTORY_PRECISION_INVALID');const note=cleanText(b.note||'',1000,'Lý do điều chỉnh');if(!note)return sendJson(res,400,{ok:false,message:'Điều chỉnh tồn kho bắt buộc ghi lý do.'});
      const result=withStockTransaction(a.user,{...b,__scope:req.url},()=>{const fresh=getInventoryMaterial(id),beforeMilli=Number(fresh.currentStockMilli),deltaMilli=toMilliUnits(delta),afterMilli=beforeMilli+deltaMilli;if(afterMilli<0)throw conflict('Điều chỉnh sẽ làm tồn kho âm.','INVENTORY_NEGATIVE');writeMaterialStock(id,afterMilli);const before=fromMilliUnits(beforeMilli),after=fromMilliUnits(afterMilli);const movementId=inventoryMovement({materialId:id,type:'ADJUST',quantity:fromMilliUnits(deltaMilli),before,after,reference:'ADJUST',note,user:a.user});audit({action:'INVENTORY_ADJUSTED',user:a.user,entityType:'INVENTORY_MATERIAL',entityId:String(id),detail:`${fresh.code} ${delta>0?'+':''}${roundDisplay(delta)}${fresh.unit} · ${note}`});return {movementId,before,after};});return sendJson(res,200,{ok:true,material:getInventoryMaterial(id),...result});
    }
    if(req.method==='GET'&&url.pathname==='/api/inventory/movements'){
      if(!['inventory.view','inventory.receive','inventory.issue'].some(p=>hasPermission(a.user,p)))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xem lịch sử kho.'});const limit=positiveLimit(url.searchParams.get('limit')),materialId=Number(url.searchParams.get('materialId')||0),fromRaw=url.searchParams.get('from')||'',toRaw=url.searchParams.get('to')||'';const from=fromRaw?stockDate(fromRaw):'',to=toRaw?stockDate(toRaw):'';if(from&&to&&from>to)throw badRequest('Khoảng ngày lọc không hợp lệ.','STOCK_DATE_RANGE');const terms=[],params=[];if(materialId){terms.push('mv.material_id=?');params.push(materialId);}if(from){terms.push('COALESCE(mv.effective_date,SUBSTR(mv.created_at,1,10))>=?');params.push(from);}if(to){terms.push('COALESCE(mv.effective_date,SUBSTR(mv.created_at,1,10))<=?');params.push(to);}const where=terms.length?'WHERE '+terms.join(' AND '):'';const rows=db.prepare(`SELECT mv.id,mv.movement_type movementType,COALESCE(mv.quantity_milli,ROUND(mv.quantity*1000))/1000.0 quantity,COALESCE(mv.balance_before_milli,ROUND(mv.balance_before*1000))/1000.0 balanceBefore,COALESCE(mv.balance_after_milli,ROUND(mv.balance_after*1000))/1000.0 balanceAfter,mv.order_id_snapshot orderId,mv.reference,mv.note,mv.user_name userName,mv.created_at createdAt,COALESCE(mv.effective_date,SUBSTR(mv.created_at,1,10)) effectiveDate,im.id materialId,im.code materialCode,im.name materialName,im.color,im.unit FROM inventory_movements mv JOIN inventory_materials im ON im.id=mv.material_id ${where} ORDER BY mv.id DESC LIMIT ?`).all(...params,limit);return sendJson(res,200,{ok:true,movements:rows});
    }
    const invOrder=url.pathname.match(/^\/api\/inventory\/orders\/([^/]+)$/);
    if(req.method==='GET'&&invOrder){
      if(!hasPermission(a.user,'inventory.issue')&&!hasPermission(a.user,'inventory.view'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xem định mức xuất vải.'});const orderId=safeDecodeURIComponent(invOrder[1]);const preview=inventoryOrderPreview(orderId);if(!preview)return sendJson(res,404,{ok:false,message:'Không tìm thấy Order.'});return sendJson(res,200,{ok:true,preview});
    }
    const invIssue=url.pathname.match(/^\/api\/inventory\/orders\/([^/]+)\/issue$/);
    if(req.method==='POST'&&invIssue){
      if(!hasPermission(a.user,'inventory.issue'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xuất vải theo Order.'});const orderId=safeDecodeURIComponent(invIssue[1]),b=await readJson(req);const result=issueOrderMaterials(orderId,a.user,{source:String(b.source||'SCANNER').toUpperCase(),stationCode:cleanText(b.stationCode||'WARE-01',100,'Station')});return sendJson(res,200,{ok:true,...result});
    }
    const invReturn=url.pathname.match(/^\/api\/inventory\/orders\/([^/]+)\/return$/);
    if(req.method==='POST'&&invReturn){
      if(!hasPermission(a.user,'inventory.adjust'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền hoàn vải.'});const orderId=safeDecodeURIComponent(invReturn[1]),b=await readJson(req);const result=returnOrderMaterial(orderId,a.user,b);return sendJson(res,200,{ok:true,...result});
    }

    if(req.method==='POST'&&url.pathname==='/api/inventory/import/preview'){
      if(!hasPermission(a.user,'inventory.import'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền nhập cấu hình vải từ Sheets.'});
      const b=await readJson(req);let text=String(b.text||'');if(String(b.sheetUrl||'').trim())text=await fetchSheetText(b.sheetUrl);const parsed=normalizeInventoryImportRows(text),rows=inventoryImportPreviewRows(parsed.rows);return sendJson(res,200,{ok:true,headers:parsed.headers,rows,summary:{total:rows.length,ready:rows.filter(x=>x.status==='READY').length,errors:rows.filter(x=>x.status==='ERROR').length,missingMaterials:rows.filter(x=>x.status==='READY'&&!x.materialExists).length}});
    }
    if(req.method==='POST'&&url.pathname==='/api/inventory/import/commit'){
      if(!hasPermission(a.user,'inventory.import'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền nhập cấu hình vải từ Sheets.'});
      const b=await readJson(req),rows=Array.isArray(b.rows)?b.rows.map((r,i)=>{if(!r||typeof r!=='object')throw badRequest('Dòng nhập không hợp lệ.');return {...r,rowNumber:r.rowNumber||i+2}}):[];
      if(!rows.length)return sendJson(res,400,{ok:false,error:'INVENTORY_IMPORT_EMPTY',message:'Không có dữ liệu Kho vải để import.'});
      const duplicate=importBatchRepo.findCompleted('INVENTORY_MAPPING',rows);
      if(duplicate&&!b.allowDuplicate)return sendJson(res,409,{ok:false,error:'IMPORT_DUPLICATE_BATCH',message:`Bộ dữ liệu này đã được import ở Batch #${duplicate.id}. Nếu thực sự muốn chạy lại, hãy xác nhận cho phép import trùng.`,details:duplicate});
      const batch=importBatchRepo.create({type:'INVENTORY_MAPPING',user:a.user,rows,mode:'COMMIT',meta:{createMissingMaterials:b.createMissingMaterials!==false}});
      try{
        const result=commitInventoryImport(rows,a.user,{createMissingMaterials:b.createMissingMaterials!==false});
        for(const row of rows)importBatchRepo.markRow(batch.batchId,row.rowNumber,{status:'UPDATED'});
        importBatchRepo.finalize(batch.batchId,{status:'COMPLETED',updated:result.updated,errorCount:0,message:`${result.updated} SKU · ${result.orders} Order · tạo ${result.createdMaterials} mã vải`});
        return sendJson(res,200,{ok:true,batchId:batch.batchId,...result});
      }catch(e){importBatchRepo.finalize(batch.batchId,{status:'FAILED',errorCount:1,message:e.message||'Import failed'});throw e;}
    }
    if(req.method==='GET'&&url.pathname==='/api/inventory/overview'){
      const canInventory=['inventory.view','inventory.issue','inventory.receive','inventory.manage','inventory.adjust'].some(p=>hasPermission(a.user,p));
      if(!canInventory)return sendJson(res,403,{ok:false,message:'Bạn không có quyền xem kho vải.'});
      const mats=materialCatalogRows(true),movements=db.prepare(`SELECT mv.id,mv.movement_type movementType,COALESCE(mv.quantity_milli,ROUND(mv.quantity*1000))/1000.0 quantity,COALESCE(mv.balance_after_milli,ROUND(mv.balance_after*1000))/1000.0 balanceAfter,mv.order_id_snapshot orderId,mv.reference,mv.note,mv.user_name userName,mv.created_at createdAt,COALESCE(mv.effective_date,SUBSTR(mv.created_at,1,10)) effectiveDate,im.code materialCode,im.name materialName,im.unit FROM inventory_movements mv JOIN inventory_materials im ON im.id=mv.material_id ORDER BY mv.id DESC LIMIT 40`).all();
      return sendJson(res,200,{ok:true,summary:{materials:mats.length,lowStock:mats.filter(x=>Number(x.currentStock)<=Number(x.minStock)).length,outOfStock:mats.filter(x=>Number(x.currentStock)<=1e-9).length,healthyStock:mats.filter(x=>Number(x.currentStock)>Number(x.minStock)).length},materials:mats,movements});
    }

    if(req.method==='GET'&&url.pathname==='/api/stations'){
      const rows=db.prepare(`SELECT code,name,type FROM stations WHERE active=1 ORDER BY id`).all().filter(st=>roleCanStation(a.user,st.type)); return sendJson(res,200,{ok:true,stations:rows});
    }
    if(req.method==='GET'&&url.pathname==='/api/dashboard'){
      if(!hasPermission(a.user,'dashboard.view'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xem Dashboard.'});
      const d=dashboardForUser(a.user);
      return sendJson(res,200,{ok:true,summary:d.summary,attention:d.attention,upcoming:d.upcoming,overview:d.overview,stageCounts:d.stageCounts});
    }
    if(req.method==='GET'&&url.pathname==='/api/customers'){
      if(!hasPermission(a.user,'customers.view'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xem khách hàng.'});
      const q=String(url.searchParams.get('q')||'').trim(); const like=`%${q}%`;
      const rows=db.prepare(`SELECT COALESCE(NULLIF(TRIM(customer),''),'(Chưa có tên)') customer,COUNT(*) totalOrders,MAX(updated_at) lastUpdated,SUM(CASE WHEN current_stage='SHIPPED' THEN 1 ELSE 0 END) shipped,SUM(CASE WHEN current_stage!='SHIPPED' THEN 1 ELSE 0 END) active FROM orders WHERE deleted_at IS NULL AND (?='' OR customer LIKE ?) GROUP BY COALESCE(NULLIF(TRIM(customer),''),'(Chưa có tên)') ORDER BY lastUpdated DESC LIMIT 500`).all(q,like);
      return sendJson(res,200,{ok:true,customers:rows});
    }
    if(req.method==='GET'&&url.pathname==='/api/operations/queue'){
      if(!hasPermission(a.user,'dashboard.view'))throw forbidden('Không có quyền xem việc cần xử lý.');
      return sendJson(res,200,{ok:true,...operationsService.queue(a.user)});
    }
    if(url.pathname==='/api/sales/snapshots'&&['GET','POST'].includes(req.method)){
      if(!hasPermission(a.user,'sales.view'))throw forbidden('Không có quyền xem bản chốt doanh số.');
      const b=req.method==='POST'?await readJson(req):Object.fromEntries(url.searchParams);
      const month=String(b.month||''),currency=String(b.currency||'USD').toUpperCase();
      if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(month)||!SALES_CURRENCIES.includes(currency))throw badRequest('Tháng/tiền tệ không hợp lệ.');
      if(req.method==='GET')return sendJson(res,200,{ok:true,snapshots:monthSnapshots.list(month,currency)});
      if(!hasPermission(a.user,'sales.close'))throw forbidden('Cần quyền sales.close để lưu bản chốt.');
      return sendJson(res,201,{ok:true,snapshot:monthSnapshots.save(month,currency,b.expectedHash,b.reason,a.user)});
    }
    const snapshotCsvMatch=url.pathname.match(/^\/api\/sales\/snapshots\/(\d+)\.csv$/);
    if(req.method==='GET'&&snapshotCsvMatch){
      if(!hasPermission(a.user,'sales.view'))throw forbidden('Không có quyền xuất bản chốt.');
      const snap=monthSnapshots.get(Number(snapshotCsvMatch[1])),report=snap.report,rows=[['Sale Date','Order ID','Customer','SKU','Currency','Amount']];
      for(const o of report.orders)rows.push([o.saleDate,o.orderId,o.customer,o.skus,report.currency,o.amountCents===null?'':(o.amountCents/100).toFixed(2)]);
      securityHeaders(res);res.statusCode=200;res.setHeader('Cache-Control','no-store');res.setHeader('Content-Type','text/csv; charset=utf-8');res.setHeader('Content-Disposition','attachment; filename="3alora-snapshot-'+snap.id+'.csv"');return res.end('\uFEFF'+rows.map(row=>row.map(csvSafe).join(',')).join('\r\n'));
    }
    const snapshotMatch=url.pathname.match(/^\/api\/sales\/snapshots\/(\d+)$/);
    if(req.method==='GET'&&snapshotMatch){
      if(!hasPermission(a.user,'sales.view'))throw forbidden('Không có quyền xem bản chốt doanh số.');
      return sendJson(res,200,{ok:true,snapshot:monthSnapshots.get(Number(snapshotMatch[1]))});
    }
    // Sales data is separate from operational reports; server-side RBAC is mandatory.
    if(req.method==='POST'&&url.pathname==='/api/sales/import-preview'){
      if(!hasPermission(a.user,'sales.manage')||!hasPermission(a.user,'sales.view'))return sendJson(res,403,{ok:false,message:'Không có quyền nhập bảng giá.'});
      const b=await readJson(req),rawText=String(b.rawText||'');const r=parseSalesSheet(rawText);
      return sendJson(res,200,{ok:true,rows:r.rows,errors:r.errors,ready:r.rows.length,invalid:r.errors.length,previewHash:salesSheetPreviewHash(rawText,r)});
    }
    if(req.method==='POST'&&url.pathname==='/api/sales/import'){
      if(!hasPermission(a.user,'sales.manage')||!hasPermission(a.user,'sales.view'))return sendJson(res,403,{ok:false,message:'Không có quyền nhập bảng giá.'});
      const b=await readJson(req),rawText=String(b.rawText||'');
      if(String(b.previewHash||'')!==salesSheetPreviewHash(rawText))return sendJson(res,409,{ok:false,message:'Dữ liệu đã thay đổi sau Preview. Hãy Preview lại trước khi nhập.'});
      const r=parseSalesSheet(rawText);
      if(r.errors.length)return sendJson(res,400,{ok:false,message:'Bảng có dòng sai; cần sửa tất cả dòng trước khi Import.',errors:r.errors});
      const result=commitSalesSheet(rawText,a.user);return sendJson(res,200,{ok:true,...result});
    }
    if(req.method==='GET'&&url.pathname==='/api/sales/monthly.csv'){
      if(!hasPermission(a.user,'sales.view'))return sendJson(res,403,{ok:false,message:'Không có quyền xuất báo cáo doanh số.'});
      const ym=String(url.searchParams.get('month')||vietnamToday().slice(0,7)),currency=String(url.searchParams.get('currency')||'USD').toUpperCase();
      if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(ym)||!SALES_CURRENCIES.includes(currency))return sendJson(res,400,{ok:false,message:'Tháng/loại tiền không hợp lệ.'});
      const data=salesMonthlyReport(ym,currency),rows=[['Sale Date','Order ID','Customer','Currency','Amount','Amount Source','Priced Lines','Total Lines']];
      for(const o of data.orders)rows.push([o.saleDate,o.orderId,o.customer||'',currency,o.amountCents===null?'':(o.amountCents/100).toFixed(2),o.amountSource,o.pricedItemCount,o.itemCount]);
      securityHeaders(res);res.statusCode=200;res.setHeader('Cache-Control','no-store');res.setHeader('Content-Type','text/csv; charset=utf-8');res.setHeader('Content-Disposition',`attachment; filename="3alora-sales-${ym}-${currency}.csv"`);return res.end('\uFEFF'+rows.map(row=>row.map(csvSafe).join(',')).join('\r\n'));
    }
    if(req.method==='GET'&&url.pathname==='/api/sales/monthly'){
      if(!hasPermission(a.user,'sales.view'))return sendJson(res,403,{ok:false,message:'Bạn không được cấp quyền xem doanh số.'});
      const ym=String(url.searchParams.get('month')||vietnamToday().slice(0,7));
      if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(ym))return sendJson(res,400,{ok:false,message:'Tháng phải có dạng YYYY-MM.'});
      const currency=String(url.searchParams.get('currency')||'USD').toUpperCase();
      if(!SALES_CURRENCIES.includes(currency))return sendJson(res,400,{ok:false,message:'Đơn vị tiền không hợp lệ.'});
      const report=salesMonthlyReport(ym,currency),history=monthSnapshots.list(ym,currency),fingerprint=monthSnapshots.fingerprint(report);return sendJson(res,200,{ok:true,...report,monthFingerprint:fingerprint,snapshot:history[0]?{...history[0],changed:history[0].contentHash!==fingerprint}:null});
    }
    if(req.method==='GET'&&url.pathname==='/api/sales/orders'){
      if(!hasPermission(a.user,'sales.view'))return sendJson(res,403,{ok:false,message:'Không có quyền xem giá đơn.'});
      const q=cleanText(url.searchParams.get('q')||'',150,'Tìm đơn');
      if(!q)return sendJson(res,200,{ok:true,orders:[]});
      const rows=db.prepare(`SELECT o.id,o.order_id orderId,o.customer,o.created_at createdAt,f.sale_date saleDate,COALESCE(f.currency,'USD') currency,f.order_total_cents orderTotalCents FROM orders o LEFT JOIN order_finance f ON f.order_db_id=o.id WHERE o.deleted_at IS NULL AND (o.order_id LIKE ? OR o.customer LIKE ?) ORDER BY o.id DESC LIMIT 25`).all('%'+q+'%','%'+q+'%');
      return sendJson(res,200,{ok:true,orders:rows});
    }
    let saleMatch=url.pathname.match(/^\/api\/sales\/orders\/([^/]+)$/);
    if(saleMatch&&req.method==='GET'){
      if(!hasPermission(a.user,'sales.view'))return sendJson(res,403,{ok:false,message:'Không có quyền xem giá đơn.'});
      const o=findSaleOrder(safeDecodeURIComponent(saleMatch[1]));if(!o)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});
      return sendJson(res,200,{ok:true,order:salesPricingDetails(o)});
    }
    if(saleMatch&&req.method==='PUT'){
      if(!hasPermission(a.user,'sales.manage')||!hasPermission(a.user,'sales.view'))return sendJson(res,403,{ok:false,message:'Không có quyền cập nhật giá đơn.'});
      const o=findSaleOrder(safeDecodeURIComponent(saleMatch[1]));if(!o)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});
      const b=await readJson(req),currency=String(b.currency||'USD').toUpperCase();
      if(b.pricingVersion!==undefined&&b.pricingVersion!==salesPricingDetails(o).pricingVersion)return sendJson(res,409,{ok:false,error:'SALES_PRICE_CONFLICT',message:'Giá hoặc SKU vừa thay đổi. Tải lại trước khi lưu.'});
      if(!SALES_CURRENCIES.includes(currency))return sendJson(res,400,{ok:false,message:'Đơn vị tiền không hợp lệ.'});
      const date=String(b.saleDate||'').trim();if(date&&!validSaleDate(date))return sendJson(res,400,{ok:false,message:'Ngày bán phải có dạng YYYY-MM-DD hợp lệ.'});
      const previousCurrency=db.prepare('SELECT currency,order_total_cents orderTotal FROM order_finance WHERE order_db_id=?').get(o.id);
      const existingPriced=db.prepare('SELECT COUNT(*) c FROM order_items oi JOIN order_item_prices ip ON ip.order_item_id=oi.id WHERE oi.order_db_id=? AND ip.unit_price_cents IS NOT NULL').get(o.id).c;
      if(previousCurrency&&previousCurrency.currency!==currency&&(previousCurrency.orderTotal!==null||existingPriced>0)&&b.confirmCurrencyChange!==true)return sendJson(res,409,{ok:false,error:'SALES_CURRENCY_CHANGE_CONFIRM',message:'Đơn đã có giá bằng loại tiền khác. Đổi loại tiền không tự quy đổi số tiền. Hãy xác nhận riêng và nhập lại giá đúng theo loại tiền mới.'});
      let total;try{total=priceToCents(b.orderTotal,'Tổng giá trị đơn')}catch(e){return sendJson(res,400,{ok:false,message:e.message});}
      const rawItems=Array.isArray(b.items)?b.items:[];
      const validItems=db.prepare('SELECT id FROM order_items WHERE order_db_id=? AND active=1').all(o.id);
      if(rawItems.length!==validItems.length||new Set(rawItems.map(x=>Number(x.id))).size!==validItems.length||validItems.some(x=>!rawItems.some(y=>Number(y.id)===Number(x.id))))return sendJson(res,409,{ok:false,message:'Danh sách SKU đã thay đổi. Hãy tải lại trước khi cập nhật.'});
      let prices;try{prices=rawItems.map(x=>({id:Number(x.id),cents:priceToCents(x.unitPrice,'Giá SKU')}));}catch(e){return sendJson(res,400,{ok:false,message:e.message});}
      for(const p of prices){const qty=db.prepare('SELECT qty FROM order_items WHERE id=?').get(p.id).qty;if(p.cents!==null&&(!Number.isSafeInteger(p.cents*qty)||p.cents*qty>100_000_000_000_000))throw badRequest('Giá trị dòng vượt giới hạn an toàn.','PRICE_RANGE');}
      const saved=withTransaction(()=>{
        const now=nowIso();
        db.prepare(`INSERT INTO order_finance(order_db_id,sale_date,currency,order_total_cents,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(order_db_id) DO UPDATE SET sale_date=excluded.sale_date,currency=excluded.currency,order_total_cents=excluded.order_total_cents,updated_at=excluded.updated_at`).run(o.id,date||null,currency,total,now);
        for(const item of prices)db.prepare('INSERT INTO order_item_prices(order_item_id,unit_price_cents) VALUES(?,?) ON CONFLICT(order_item_id) DO UPDATE SET unit_price_cents=excluded.unit_price_cents').run(item.id,item.cents);
        audit({orderId:o.order_id,orderDbId:o.id,action:'SALES_PRICING_UPDATED',user:a.user,detail:`Currency ${currency}; sale date ${date||'(created_at)'}; amount set=${total!==null}; item prices set=${prices.filter(p=>p.cents!==null).length}/${prices.length}`});
        return salesPricingDetails(o);
      });return sendJson(res,200,{ok:true,order:saved});
    }
    if(req.method==='GET'&&url.pathname==='/api/reports'){
      if(!hasPermission(a.user,'reports.view'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xem báo cáo.'});
      const total=db.prepare(`SELECT COUNT(*) c FROM orders WHERE deleted_at IS NULL`).get().c;
      const priorityRows=db.prepare(`SELECT priority,COUNT(*) c FROM orders WHERE deleted_at IS NULL GROUP BY priority`).all();
      const stageRows=db.prepare(`SELECT current_stage stage,COUNT(*) c FROM orders WHERE deleted_at IS NULL GROUP BY current_stage`).all();
      const topProducts=db.prepare(`SELECT oi.product_name productName,SUM(oi.qty) qty,COUNT(DISTINCT oi.order_db_id) orders FROM order_items oi JOIN orders o ON o.id=oi.order_db_id WHERE o.deleted_at IS NULL AND oi.active=1 GROUP BY oi.product_name ORDER BY qty DESC LIMIT 10`).all();
      const qc=db.prepare(`SELECT q.result,COUNT(*) c FROM qc_checks q JOIN orders o ON o.id=q.order_db_id WHERE o.deleted_at IS NULL GROUP BY q.result`).all();
      return sendJson(res,200,{ok:true,total,priorities:Object.fromEntries(priorityRows.map(x=>[x.priority,x.c])),stages:Object.fromEntries(stageRows.map(x=>[x.stage,x.c])),topProducts,qc:Object.fromEntries(qc.map(x=>[x.result,x.c]))});
    }
    if(req.method==='GET'&&url.pathname==='/api/orders/export.csv'){
      if(!hasPermission(a.user,'orders.export'))return sendJson(res,403,{ok:false,message:'Không có quyền xuất dữ liệu.'});
      const vis=hasPermission(a.user,'orders.view_all')?{sql:'1=1',params:[]}:visibleOrderClause(a.user,'o');
      const rows=db.prepare(`SELECT o.order_id orderId,o.customer,o.need_by needBy,o.priority,o.current_stage currentStage,o.tracking_number tracking,u.full_name worker,o.notes,(SELECT GROUP_CONCAT(COALESCE(oi.sku,'')||' - '||oi.product_name||' x'||oi.qty,' | ') FROM order_items oi WHERE oi.order_db_id=o.id AND oi.active=1) items FROM orders o LEFT JOIN users u ON u.id=o.assigned_worker_id WHERE o.deleted_at IS NULL AND ${vis.sql} ORDER BY o.id DESC`).all(...vis.params);
      const csvCell=csvSafe; const lines=[['Order ID','Customer','Need By','Priority','Stage','Tracking','Worker','Items','Notes'].map(csvCell).join(',')]; for(const r of rows)lines.push([r.orderId,r.customer,r.needBy,r.priority,r.currentStage,r.tracking,r.worker,r.items,r.notes].map(csvCell).join(','));
      securityHeaders(res); res.statusCode=200; res.setHeader('Cache-Control','no-store'); res.setHeader('Content-Type','text/csv; charset=utf-8'); res.setHeader('Content-Disposition','attachment; filename="3alora-orders.csv"'); return res.end('\uFEFF'+lines.join('\r\n'));
    }

    if(req.method==='GET'&&url.pathname==='/api/orders'){
      const q=cleanText(url.searchParams.get('q')||'',200,'Tìm kiếm');
      const stage=String(url.searchParams.get('stage')||'').trim().toUpperCase();
      const priority=String(url.searchParams.get('priority')||'').trim().toUpperCase();
      const deleted=url.searchParams.get('deleted')==='1';
      const due=String(url.searchParams.get('due')||'');if(due&&!['overdue','soon'].includes(due))throw badRequest('Bộ lọc hạn cần hàng không hợp lệ.');
      const page=Math.max(1,Number.parseInt(url.searchParams.get('page')||'1',10)||1);
      const pageSize=Math.min(100,Math.max(10,Number.parseInt(url.searchParams.get('pageSize')||'50',10)||50));
      if(stage && !STAGE_ORDER.includes(stage)&&!['PRODUCTION','QC','PACKING','SHIPPING'].includes(stage))return sendJson(res,400,{ok:false,message:'Stage không hợp lệ.'});
      if(priority && !['NORMAL','HIGH','EXPRESS'].includes(priority))return sendJson(res,400,{ok:false,message:'Priority không hợp lệ.'});
      if(deleted&&!hasPermission(a.user,'orders.view_deleted'))return sendJson(res,403,{ok:false,message:'Không có quyền xem thùng rác.'});
      const vis=hasPermission(a.user,'orders.view_all')?{sql:'1=1',params:[]}:visibleOrderClause(a.user,'o');
      let where=`${deleted?'o.deleted_at IS NOT NULL':'o.deleted_at IS NULL'} AND ${vis.sql}`, params=[...vis.params];
      if(q){ const like=`%${q}%`; const key=normalizeOrderId(q); where+=` AND (o.order_id LIKE ? OR o.order_key=? OR o.customer LIKE ? OR o.tracking_number LIKE ? OR EXISTS(SELECT 1 FROM order_items oi WHERE oi.order_db_id=o.id AND oi.active=1 AND (oi.product_name LIKE ? OR oi.sku LIKE ?)))`; params.push(like,key,like,like,like,like); }
      if(stage){const group={NEW:['NEW','READY_FOR_PRODUCTION','ASSIGNED'],PRODUCTION:['IN_PRODUCTION','QC_FAILED'],QC:['WAITING_QC','QC_PASSED'],PACKING:['PACKED','TRACKING_CREATED'],SHIPPING:['SHIPPED']}[stage];if(group){where+=` AND o.current_stage IN (${group.map(()=>'?').join(',')})`;params.push(...group)}else {where+=' AND o.current_stage=?';params.push(stage)}}
      if(priority){where+=' AND o.priority=?';params.push(priority)}
      if(due){where+=" AND o.current_stage!='SHIPPED' AND COALESCE(o.need_by_iso,'')<>''";const today=vietnamToday();if(due==='overdue'){where+=' AND o.need_by_iso<?';params.push(today)}else{const end=new Date(today+'T00:00:00Z');end.setUTCDate(end.getUTCDate()+5);where+=' AND o.need_by_iso>=? AND o.need_by_iso<=?';params.push(today,end.toISOString().slice(0,10))}}
      const total=Number(db.prepare(`SELECT COUNT(*) c FROM orders o WHERE ${where}`).get(...params).c||0);
      const rows=db.prepare(`SELECT o.order_id orderId,o.version,o.customer,o.need_by needBy,o.priority,o.current_stage currentStage,o.tracking_number trackingNumber,o.updated_at updatedAt,o.deleted_at deletedAt,o.deleted_reason deletedReason,u.full_name assignedWorker,(SELECT product_name FROM order_items oi WHERE oi.order_db_id=o.id AND oi.active=1 ORDER BY oi.id LIMIT 1) productName,(SELECT COUNT(*) FROM order_items oi WHERE oi.order_db_id=o.id AND oi.active=1) itemCount,(SELECT COALESCE(SUM(qty),0) FROM order_items oi WHERE oi.order_db_id=o.id AND oi.active=1) totalQty FROM orders o LEFT JOIN users u ON u.id=o.assigned_worker_id WHERE ${where} ORDER BY CASE WHEN ?=1 THEN o.id END DESC,CASE WHEN COALESCE(o.need_by_iso,'')='' THEN 1 ELSE 0 END,o.need_by_iso ASC,CASE o.priority WHEN 'EXPRESS' THEN 0 WHEN 'HIGH' THEN 1 ELSE 2 END,o.id DESC LIMIT ? OFFSET ?`).all(...params,deleted?1:0,pageSize,(page-1)*pageSize);
      return sendJson(res,200,{ok:true,orders:rows,pagination:{page,pageSize,total,totalPages:Math.max(1,Math.ceil(total/pageSize))}});
    }
    if(req.method==='POST'&&url.pathname==='/api/orders'){
      if(!hasPermission(a.user,'orders.create'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền tạo đơn.'});
      const b=await readJson(req), orderId=normalizeOrderId(b.orderId), idErr=validateOrderId(orderId); if(idErr)return sendJson(res,400,{ok:false,message:idErr});
      const orderKey=normalizeOrderId(orderId); if(db.prepare('SELECT id,deleted_at FROM orders WHERE order_key=?').get(orderKey))return sendJson(res,409,{ok:false,message:'Order ID đã tồn tại (kể cả trong Thùng rác).'});
      const priority=String(b.priority||'NORMAL').toUpperCase(); if(!['NORMAL','HIGH','EXPRESS'].includes(priority))return sendJson(res,400,{ok:false,message:'Priority không hợp lệ.'});
      let needBy=''; try{needBy=strictNeedBy(b.needBy)}catch(e){return sendJson(res,400,{ok:false,message:e.message})}
      let items=Array.isArray(b.items)?b.items:[]; if(!items.length)items=[{sku:b.sku,productName:b.productName,qty:b.qty,material:b.material,personalization:b.personalization}];
      try{ items=validateItemsLength(items).filter(x=>String(x?.productName||'').trim()||String(x?.sku||'').trim()).map(cleanItem); if(!items.length)items=[cleanItem({productName:'Sản phẩm',qty:1})]; validateInventoryItemReferences(items); }catch(e){ return sendJson(res,e.status||400,{ok:false,error:e.code||'VALIDATION',message:e.message}); }
      const customer=(()=>{try{return cleanText(b.customer||'',200,'Khách hàng')}catch(e){throw e}})();
      const tracking=cleanText(b.trackingNumber||'',200,'Tracking')||null, notes=cleanText(b.notes||'',5000,'Ghi chú');
      const requestedStage=String(b.currentStage||'NEW').toUpperCase(); const initialStage=(hasPermission(a.user,'orders.move_stage')&&STAGE_ORDER.includes(requestedStage))?requestedStage:'NEW';
      let created;
      try{ created=withTransaction(()=>{
        const t=nowIso(); const r=db.prepare(`INSERT INTO orders(order_id,order_key,customer,need_by,need_by_iso,priority,current_stage,stage_updated_at,tracking_number,notes,version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,1,?,?)`).run(orderId,orderKey,customer,needBy,needByIso(needBy),priority,initialStage,t,tracking,notes,t,t);
        const ins=db.prepare(`INSERT INTO order_items(order_db_id,sku,product_name,qty,material,personalization,item_status,current_stage,inventory_material_id,material_usage_per_unit,material_usage_milli) VALUES(?,?,?,?,?,?,?,?,?,?,?)`);
        for(const it of items)ins.run(r.lastInsertRowid,it.sku,it.productName,it.qty,it.material,it.personalization,it.itemStatus,it.itemStatus,it.inventoryMaterialId,it.materialUsagePerUnit,toMilliUnits(it.materialUsagePerUnit||0));
        audit({orderId,orderDbId:r.lastInsertRowid,action:'ORDER_CREATED',user:a.user,detail:`Created from web · ${items.length} item(s)`});
        return orderObject(orderId);
      }); }catch(e){ if(String(e.message).includes('UNIQUE'))return sendJson(res,409,{ok:false,message:'Order ID đã tồn tại.'}); throw e; }
      try{chatMobileService.autoAssignOrder(db.prepare('SELECT id FROM orders WHERE order_key=?').get(orderKey).id)}catch(e){console.error('[CHAT_ROUTING] After create:',e.message)}
      return sendJson(res,201,{ok:true,order:created});
    }

    let m=url.pathname.match(/^\/api\/orders\/([^/]+)$/);
    if(req.method==='GET'&&m){
      const orderId=safeDecodeURIComponent(m[1]);
      const canViewAll=hasPermission(a.user,'orders.view_all');
      const canViewDeleted=hasPermission(a.user,'orders.view_deleted');
      if(!canViewAll&&!userCanViewOrder(a.user,orderId))return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn hoặc bạn không có quyền xem đơn này.'});
      const o=orderObject(orderId,canViewDeleted);
      // Internal note timeline is a read-only projection over order-related chat.
      // It is never copied into orders.notes and is not returned by partner APIs.
      if(o&&!o.deletedAt&&a.user.role!=='PARTNER')o.chatHistory=db.prepare(`
        SELECT m.id,m.author_name authorName,m.body,m.created_at createdAt,m.recalled_at recalledAt,
               t.audience,p.name partnerName,t.id threadId
        FROM chat_messages m JOIN chat_threads t ON t.id=m.thread_id
        LEFT JOIN chat_partner_orgs p ON p.id=t.partner_org_id
        WHERE t.order_db_id=? ORDER BY m.id DESC LIMIT 80`).all(o.id).map(row=>({
          ...row,body:row.recalledAt?'[Tin nhắn đã thu hồi]':row.body
        }));
      if(!o||(!canViewDeleted&&o.deletedAt))return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});
      if(!hasPermission(a.user,'audit.view'))o.timeline=[];
      return sendJson(res,200,{ok:true,order:o});
    }
    if(req.method==='PATCH'&&m){
      if(!hasPermission(a.user,'orders.edit'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền sửa đơn.'});
      const oldId=safeDecodeURIComponent(m[1]), oldKey=normalizeOrderId(oldId); const old=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NULL ORDER BY CASE WHEN order_id=? THEN 0 ELSE 1 END LIMIT 1').get(oldId,oldKey,oldId); if(!old)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});
      const b=await readJson(req); if(Number(b.version)!==Number(old.version||1))return sendJson(res,409,{ok:false,error:'EDIT_CONFLICT',message:'Đơn đã được người khác cập nhật. Hãy tải lại trước khi lưu.'});
      const newId=normalizeOrderId(b.orderId||old.order_id), idErr=validateOrderId(newId); if(idErr)return sendJson(res,400,{ok:false,message:idErr}); const newKey=normalizeOrderId(newId);
      const conflict=db.prepare('SELECT id FROM orders WHERE order_key=? AND id<>?').get(newKey,old.id); if(conflict)return sendJson(res,409,{ok:false,message:'Order ID mới đã tồn tại.'});
      const priority=String(b.priority||old.priority||'NORMAL').toUpperCase(); if(!['NORMAL','HIGH','EXPRESS'].includes(priority))return sendJson(res,400,{ok:false,message:'Priority không hợp lệ.'});
      let needBy; try{needBy=strictNeedBy(b.needBy??old.need_by)}catch(e){return sendJson(res,400,{ok:false,message:e.message})}
      let items=null; if(Array.isArray(b.items)){ try{items=validateItemsLength(b.items).filter(x=>String(x?.productName||'').trim()||String(x?.sku||'').trim()).map(cleanItem);if(!items.length)items=[cleanItem({productName:'Sản phẩm',qty:1})];validateInventoryItemReferences(items)}catch(e){return sendJson(res,e.status||400,{ok:false,error:e.code||'VALIDATION',message:e.message})} }
      let after;
      try{after=withTransaction(()=>{
        const before=orderObject(old.order_id,true),t=nowIso();
        const info=db.prepare(`UPDATE orders SET order_id=?,order_key=?,customer=?,need_by=?,need_by_iso=?,priority=?,tracking_number=?,notes=?,version=version+1,updated_at=? WHERE id=? AND version=?`).run(newId,newKey,cleanText(b.customer??old.customer??'',200,'Khách hàng'),needBy,needByIso(needBy),priority,cleanText(b.trackingNumber??old.tracking_number??'',200,'Tracking')||null,cleanText(b.notes??old.notes??'',5000,'Ghi chú'),t,old.id,old.version||1);
        if(info.changes!==1)throw new Error('EDIT_CONFLICT');
        if(items){ syncOrderItems(old.id,items); }
        after=orderObject(newId,true); const changes=[];
        for(const [label,key] of [['Order ID','orderId'],['Khách','customer'],['Ngày cần','needBy'],['Ưu tiên','priority'],['Tracking','trackingNumber'],['Ghi chú','notes']])if(String(before?.[key]??'')!==String(after?.[key]??''))changes.push(`${label}: ${before?.[key]??'-'} → ${after?.[key]??'-'}`);
        if(items&&JSON.stringify(before?.items||[])!==JSON.stringify(after?.items||[]))changes.push('Đã cập nhật danh sách sản phẩm/SKU');
        audit({orderId:newId,orderDbId:old.id,action:'ORDER_EDITED',user:a.user,detail:changes.join(' | ')||'Order updated'}); return after;
      });}catch(e){if(e.message==='EDIT_CONFLICT')return sendJson(res,409,{ok:false,error:'EDIT_CONFLICT',message:'Đơn vừa được người khác cập nhật. Hãy tải lại.'});throw e;}
      return sendJson(res,200,{ok:true,order:after});
    }
    if(req.method==='DELETE'&&m){
      if(!hasPermission(a.user,'orders.delete'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xóa đơn.'});
      const orderId=safeDecodeURIComponent(m[1]),key=normalizeOrderId(orderId); const o=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NULL LIMIT 1').get(orderId,key); if(!o)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'}); const b=await readJson(req);
      db.prepare('UPDATE orders SET deleted_at=?,deleted_by=?,deleted_reason=?,version=version+1,updated_at=? WHERE id=?').run(nowIso(),a.user.id,String(b.reason||'Xóa theo yêu cầu'),nowIso(),o.id); audit({orderId,action:'ORDER_DELETED',user:a.user,detail:String(b.reason||'Xóa theo yêu cầu')}); return sendJson(res,200,{ok:true});
    }
    const restoreMatch=url.pathname.match(/^\/api\/orders\/([^/]+)\/restore$/);
    if(req.method==='POST'&&restoreMatch){ if(!hasPermission(a.user,'orders.restore'))return sendJson(res,403,{ok:false,message:'Không có quyền khôi phục.'}); const orderId=safeDecodeURIComponent(restoreMatch[1]),key=normalizeOrderId(orderId); const o=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NOT NULL LIMIT 1').get(orderId,key); if(!o)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn trong thùng rác.'}); db.prepare('UPDATE orders SET deleted_at=NULL,deleted_by=NULL,deleted_reason=NULL,version=version+1,updated_at=? WHERE id=?').run(nowIso(),o.id); audit({orderId,action:'ORDER_RESTORED',user:a.user}); return sendJson(res,200,{ok:true,order:orderObject(orderId)}); }

    const moveMatch=url.pathname.match(/^\/api\/orders\/([^/]+)\/move-stage$/);
    if(req.method==='POST'&&moveMatch){
      if(!hasPermission(a.user,'orders.move_stage')&&!hasPermission(a.user,'orders.move_early'))return sendJson(res,403,{ok:false,message:'Không có quyền chuyển công đoạn.'});
      const body=await readJson(req);
      const orderId=safeDecodeURIComponent(moveMatch[1]),key=normalizeOrderId(orderId); const o=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NULL LIMIT 1').get(orderId,key); if(!o)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});checkOrderVersion(o,body);
      const b=body; const to=String(b.toStage||'').toUpperCase(); const reason=String(b.reason||'').trim(); if(!isValidStage(to))return sendJson(res,400,{ok:false,error:'STAGE_INVALID',message:'Công đoạn không hợp lệ.'});
      if(!hasPermission(a.user,'orders.move_stage')&&hasPermission(a.user,'orders.move_early')&&(!isEarlyStage(o.current_stage)||!isEarlyStage(to)))return sendJson(res,403,{ok:false,message:'Quyền chuyển công đoạn đầu chỉ áp dụng khi cả trạng thái hiện tại và trạng thái đích đều thuộc Đơn mới / Sẵn sàng sản xuất / Đã phân công.'});
      if(to===o.current_stage)return sendJson(res,200,{ok:true,order:orderObject(orderId),unchanged:true}); if(!reason)return sendJson(res,400,{ok:false,message:'Cần ghi lý do chuyển công đoạn để lưu lịch sử.'});
      {const t=nowIso();db.prepare('UPDATE orders SET current_stage=?,stage_updated_at=?,version=version+1,updated_at=? WHERE id=?').run(to,t,t,o.id);} syncItemWorkflowStage(o.id,to); audit({orderId,action:'STAGE_OVERRIDE',fromStage:o.current_stage,toStage:to,user:a.user,detail:reason}); return sendJson(res,200,{ok:true,order:orderObject(orderId)});
    }
    const duplicateMatch=url.pathname.match(/^\/api\/orders\/([^/]+)\/duplicate$/);
    if(req.method==='POST'&&duplicateMatch){
      if(!hasPermission(a.user,'orders.duplicate'))return sendJson(res,403,{ok:false,message:'Không có quyền nhân bản đơn.'});
      const sourceId=safeDecodeURIComponent(duplicateMatch[1]); const source=orderObject(sourceId,false); if(!source)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn gốc.'});
      const b=await readJson(req); const newId=normalizeOrderId(b.newOrderId); const idErr=validateOrderId(newId); if(idErr)return sendJson(res,400,{ok:false,message:idErr}); if(db.prepare('SELECT id FROM orders WHERE order_key=?').get(normalizeOrderId(newId)))return sendJson(res,409,{ok:false,message:'Order ID mới đã tồn tại.'});
      const created=withTransaction(()=>{ const t=nowIso(); const r=db.prepare(`INSERT INTO orders(order_id,order_key,customer,need_by,need_by_iso,priority,current_stage,stage_updated_at,tracking_number,assigned_worker_id,notes,version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,1,?,?)`).run(newId,normalizeOrderId(newId),source.customer||'',source.needBy||'',needByIso(source.needBy||''),source.priority||'NORMAL','NEW',t,null,null,source.notes||'',t,t); const ins=db.prepare(`INSERT INTO order_items(order_db_id,sku,product_name,qty,material,personalization,item_status,current_stage,inventory_material_id,material_usage_per_unit,material_usage_milli) VALUES(?,?,?,?,?,?,?,?,?,?,?)`); for(const it of source.items)ins.run(r.lastInsertRowid,it.sku||'',it.productName||'Sản phẩm',cleanQty(it.qty||1),it.material||'',it.personalization||'','PENDING','PENDING',it.inventoryMaterialId||null,Number(it.materialUsagePerUnit||0),toMilliUnits(Number(it.materialUsagePerUnit||0))); audit({orderId:newId,orderDbId:r.lastInsertRowid,action:'ORDER_DUPLICATED',user:a.user,detail:`Copied from #${sourceId}`}); return orderObject(newId); }); return sendJson(res,201,{ok:true,order:created});
    }
    const permanentMatch=url.pathname.match(/^\/api\/orders\/([^/]+)\/permanent$/);
    if(req.method==='DELETE'&&permanentMatch){
      if(!hasPermission(a.user,'orders.permanent_delete'))return sendJson(res,403,{ok:false,message:'Không có quyền xóa vĩnh viễn.'});
      const orderId=safeDecodeURIComponent(permanentMatch[1]),key=normalizeOrderId(orderId); const o=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NOT NULL LIMIT 1').get(orderId,key); if(!o)return sendJson(res,404,{ok:false,message:'Đơn phải nằm trong Thùng rác trước khi xóa vĩnh viễn.'}); const b=await readJson(req); if(normalizeOrderId(b.confirmOrderId||'')!==normalizeOrderId(o.order_id))return sendJson(res,400,{ok:false,message:'Xác nhận Order ID không khớp.'});
      audit({orderId:o.order_id,orderDbId:o.id,action:'ORDER_PERMANENT_DELETE',user:a.user,detail:`Permanent delete · reason=${o.deleted_reason||''}`}); db.prepare('DELETE FROM orders WHERE id=?').run(o.id); try{fs.rmSync(path.join(EVIDENCE_DIR,String(o.id)),{recursive:true,force:true})}catch(e){structuredLog('warn','evidence_cleanup_failed',{orderDbId:o.id,message:e.message})} return sendJson(res,200,{ok:true});
    }
    const qcMatch=url.pathname.match(/^\/api\/orders\/([^/]+)\/qc-check$/);
    if(req.method==='POST'&&qcMatch){
      if(!hasPermission(a.user,'qc.execute'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền ghi phiếu Kiểm Tra.'});
      const body=await readJson(req);
      const orderId=safeDecodeURIComponent(qcMatch[1]),key=normalizeOrderId(orderId); const o=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NULL LIMIT 1').get(orderId,key); if(!o)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});checkOrderVersion(o,body);
      if(!userCanViewOrder(a.user,o.order_id)&&!hasPermission(a.user,'orders.view_all'))return sendJson(res,403,{ok:false,message:'Order không thuộc phạm vi Kiểm Tra của bạn.'});
      if(o.current_stage!=='WAITING_QC')return sendJson(res,409,{ok:false,error:'QC_STAGE_INVALID',message:'Chỉ đơn đang Chờ Kiểm Tra mới được tạo phiếu Kiểm Tra. Nếu đơn đã FAIL, Production phải làm lại và chuyển về Chờ Kiểm Tra trước.'});
      const b=body,result=String(b.result||'').toUpperCase(); if(!['PASS','FAIL'].includes(result))return sendJson(res,400,{ok:false,message:'Kết quả Kiểm Tra không hợp lệ.'});
      const checklist=b.checklist&&typeof b.checklist==='object'&&!Array.isArray(b.checklist)?b.checklist:{};
      const required=['size','color','material','qty','personalization','sewing','clean','packing']; const normalized=Object.fromEntries(required.map(k=>[k,checklist[k]===true]));
      const itemRows=db.prepare('SELECT id,sku,product_name productName,qty,material FROM order_items WHERE order_db_id=? AND active=1 ORDER BY id').all(o.id); if(!itemRows.length)return sendJson(res,409,{ok:false,message:'Order không có sản phẩm đang hoạt động để Kiểm Tra.'});
      const itemResults=b.itemResults&&typeof b.itemResults==='object'&&!Array.isArray(b.itemResults)?b.itemResults:{};
      let notes;try{notes=cleanText(b.notes||'',2000,'Ghi chú Kiểm Tra')}catch(e){return sendJson(res,400,{ok:false,message:e.message})}
      if(result==='PASS'&&(required.some(k=>normalized[k]!==true)||itemRows.some(it=>itemResults[String(it.id)]!==true)))return sendJson(res,400,{ok:false,message:'Kiểm Tra PASS yêu cầu xác nhận đầy đủ checklist và từng sản phẩm trong đơn.'});
      if(result==='FAIL'&&!notes)return sendJson(res,400,{ok:false,message:'Kiểm Tra FAIL bắt buộc mô tả lỗi.'});
      if(result==='FAIL'&&itemRows.every(it=>itemResults[String(it.id)]===true))return sendJson(res,400,{ok:false,message:'Kiểm Tra FAIL phải đánh dấu ít nhất một sản phẩm chưa đạt.'});
      const defect=result==='FAIL'?String(b.defectCategory||'OTHER').toUpperCase():'OTHER'; if(result==='FAIL'&&!DEFECT_CATEGORIES.has(defect))return sendJson(res,400,{ok:false,message:'Nhóm lỗi Kiểm Tra không hợp lệ.'});
      const to=result==='PASS'?'QC_PASSED':'QC_FAILED';let prep=null,qcId=null;
      try{
        if(b.evidence?.dataUrl)prep=prepareEvidence(b.evidence,o,a.user,result==='PASS'?'QC_PASS':'QC_FAIL');
        withTransaction(()=>{
          const qcRun=db.prepare('INSERT INTO qc_checks(order_db_id,result,checklist_json,notes,checked_by,checked_by_name,defect_category,created_at) VALUES(?,?,?,?,?,?,?,?)').run(o.id,result,JSON.stringify({...normalized,itemResults}),notes,a.user.id,a.user.full_name,result==='FAIL'?defect:null,nowIso());qcId=Number(qcRun.lastInsertRowid);
          const qci=db.prepare('INSERT INTO qc_item_results(qc_check_id,order_item_id,result,sku_snapshot,product_name_snapshot,qty_snapshot,material_snapshot,created_at) VALUES(?,?,?,?,?,?,?,?)');for(const it of itemRows)qci.run(qcId,it.id,itemResults[String(it.id)]===true?'PASS':'FAIL',it.sku||'',it.productName||'',it.qty||1,it.material||'',nowIso());
          if(prep)commitPreparedEvidence(prep,o,a.user,{qcCheckId:qcId});
          {const stageAt=nowIso();db.prepare('UPDATE orders SET current_stage=?,stage_updated_at=?,version=version+1,updated_at=? WHERE id=?').run(to,stageAt,stageAt,o.id);}
          db.prepare('UPDATE order_items SET current_stage=?,item_status=? WHERE order_db_id=? AND active=1').run(result==='PASS'?'DONE':'REWORK',result==='PASS'?'DONE':'REWORK',o.id);
          audit({orderId:o.order_id,orderDbId:o.id,action:`QC_${result}_CHECKLIST`,fromStage:o.current_stage,toStage:to,user:a.user,stationCode:cleanText(b.stationCode||'',100,'Station'),detail:`${notes||'Đã lưu phiếu Kiểm Tra'}${prep?` · evidenceId linked`:''}`});
        });
      }catch(e){cleanupPreparedEvidence(prep);throw e}
      return sendJson(res,200,{ok:true,qcCheckId:qcId,order:orderObject(o.order_id)});
    }
    const packingMatch=url.pathname.match(/^\/api\/orders\/([^/]+)\/packing-check$/);
    if(req.method==='POST'&&packingMatch){
      if(!hasPermission(a.user,'station.packing'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền đóng gói.'});
      const body=await readJson(req);
      const orderId=safeDecodeURIComponent(packingMatch[1]),key=normalizeOrderId(orderId);const o=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NULL LIMIT 1').get(orderId,key);if(!o)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});checkOrderVersion(o,body);
      if(!userCanViewOrder(a.user,o.order_id)&&!hasPermission(a.user,'orders.view_all'))return sendJson(res,403,{ok:false,message:'Order không thuộc phạm vi đóng gói của bạn.'});
      if(o.current_stage!=='QC_PASSED')return sendJson(res,409,{ok:false,message:'Chỉ đơn Kiểm Tra đạt mới được đóng gói.'});
      const b=body,checklist=b.checklist&&typeof b.checklist==='object'&&!Array.isArray(b.checklist)?b.checklist:{};
      const required=['qty','personalization','accessories','labelAddress','finalVisual'];const normalized=Object.fromEntries(required.map(k=>[k,checklist[k]===true]));if(required.some(k=>normalized[k]!==true))return sendJson(res,400,{ok:false,message:'Cần xác nhận đầy đủ checklist đóng gói.'});
      const activeItems=db.prepare('SELECT id FROM order_items WHERE order_db_id=? AND active=1 ORDER BY id').all(o.id),itemResults=b.itemResults&&typeof b.itemResults==='object'&&!Array.isArray(b.itemResults)?b.itemResults:{};if(activeItems.some(it=>itemResults[String(it.id)]!==true))return sendJson(res,400,{ok:false,message:'Cần xác nhận từng sản phẩm trước khi đóng gói.'});
      let notes;try{notes=cleanText(b.notes||'',2000,'Ghi chú đóng gói')}catch(e){return sendJson(res,400,{ok:false,message:e.message})}
      let packingMeasurements=null;
      if(b.packingMetrics!==undefined){
        try{packingMeasurements=computeParcel(b.packingMetrics,5000)}
        catch(err){return sendJson(res,422,{ok:false,error:'INVALID_PACKING_METRICS',message:err.message})}
      }
      let prep=null,packingId=null;
      try{
        if(b.evidence?.dataUrl)prep=prepareEvidence(b.evidence,o,a.user,'PACKING');
        withTransaction(()=>{const r=db.prepare('INSERT INTO packing_checks(order_db_id,checklist_json,notes,checked_by,checked_by_name,created_at) VALUES(?,?,?,?,?,?)').run(o.id,JSON.stringify({...normalized,itemResults}),notes,a.user.id,a.user.full_name,nowIso());packingId=Number(r.lastInsertRowid);
          if(packingMeasurements){const m=packingMeasurements;db.prepare('INSERT INTO order_packing_metrics(order_db_id,packing_check_id,actual_grams,volume_cm3,volumetric_grams,billable_grams,length_mm,width_mm,height_mm,divisor_cm3_per_kg,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)').run(o.id,packingId,m.actualGrams,m.volumeCm3,m.volumetricGrams,m.billableGrams,Math.round(m.dimensionsCm.length*10),Math.round(m.dimensionsCm.width*10),Math.round(m.dimensionsCm.height*10),m.divisorCm3PerKg,a.user.id,nowIso());}
          if(prep)commitPreparedEvidence(prep,o,a.user,{packingCheckId:packingId});{const stageAt=nowIso();db.prepare('UPDATE orders SET current_stage=?,stage_updated_at=?,version=version+1,updated_at=? WHERE id=?').run('PACKED',stageAt,stageAt,o.id);}syncItemWorkflowStage(o.id,'PACKED');audit({orderId:o.order_id,orderDbId:o.id,action:'PACKING_CHECKLIST_PASS',fromStage:o.current_stage,toStage:'PACKED',user:a.user,stationCode:cleanText(b.stationCode||'',100,'Station'),detail:`${notes||'Đã hoàn tất checklist đóng gói'}${prep?' · evidence linked':''}`});});
      }catch(e){cleanupPreparedEvidence(prep);throw e}
      return sendJson(res,200,{ok:true,packingCheckId:packingId,order:orderObject(o.order_id)});
    }
    // Single scanner endpoint: the current order stage chooses the next action.
    // Existing station-specific APIs remain for backward compatibility and detailed Kiểm Tra/packing.
    if(req.method==='POST'&&url.pathname==='/api/workflow/quick-scan'){const body=await readJson(req);if(db.prepare('SELECT 1 FROM alora_order_links l JOIN orders o ON o.id=l.order_db_id WHERE o.order_id=? OR o.order_key=?').get(String(body.orderId||''),normalizeOrderId(body.orderId||'')))return sendJson(res,409,{ok:false,error:'ENTERPRISE_ORDER_REQUIRED',message:'Quét đơn Enterprise tại Workspace → Quét quy trình.'});return sendJson(res,200,quickScanService.scan(body,a.user));}
    m=url.pathname.match(/^\/api\/orders\/([^/]+)\/scan$/);
    if(req.method==='POST'&&m){ const b=await readJson(req);const orderId=safeDecodeURIComponent(m[1]); const station=db.prepare('SELECT * FROM stations WHERE code=? AND active=1').get(String(b.stationCode||'')); if(!station)return sendJson(res,400,{ok:false,message:'Station không hợp lệ.'}); if(!roleCanStation(a.user,station.type))return sendJson(res,403,{ok:false,message:'Tài khoản không có quyền dùng station này.'}); if(!userCanViewOrder(a.user,orderId))return sendJson(res,404,{ok:false,message:'Không tìm thấy Order hoặc Order không thuộc công đoạn của bạn.'}); const result=applyScan(orderId,a.user,station,b.source); if(!result.ok)return sendJson(res,404,{ok:false,message:'Không tìm thấy Order.'}); return sendJson(res,200,result); }
    m=url.pathname.match(/^\/api\/orders\/([^/]+)\/action$/);
    if(req.method==='POST'&&m){
      const orderId=safeDecodeURIComponent(m[1]),key=normalizeOrderId(orderId); const b=await readJson(req); const action=String(b.action||'').toUpperCase(); if(!userCanViewOrder(a.user,orderId))return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn hoặc bạn không có quyền thao tác đơn này.'}); const o=db.prepare('SELECT * FROM orders WHERE (order_id=? OR order_key=?) AND deleted_at IS NULL LIMIT 1').get(orderId,key); if(!o)return sendJson(res,404,{ok:false,message:'Không tìm thấy đơn.'});
      if(action==='QC_FAIL'){ return sendJson(res,400,{ok:false,message:'Kiểm Tra FAIL phải thực hiện qua Phiếu Kiểm Tra có checklist.'}); }
      if(action==='SET_TRACKING'){
        if(!hasPermission(a.user,'tracking.update'))return sendJson(res,403,{ok:false,message:'Không có quyền cập nhật Tracking.'}); let tracking; try{tracking=cleanText(b.tracking||'',200,'Tracking')}catch(e){return sendJson(res,400,{ok:false,message:e.message})} if(!tracking)return sendJson(res,400,{ok:false,message:'Tracking trống.'}); if(!['PACKED','TRACKING_CREATED'].includes(o.current_stage))return sendJson(res,400,{ok:false,message:`Không thể tạo Tracking ở trạng thái ${o.current_stage}`}); const from=o.current_stage; {const stageAt=nowIso();db.prepare('UPDATE orders SET tracking_number=?,current_stage=?,stage_updated_at=?,version=version+1,updated_at=? WHERE id=?').run(tracking,'TRACKING_CREATED',stageAt,stageAt,o.id);} syncItemWorkflowStage(o.id,'TRACKING_CREATED'); audit({orderId,action:'SET_TRACKING',fromStage:from,toStage:'TRACKING_CREATED',user:a.user,stationCode:String(b.stationCode||''),detail:tracking}); return sendJson(res,200,{ok:true,order:orderObject(orderId)});
      }
      if(action==='ASSIGN'){
        if(!hasPermission(a.user,'orders.assign'))return sendJson(res,403,{ok:false,message:'Không có quyền phân công.'}); const worker=db.prepare("SELECT * FROM users WHERE id=? AND active=1 AND role='PRODUCTION'").get(Number(b.workerId)); if(!worker)return sendJson(res,400,{ok:false,message:'Chỉ có thể phân công cho tài khoản Production đang hoạt động.'}); const from=o.current_stage; const stage=['NEW','READY_FOR_PRODUCTION','ASSIGNED'].includes(from)?'ASSIGNED':from; {const t=nowIso();db.prepare('UPDATE orders SET assigned_worker_id=?,current_stage=?,stage_updated_at=CASE WHEN current_stage<>? THEN ? ELSE stage_updated_at END,version=version+1,updated_at=? WHERE id=?').run(worker.id,stage,stage,t,t,o.id);} syncItemWorkflowStage(o.id,stage); audit({orderId,action:'ASSIGN',fromStage:from,toStage:stage,user:a.user,detail:`Assigned to ${worker.full_name}`}); return sendJson(res,200,{ok:true,order:orderObject(orderId)});
      }
      return sendJson(res,400,{ok:false,message:'Action không hợp lệ.'});
    }

    if(req.method==='GET'&&url.pathname==='/api/production-workers'){
      if(!hasPermission(a.user,'orders.assign'))return sendJson(res,403,{ok:false,message:'Không có quyền xem nhân viên sản xuất.'});
      const rows=db.prepare(`SELECT id,username,full_name fullName FROM users WHERE active=1 AND deleted_at IS NULL AND role='PRODUCTION' ORDER BY full_name`).all();
      return sendJson(res,200,{ok:true,workers:rows});
    }

    if(req.method==='POST'&&url.pathname==='/api/import/preview'){
      if(!hasPermission(a.user,'import.manage'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền nhập dữ liệu.'});
      const b=await readJson(req);
      let text='';
      if(String(b.sheetUrl||'').trim()){
        try{ text=await fetchSheetText(b.sheetUrl); }
        catch(e){ return sendJson(res,400,{ok:false,message:e.message||'Link Google Sheets không hợp lệ.'}); }
      }else text=String(b.rawText||'');
      if(!text.trim())return sendJson(res,400,{ok:false,message:'Chưa có dữ liệu để đọc.'});
      const parsed=normalizeImportedRows(text),valid=[],errors=[];
      for(const row of parsed.rows){try{validateImportedRow(row);valid.push(row)}catch(e){errors.push({rowNumber:row.rowNumber,orderId:row.orderId,message:e.message})}}
      const expectedVersions={};for(const row of valid){const key=normalizeOrderId(row.orderId);expectedVersions[key]=db.prepare('SELECT version FROM orders WHERE order_key=?').get(key)?.version||0;}
      return sendJson(res,200,{ok:true,headers:parsed.headers,rows:valid,expectedVersions,total:valid.length,invalid:errors.length,errors});
    }
    if(req.method==='POST'&&url.pathname==='/api/import/commit'){
      if(!hasPermission(a.user,'import.manage'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền nhập dữ liệu.'});
      const b=await readJson(req);
      const mode=String(b.mode||'SKIP').toUpperCase()==='UPDATE'?'UPDATE':'SKIP';
      const rows=Array.isArray(b.rows)?b.rows.map((r,i)=>{if(!r||typeof r!=='object')throw badRequest('Dòng nhập không hợp lệ.');return {...r,rowNumber:r.rowNumber||i+2}}):[];
      if(!rows.length)return sendJson(res,400,{ok:false,error:'IMPORT_EMPTY',message:'Không có dòng nào để import.'});
      if(rows.length>2000)return sendJson(res,400,{ok:false,error:'IMPORT_TOO_LARGE',message:'Mỗi lần chỉ import tối đa 2000 dòng.'});
      const duplicate=importBatchRepo.findCompleted('ORDERS',rows);
      if(duplicate&&!b.allowDuplicate)return sendJson(res,409,{ok:false,error:'IMPORT_DUPLICATE_BATCH',message:`Bộ dữ liệu này đã được import ở Batch #${duplicate.id}. Nếu thực sự muốn chạy lại, hãy xác nhận cho phép import trùng.`,details:duplicate});
      // A preview is a promise about the versions that the operator reviewed.
      // Keep the token outside business rows so the batch fingerprint is stable.
      if(b.expectedVersions!==undefined){
        if(!b.expectedVersions||typeof b.expectedVersions!=='object'||Array.isArray(b.expectedVersions))throw badRequest('Phiên bản xem trước không hợp lệ.');
        for(const key of new Set(rows.map(r=>normalizeOrderId(r.orderId)))){
          const expected=b.expectedVersions[key];if(!Number.isSafeInteger(expected)||expected<0)throw badRequest('Thiếu phiên bản xem trước; hãy Preview lại.');
          const actual=db.prepare('SELECT version FROM orders WHERE order_key=?').get(key)?.version||0;
          if(actual!==expected)throw conflict(`Đơn #${key} đã đổi sau Preview; hãy xem trước lại.`,'IMPORT_PREVIEW_STALE');
        }
      }
      const batch=importBatchRepo.create({type:'ORDERS',user:a.user,rows,mode,meta:{source:'SHEETS_OR_PASTE'}});
      const result={created:0,updated:0,skipped:0,errors:[],details:[]};
      const keys=[...new Set(rows.map(r=>normalizeOrderId(r.orderId)).filter(Boolean))],preexistingKeys=new Set();
      for(const k of keys){if(db.prepare('SELECT 1 FROM orders WHERE order_key=?').get(k))preexistingKeys.add(k)}
      const sourceLines=new Set();for(const row of rows){validateImportedRow(row);const lineKey=normalizeOrderId(row.orderId)+'|'+String(row.sku||row.productName||'').trim().toUpperCase();if(sourceLines.has(lineKey))throw badRequest('Trùng Order + SKU/tên trong bảng; gộp số lượng hoặc cấp SKU riêng trước khi nhập.','IMPORT_AMBIGUOUS_ITEM');sourceLines.add(lineKey)}
      const importContext={preexistingKeys,createdKeys:new Set()};
      db.exec('BEGIN');
      let currentRow=null;
      try{
        for(const row of rows){
          currentRow=row;
          validateImportedRow(row);
          const rr=upsertImportedOrder(row,mode,a.user,importContext);
          result.details.push({...rr,rowNumber:row.rowNumber});
          if(rr.status==='CREATED')result.created++; else if(rr.status==='UPDATED')result.updated++; else result.skipped++;
        }
        db.exec('COMMIT');
      }catch(e){
        try{db.exec('ROLLBACK')}catch{}
        importBatchRepo.finalize(batch.batchId,{status:'FAILED',errorCount:1,message:`Row ${currentRow?.rowNumber||'?'}: ${e.message}`});
        if(currentRow)importBatchRepo.markRow(batch.batchId,currentRow.rowNumber,{status:'ERROR',message:e.message});
        return sendJson(res,400,{ok:false,error:'IMPORT_ROLLED_BACK',message:'Import đã được rollback toàn bộ do có dòng lỗi.',details:{rowNumber:currentRow?.rowNumber||null,reason:e.message,batchId:batch.batchId}});
      }
      for(const d of result.details)importBatchRepo.markRow(batch.batchId,d.rowNumber,{status:d.status,message:d.message||''});
      importBatchRepo.finalize(batch.batchId,{status:'COMPLETED',created:result.created,updated:result.updated,skipped:result.skipped,errorCount:0,message:`created=${result.created};updated=${result.updated};skipped=${result.skipped}`});
      audit({action:'BULK_IMPORT',user:a.user,entityType:'IMPORT_BATCH',entityId:String(batch.batchId),detail:`batch=${batch.batchId};created=${result.created};updated=${result.updated};skipped=${result.skipped}`});
      return sendJson(res,200,{ok:true,batchId:batch.batchId,...result});
    }

    if(req.method==='GET'&&url.pathname==='/api/import/batches'){
      if(!hasPermission(a.user,'import.manage')&&!hasPermission(a.user,'inventory.import'))return sendJson(res,403,{ok:false,message:'Bạn không có quyền xem lịch sử import.'});
      return sendJson(res,200,{ok:true,batches:importBatchRepo.recent(Number(url.searchParams.get('limit')||50))});
    }

    if(req.method==='GET'&&url.pathname==='/api/users'){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được quản lý tài khoản.'});
      const rows=db.prepare(`SELECT * FROM users WHERE deleted_at IS NULL ORDER BY id`).all();
      return sendJson(res,200,{ok:true,users:rows.map(u=>safeUser(u))});
    }
    if(req.method==='GET'&&url.pathname==='/api/permissions'){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được xem cấu hình phân quyền.'});
      return sendJson(res,200,{ok:true,catalog:PERMISSION_CATALOG,roleDefaults:ROLE_PERMISSIONS,roles:Object.keys(ROLE_PERMISSIONS),adminOnly:[...ADMIN_ONLY_PERMISSIONS]});
    }
    if(req.method==='POST'&&url.pathname==='/api/users'){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được tạo tài khoản.'}); const b=await readJson(req); const role=String(b.role||'').toUpperCase(); if(!ROLE_STATIONS[role])return sendJson(res,400,{ok:false,message:'Role không hợp lệ.'}); if(role==='ADMIN'&&a.user.role!=='ADMIN')return sendJson(res,403,{ok:false,message:'Chỉ Admin được tạo tài khoản Admin.'}); const err=validatePassword(b.password); if(err)return sendJson(res,400,{ok:false,message:err}); try{ const id=createUser({username:String(b.username||'').trim(),fullName:String(b.fullName||'').trim(),password:String(b.password||''),role,department:String(b.department||''),defaultStationCode:String(b.defaultStationCode||'')||null,mustChange:1,permissionOverrides:b.permissionOverrides||{}}); audit({action:'USER_CREATED',user:a.user,detail:`userId=${id}`}); return sendJson(res,201,{ok:true}); }catch(e){ return sendJson(res,400,{ok:false,message:e.message.includes('UNIQUE')?'Tài khoản đã tồn tại.':e.message}); }
    }
    m=url.pathname.match(/^\/api\/users\/(\d+)$/);
    if(req.method==='PATCH'&&m){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được quản lý tài khoản.'});
      const id=Number(m[1]); const target=getUserById(id); if(!target)return sendJson(res,404,{ok:false,message:'Không tìm thấy user.'});
      if(target.role==='PARTNER')return sendJson(res,403,{ok:false,message:'Tài khoản đối tác chỉ quản lý trong phần 3ALORA CHAT; không được chuyển thành tài khoản nội bộ.'});
      const b=await readJson(req); const role=String(b.role||target.role).toUpperCase();
      if(!ROLE_PERMISSIONS[role])return sendJson(res,400,{ok:false,message:'Role không hợp lệ.'});
      if((target.role==='ADMIN'||role==='ADMIN')&&a.user.role!=='ADMIN')return sendJson(res,403,{ok:false,message:'Chỉ Admin được chỉnh tài khoản Admin hoặc cấp role Admin.'});
      if(target.role==='ADMIN'&&role!=='ADMIN'){
        const admins=db.prepare(`SELECT COUNT(*) c FROM users WHERE active=1 AND deleted_at IS NULL AND role='ADMIN'`).get().c;
        if(admins<=1)return sendJson(res,400,{ok:false,message:'Phải giữ lại ít nhất 1 Admin đang hoạt động.'});
      }
      const fullName=String(b.fullName??target.full_name).trim(),department=String(b.department??target.department??'').trim();const ve=validateFullName(fullName)||validateDepartment(department);if(ve)return sendJson(res,400,{ok:false,message:ve});
      const defaultStationCode=String(b.defaultStationCode??target.default_station_code??'').trim()||null;
      const raw=b.permissionOverrides&&typeof b.permissionOverrides==='object'&&!Array.isArray(b.permissionOverrides)?b.permissionOverrides:parsePermissionOverrides(target);
      const clean=sanitizePermissionOverrides(raw,PERMISSION_CATALOG,ADMIN_ONLY_PERMISSIONS);
      db.prepare(`UPDATE users SET full_name=?,role=?,department=?,default_station_code=?,permission_overrides=?,updated_at=? WHERE id=?`).run(fullName,role,department,defaultStationCode,JSON.stringify(clean),nowIso(),id);
      db.prepare('DELETE FROM sessions WHERE user_id=? AND id<>?').run(id,a.user.id===id?a.session.id:-1);db.prepare('DELETE FROM chat_mobile_sessions WHERE user_id=?').run(id);
      audit({action:'USER_PERMISSIONS_UPDATED',user:a.user,detail:`target=${target.username}; role=${target.role}->${role}; overrides=${JSON.stringify(clean)}`});
      return sendJson(res,200,{ok:true,user:safeUser(getUserById(id))});
    }
    m=url.pathname.match(/^\/api\/users\/(\d+)\/reset-password$/);
    if(req.method==='POST'&&m){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được quản lý tài khoản.'});
      const id=Number(m[1]); const u=getUserById(id); if(!u)return sendJson(res,404,{ok:false,message:'Không tìm thấy user.'});
      const b=await readJson(req); const password=String(b.password||''); const err=validatePassword(password);
      if(err)return sendJson(res,400,{ok:false,message:err});
      const salt=makeSalt(), hash=hashPassword(password,salt);
      db.prepare('UPDATE users SET password_hash=?,salt=?,must_change_password=1,password_changed_at=NULL,auth_version=auth_version+1,updated_at=? WHERE id=?').run(hash,salt,nowIso(),id);
      db.prepare('DELETE FROM sessions WHERE user_id=?').run(id);
      audit({action:'USER_PASSWORD_RESET',user:a.user,detail:`target=${u.username}; reset to administrator-defined initial password`});
      return sendJson(res,200,{ok:true,message:'Đã đặt mật khẩu khởi tạo. Nhân viên phải đổi một lần sau khi đăng nhập; mật khẩu mới sau đó sẽ được giữ nguyên.'});
    }
    m=url.pathname.match(/^\/api\/users\/(\d+)\/toggle$/);
    if(req.method==='POST'&&m){
      if(!requireAdminUser(a.user))return sendJson(res,403,{ok:false,message:'Chỉ Admin được quản lý tài khoản.'});
      const id=Number(m[1]); if(id===a.user.id)return sendJson(res,400,{ok:false,message:'Không thể tự vô hiệu hóa tài khoản đang đăng nhập.'});
      const u=getUserById(id); if(!u)return sendJson(res,404,{ok:false,message:'Không tìm thấy user.'});
      if(u.active&&u.role==='ADMIN'){const admins=db.prepare(`SELECT COUNT(*) c FROM users WHERE active=1 AND deleted_at IS NULL AND role='ADMIN'`).get().c;if(admins<=1)return sendJson(res,400,{ok:false,message:'Không thể khóa Admin cuối cùng của hệ thống.'});}
      withTransaction(()=>{db.prepare('UPDATE users SET active=?,auth_version=auth_version+1,updated_at=? WHERE id=?').run(u.active?0:1,nowIso(),id);db.prepare('DELETE FROM sessions WHERE user_id=?').run(id);});
      audit({action:u.active?'USER_DISABLED':'USER_ENABLED',user:a.user,detail:`${u.username}; sessions revoked`}); return sendJson(res,200,{ok:true});
    }
    m=url.pathname.match(/^\/api\/users\/(\d+)$/);
    if(req.method==='DELETE'&&m){
      if(a.user.role!=='ADMIN' || !hasPermission(a.user,'users.delete'))return sendJson(res,403,{ok:false,message:'Chỉ Admin được xóa tài khoản.'});
      const id=Number(m[1]); if(id===a.user.id)return sendJson(res,400,{ok:false,message:'Không thể tự xóa tài khoản Admin đang đăng nhập.'});
      const u=getUserById(id); if(!u)return sendJson(res,404,{ok:false,message:'Không tìm thấy tài khoản.'});
      if(u.role==='ADMIN'){const admins=db.prepare(`SELECT COUNT(*) c FROM users WHERE active=1 AND deleted_at IS NULL AND role='ADMIN'`).get().c;if(admins<=1)return sendJson(res,400,{ok:false,message:'Không thể xóa Admin cuối cùng của hệ thống.'});}
      const b=await readJson(req); if(String(b.confirmUsername||'').trim().toLowerCase()!==String(u.username).toLowerCase())return sendJson(res,400,{ok:false,message:'Tên tài khoản xác nhận không khớp.'});
      withTransaction(()=>{
        const t=nowIso(),tombstone=`deleted_${id}_${sha256(u.username).slice(0,10)}`;
        db.prepare('UPDATE orders SET assigned_worker_id=NULL,version=version+1,updated_at=? WHERE assigned_worker_id=?').run(t,id);
        db.prepare('DELETE FROM sessions WHERE user_id=?').run(id);
        audit({action:'USER_DELETED',user:a.user,detail:`targetUserId=${id}; username=${u.username}; fullName=${u.full_name}; role=${u.role}`});
        db.prepare('UPDATE users SET original_username=COALESCE(original_username,username),username=?,active=0,deleted_at=?,deleted_by=?,auth_version=auth_version+1,permission_overrides=?,updated_at=? WHERE id=?').run(tombstone,t,a.user.id,'{}',t,id);
      });
      return sendJson(res,200,{ok:true,message:'Đã xóa tài khoản an toàn. Session bị thu hồi, Order được bỏ phân công và lịch sử người dùng vẫn được bảo toàn.'});
    }

    return sendJson(res,404,{ok:false,message:'API not found'});
  }catch(e){
    const status=e instanceof AppError?e.status:(e?.code==='SQLITE_CONSTRAINT_UNIQUE'?409:500),code=e instanceof AppError?e.code:(status===409?'DB_CONFLICT':'INTERNAL_ERROR');
    structuredLog(status>=500?'error':'warn','api_error',{requestId:req._requestId||'',method:req.method,url:req.url,userId:(()=>{try{return getSession(req)?.user?.id||null}catch{return null}})(),status,code,message:e.message,stack:status>=500&&!IS_PROD?e.stack:undefined});
    if(status>=500)console.error(e);
    return sendJson(res,status,{ok:false,error:code,message:status>=500?'Server error':e.message,requestId:req._requestId||'',details:e instanceof AppError?e.details:undefined,detail:status>=500&&NODE_ENV==='development'?e.message:undefined});
  }
}

const server=http.createServer((req,res)=>{
  req._requestId=/^[A-Za-z0-9_-]{1,100}$/.test(String(req.headers['x-request-id']||''))?String(req.headers['x-request-id']):crypto.randomUUID(); res.setHeader('X-Request-ID',req._requestId);
  return runWithRequestContext({requestId:req._requestId},()=>{
  const started=Date.now(); res.on('finish',()=>{if(res.statusCode<400&&['POST','PUT','PATCH','DELETE'].includes(req.method))realtime?.publish('refresh'); if(res.statusCode>=400)structuredLog(res.statusCode>=500?'error':'warn','http_request',{requestId:req._requestId,method:req.method,url:req.url,status:res.statusCode,ms:Date.now()-started,ip:clientIp(req)}); });
  let url;try{url=new URL(req.url,`http://${req.headers.host||'localhost'}`)}catch{return sendJson(res,400,{ok:false,error:'BAD_URL',message:'URL không hợp lệ.'})}
  const rateCookie=parseCookies(req);const rateToken=rateCookie['__Host-alora_session']||rateCookie.alora_session;const rateSession=rateToken?db.prepare('SELECT user_id FROM sessions WHERE token_hash=? AND expires_at>?').get(sha256(rateToken),nowIso()):null;const limitKey=rateSession&&!url.pathname.includes('/auth/login')?'user:'+rateSession.user_id:clientIp(req);
  if(url.pathname.startsWith('/api/')&&!url.pathname.startsWith('/api/realtime')&&!requestAllowed(limitKey)) return sendJson(res,429,{ok:false,message:'Quá nhiều yêu cầu. Vui lòng thử lại sau.'});
  if(FORCE_HTTPS&&!requestIsHttps(req)&&!['/healthz','/readyz'].includes(url.pathname)){
    let target;try{target=new URL(PUBLIC_BASE_URL||`https://${url.host}`);if(target.protocol!=='https:')throw Error('HTTPS required')}catch{return sendJson(res,503,{ok:false,error:'PUBLIC_URL_INVALID',message:'Cấu hình PUBLIC_BASE_URL phải là HTTPS hợp lệ.'})}
    securityHeaders(res);res.statusCode=308;res.setHeader('Location',target.origin+url.pathname+url.search);return res.end();
  }
  if(url.pathname==='/robots.txt') return sendText(res,200,'User-agent: *\nDisallow: /\n','text/plain; charset=utf-8');
  if(url.pathname==='/healthz'){
    try{ db.prepare('SELECT 1 ok').get(); return sendJson(res,200,{ok:true,status:'healthy',version:APP_VERSION}); }
    catch{ return sendJson(res,503,{ok:false,status:'unhealthy'}); }
  }
  if(url.pathname==='/readyz'){
    try{const schema=Number(db.prepare('SELECT COALESCE(MAX(version),0) v FROM schema_migrations').get().v||0);if(schema!==SCHEMA_VERSION)return sendJson(res,503,{ok:false,error:'SCHEMA_NOT_READY',message:`Schema ${schema}/${SCHEMA_VERSION}`});if(missingChatSchema().length||missingMobileSchema().length||missingIntakeSchema().length||[...PWA_REQUIRED_TABLES,'enterprise_grants','enterprise_order_meta','enterprise_financials','enterprise_periods','enterprise_estimates','enterprise_rooms','enterprise_messages','enterprise_files','enterprise_source_financials','enterprise_products','enterprise_product_images','enterprise_push_outbox','chat_groups','chat_group_members','chat_group_messages','chat_message_hidden','chat_group_message_hidden','chat_message_audit','chat_user_profiles','chat_attachments'].some(name=>!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name)))return sendJson(res,503,{ok:false,error:'CHAT_SCHEMA_INCOMPLETE',message:'Chat database schema is incomplete.'});db.prepare('SELECT 1 ok').get();return sendJson(res,200,{ok:true,status:'ready',version:APP_VERSION,schemaVersion:schema});}catch(e){return sendJson(res,503,{ok:false,error:'NOT_READY',message:'Database chưa sẵn sàng.'});}
  }
  if(url.pathname.startsWith('/api/')) return api(req,res,url);
  return serveStatic(req,res,url);
  });
});
server.keepAliveTimeout=65_000;
server.headersTimeout=66_000;
server.requestTimeout=30_000;
server.on('error',(err)=>{
  if(err && err.code==='EADDRINUSE'){
    console.error(`[LOI] Cong ${PORT} dang bi chuong trinh khac su dung.`);
    console.error(`Hay dong server 3ALORA cu hoac chuong trinh dang chiem cong ${PORT}, sau do chay lai.`);
    process.exit(98);
  }
  console.error('[LOI] Khong the khoi dong 3ALORA:', err);
  process.exit(1);
});
server.listen(PORT,HOST,()=>{
  console.log(`3 Alora Enterprise ${APP_VERSION} running at http://localhost:${PORT}`);
  console.log(`Environment: ${NODE_ENV}`);
  if(PUBLIC_BASE_URL) console.log(`Public URL: ${PUBLIC_BASE_URL}`);
  console.log(`Persistent root: ${PERSIST_ROOT}`);
  console.log(`Database: ${DB_PATH}`);
  console.log(`Evidence: ${EVIDENCE_DIR}`);
  console.log(`Backups: ${BACKUP_DIR}`);
});

let shuttingDown=false;
function shutdown(signal){
  if(shuttingDown)return;shuttingDown=true;
  console.log(`${signal}: graceful shutdown...`);
  realtime?.close();server.close(async()=>{await createRuntimeBackup('shutdown');try{db.close()}catch{};process.exit(0)});
  setTimeout(()=>process.exit(1),10_000).unref();
}
process.on('SIGTERM',()=>shutdown('SIGTERM'));
process.on('SIGINT',()=>shutdown('SIGINT'));
