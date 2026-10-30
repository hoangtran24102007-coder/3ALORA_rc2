const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { DatabaseSync } = require('node:sqlite');
const { validatePassword } = require('./lib/validators');
const config=require('./lib/config').loadConfiguration(__dirname);
const ROOT=__dirname, DATA=path.join(ROOT,'data'),PERSIST_ROOT=config.persistRoot,DB_PATH=config.dbPath;
console.log('Database: '+DB_PATH);
const supplied=String(process.env.ALORA_ADMIN_RESET_PASSWORD||process.env.ALORA_ADMIN_PASSWORD||'');
const RESET_PASSWORD=supplied || `A!${crypto.randomBytes(18).toString('base64url')}9z`;
function makeSalt(){return crypto.randomBytes(16).toString('hex')}
function hashPassword(password,salt){return crypto.scryptSync(String(password),Buffer.from(salt,'hex'),64).toString('hex')}
function nowIso(){return new Date().toISOString()}
const passwordError=validatePassword(RESET_PASSWORD);
if(passwordError){console.error(`\n[LOI] Mat khau Admin moi khong hop le: ${passwordError}\n`);process.exit(2)}
if(!fs.existsSync(DB_PATH)){console.error(`\n[LOI] Khong tim thay database: ${DB_PATH}\nHay khoi dong 3ALORA it nhat 1 lan truoc khi reset Admin.\n`);process.exit(1)}
const db=new DatabaseSync(DB_PATH);
try{
  const admin=db.prepare("SELECT * FROM users WHERE lower(username)='admin' AND deleted_at IS NULL LIMIT 1").get();
  if(!admin){console.error('\n[LOI] Khong tim thay tai khoan admin.\n');process.exitCode=1}
  else{
    const salt=makeSalt(),hash=hashPassword(RESET_PASSWORD,salt),t=nowIso();
    db.exec('BEGIN IMMEDIATE');
    try{
      db.prepare('UPDATE users SET password_hash=?,salt=?,active=1,must_change_password=1,password_changed_at=NULL,auth_version=COALESCE(auth_version,1)+1,updated_at=? WHERE id=?').run(hash,salt,t,admin.id);
      db.prepare('DELETE FROM sessions WHERE user_id=?').run(admin.id);
      try{db.prepare(`INSERT INTO audit_events(order_id,action,user_id,username,full_name,role,detail,created_at) VALUES(NULL,'LOCAL_ADMIN_PASSWORD_RESET',?,?,?,?,?,?)`).run(admin.id,admin.username,admin.full_name,admin.role,'Local reset; all old sessions revoked; password not stored in audit',t)}catch{}
      db.exec('COMMIT');
    }catch(e){db.exec('ROLLBACK');throw e}
    console.log('\n==========================================');
    console.log('  3 ALORA - RESET ADMIN THANH CONG');
    console.log('==========================================');
    console.log('Tai khoan : admin');
    if(!supplied) console.log(`Mat khau tam thoi: ${RESET_PASSWORD}`);
    else console.log('Mat khau tam thoi: DA DAT THEO MAT KHAU BAN VUA NHAP');
    console.log('Dang nhap xong bat buoc doi mat khau; toan bo phien cu da bi thu hoi.');
    console.log('KHONG xoa Order, QC, Packing, tai khoan nhan vien hay database.');
    console.log('==========================================\n');
  }
}catch(e){console.error('\n[LOI] Reset Admin that bai:',e.message,'\n');process.exitCode=1}
finally{try{db.close()}catch{}}
