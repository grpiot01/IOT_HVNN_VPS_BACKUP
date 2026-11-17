"""Capture deployed VPS code into an independent Git checkout; no live changes.

Run on the VPS: python backup_vps_code.py --prepare
Publish only after GitHub CLI authentication: python backup_vps_code.py --push
"""
import argparse
import ast
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

from dotenv import dotenv_values
from secure_vps_github import github_env

SOURCE = Path('C:/backend').resolve()
DEST = Path('C:/vps-backups/IOT_HVNN_VPS_BACKUP').resolve()
GIT = Path('C:/tools/hvnn-backup/git/cmd/git.exe')
GH = Path('C:/tools/hvnn-backup/gh/bin/gh.exe')
OWNER = 'grpiot01'
REPO = OWNER + '/IOT_HVNN_VPS_BACKUP'
SECRET_KEY = re.compile(r'password|passwd|secret|token|private.?key|api.?key|credential', re.I)
TOKEN_PATTERNS = [re.compile(rb'gh[pousr]_[A-Za-z0-9]{20,}'), re.compile(rb'github_pat_[A-Za-z0-9_]{30,}'), re.compile(rb'-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----'), re.compile(rb'eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}')]


def run(args, **kwargs):
    result = subprocess.run([str(x) for x in args], cwd=DEST, capture_output=True, text=True, encoding='utf-8', errors='replace', env=github_env(), **kwargs)
    if result.returncode:
        # Credential and HTTP debug output must never be copied into logs.
        raise RuntimeError(f'{Path(str(args[0])).name} {args[1]} failed (exit {result.returncode})')
    return result.stdout


def prepare():
    if DEST == SOURCE or SOURCE in DEST.parents or not str(DEST).lower().startswith('c:\\vps-backups\\'):
        raise RuntimeError('Backup destination must be separate from the running app')
    DEST.mkdir(parents=True,exist_ok=True)
    env = dotenv_values(SOURCE/'.env')
    secrets = {key:str(value) for key,value in env.items() if value and SECRET_KEY.search(key) and len(str(value)) >= 4}
    modules = set()
    pending = ['main']
    while pending:
        module = pending.pop()
        path = SOURCE/(module+'.py')
        if module in modules or not path.is_file():
            continue
        modules.add(module)
        tree = ast.parse(path.read_text(encoding='utf-8-sig'))
        for node in ast.walk(tree):
            if isinstance(node,ast.ImportFrom) and node.module:
                pending.append(node.module.split('.')[0])
            elif isinstance(node,ast.Import):
                pending.extend(alias.name.split('.')[0] for alias in node.names)
    files = [SOURCE/(module+'.py') for module in sorted(modules)]
    for path in files:
        for node in ast.walk(ast.parse(path.read_text(encoding='utf-8-sig'))):
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and node.func.attr=='getenv' and len(node.args)>=2:
                key,value=node.args[:2]
                if isinstance(key,ast.Constant) and isinstance(key.value,str) and SECRET_KEY.search(key.value) and isinstance(value,ast.Constant) and isinstance(value.value,str) and len(value.value)>=4 and not value.value.startswith(('CHANGE_ME','REDACTED_')):
                    secrets[key.value+'_SOURCE_DEFAULT']=value.value
    files.extend(SOURCE/name for name in ['requirements.txt','start-backend.bat','run-background.vbs'] if (SOURCE/name).is_file())
    for name in ['templates','static']:
        for path in (SOURCE/name).rglob('*'):
            if path.is_file() and path.suffix.lower() in {'.html','.css','.js','.json','.svg','.webp','.png','.jpg','.jpeg','.ico','.woff','.woff2','.ttf'}:
                if path.name.startswith('.'):
                    continue
                files.append(path)
    records = []
    for source in files:
        if source.is_symlink() or not source.resolve().is_relative_to(SOURCE):
            raise RuntimeError('Source path escapes backend')
        rel = source.relative_to(SOURCE).as_posix()
        raw = source.read_bytes()
        sanitized = raw
        redactions = []
        text_file = source.suffix.lower() in {'.py','.txt','.bat','.vbs','.html','.css','.js','.json','.svg'}
        for key, value in sorted(secrets.items(),key=lambda x:len(x[1]),reverse=True):
            encoded = value.encode()
            replacement = ('REDACTED_'+key+'_RESTORE_FROM_ENV').encode()
            if len(value) >= 8:
                if encoded in sanitized:
                    if not text_file:
                        raise RuntimeError(f'Credential-like content in binary asset: {rel}')
                    sanitized = sanitized.replace(encoded,replacement)
                    redactions.append(key)
            elif text_file:
                for quote in [b'"',b"'"]:
                    literal = quote+encoded+quote
                    if literal in sanitized:
                        sanitized=sanitized.replace(literal,quote+replacement+quote)
                        redactions.append(key)
        if any(pattern.search(sanitized) for pattern in TOKEN_PATTERNS):
            raise RuntimeError(f'Unrecognized credential pattern in {rel}; publication stopped')
        if source.suffix == '.py':
            ast.parse(sanitized.decode('utf-8-sig'))
        target = DEST/'backend'/rel
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(sanitized)
        records.append({'path':'backend/'+rel,'bytes':len(sanitized),'source_sha256':hashlib.sha256(raw).hexdigest(),'sha256':hashlib.sha256(sanitized).hexdigest(),'redacted_keys':sorted(set(redactions))})
    example = '# Fill values from the separate private recovery archive. Never commit .env.\n'
    for key in sorted(env):
        if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',key):
            example += key+'=\n'
    (DEST/'.env.example').write_text(example,encoding='utf-8')
    packages = sorted({dist.metadata['Name']+'=='+dist.version for dist in importlib.metadata.distributions() if dist.metadata['Name']},key=str.lower)
    (DEST/'requirements-vps-lock.txt').write_text('\n'.join(packages)+'\n',encoding='utf-8')
    (DEST/'.gitignore').write_text('.env\n.env.*\n!.env.example\n*.db\n*.sqlite*\n*.db-*\n*.log\n__pycache__/\n*.pyc\n.venv/\nvenv/\nbackend/exports_csv/\nbackend/firmware_uploads/\nbackend/deployment-backups/\nbackend/source-snapshots/\n*.zip\n*.pem\n*.key\n',encoding='utf-8')
    (DEST/'README.md').write_text('''# IOT_HVNN — Bản sao code đang chạy trên VPS

Repo độc lập với repo phát triển trên PC. Nguồn được lấy trực tiếp từ `C:\\backend`, không lấy từ working tree PC.

## Có trong bản sao

- Các module Python mà ứng dụng backend sử dụng, template, tệp khởi động.
- Web đã build trong `backend/static`, gồm các asset hiện có trên VPS.
- Danh sách phiên bản thư viện của môi trường Python đang chạy.
- `.env.example` chỉ có tên biến, không chứa giá trị thật.
- `snapshot-manifest.json`: dấu thời gian, checksum và tên biến đã được loại khỏi source nếu có.

Đây là bản sao triển khai: VPS chỉ có web đã build nên repo không tự bổ sung source React/STM32/ESP32 từ PC. Một số giá trị bí mật nếu xuất hiện trong source được thay bằng placeholder; khôi phục chúng từ kho riêng trước khi chạy lại.

## Giữ ngoài GitHub

`.env`, JWT/broker/Drive/tunnel credentials, database SQLite, dữ liệu CSV, log, bộ đếm nước, các bản firmware đã upload và các gói backup riêng. Repo này không thay thế bản sao lưu đầy đủ dữ liệu và hạ tầng. Không đưa cookie Chrome hoặc thông tin đăng nhập GitHub vào repo.

## Khôi phục code

1. Clone repo vào thư mục mới; dừng/đổi dịch vụ đang chạy chỉ khi đã sẵn sàng triển khai.
2. Cài Python cùng phiên bản chính/phụ ghi trong manifest. Tạo venv và cài `requirements-vps-lock.txt`.
3. Lấy các giá trị cấu hình và database từ bản sao lưu riêng, đặt `.env` cạnh các file Python của backend.
4. Khôi phục các placeholder nêu trong manifest. Chạy `python -m uvicorn main:app --host 127.0.0.1 --port 10000` từ thư mục backend, rồi kiểm tra `/health` và `/`.
5. Thiết lập Scheduled Task `TomatoBackend`, tunnel, MQTT và Drive theo gói khôi phục riêng. Kiểm tra kết nối thiết bị; HTTP 200 không chứng minh thiết bị đã chạy lệnh.

## Cập nhật bản sao

Chạy bằng Python venv trên VPS: `python C:\\vps-backups\\IOT_HVNN_VPS_BACKUP\\tools\\backup_vps_code.py --prepare`, sau đó dùng `--push` để commit/push. Mỗi lần chụp sẽ cập nhật manifest. Chưa thiết lập lịch tự động. Công cụ không sửa `C:\\backend`, không khởi động lại dịch vụ và không gửi lệnh thiết bị.
''',encoding='utf-8')
    (DEST/'tools').mkdir(exist_ok=True)
    helper=DEST/'tools/backup_vps_code.py'
    if Path(__file__).resolve()!=helper.resolve():
        shutil.copy2(__file__,helper)
    security_helper=Path(__file__).with_name('secure_vps_github.py')
    if security_helper.resolve()!=(DEST/'tools/secure_vps_github.py').resolve():
        shutil.copy2(security_helper,DEST/'tools/secure_vps_github.py')
    manifest={'captured_utc':datetime.now(timezone.utc).isoformat(),'source':'C:/backend','python':sys.version.split()[0],'files':records,'excluded':['environment values','database','CSV and logs','water_offsets.json','firmware_uploads','deployment backups','tunnel credentials','Chrome profile'],'secret_scan':'Known environment secrets removed from selected source files; token/private-key patterns rejected before commit.'}
    (DEST/'snapshot-manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'prepared':str(DEST),'files':len(records),'redacted_files':[r['path'] for r in records if r['redacted_keys']],'python':manifest['python']}))


def publish():
    if not (DEST/'snapshot-manifest.json').is_file():
        raise RuntimeError('Prepare the snapshot first')
    account=json.loads(run([GH,'api','user']))
    if account['login'].lower()!=OWNER:
        raise RuntimeError('GitHub CLI account does not match the VPS browser account')
    # Never accept a plaintext fallback from gh auth login.
    config=Path(os.getenv('APPDATA',''))/'GitHub CLI/hosts.yml'
    candidates=[config,Path.home()/'.config/gh/hosts.yml']
    if any(p.exists() and re.search(r'^\s*oauth_token:',p.read_text(encoding='utf-8')) for p in candidates):
        raise RuntimeError('GitHub token was stored in plaintext; move it to Windows Credential Manager before publishing')
    if not (DEST/'.git').exists():
        run([GIT,'init','--initial-branch=main'])
    run([GIT,'config','user.name',account['login']])
    run([GIT,'config','user.email',f"{account['id']}+{account['login']}@users.noreply.github.com"])
    run([GIT,'config','credential.helper',''])
    helper='!'+str(GH).replace('\\','/')+' auth git-credential'
    run([GIT,'config','--add','credential.helper',helper])
    run([GIT,'config','credential.useHttpPath','true'])
    # Re-scan every selected source byte immediately before staging.
    manifest=json.loads((DEST/'snapshot-manifest.json').read_text(encoding='utf-8'))
    env=dotenv_values(SOURCE/'.env')
    secrets=[str(v).encode() for k,v in env.items() if v and SECRET_KEY.search(k) and len(str(v))>=8]
    for path in DEST.rglob('*'):
        if not path.is_file() or '.git' in path.relative_to(DEST).parts:
            continue
        raw=path.read_bytes()
        if any(v in raw for v in secrets) or any(pattern.search(raw) for pattern in TOKEN_PATTERNS):
            raise RuntimeError('Secret scan failed before staging: '+str(path.relative_to(DEST)))
    for item in manifest['files']:
        if hashlib.sha256((DEST/item['path']).read_bytes()).hexdigest()!=item['sha256']:
            raise RuntimeError('Prepared snapshot changed before staging')
    run([GIT,'add','--','backend','tools','.env.example','.gitignore','README.md','requirements-vps-lock.txt','snapshot-manifest.json'])
    staged=run([GIT,'diff','--cached','--name-only']).splitlines()
    for name in staged:
        if any(part in {'.env','.venv','firmware_uploads','exports_csv','deployment-backups','source-snapshots'} for part in Path(name).parts) or name.endswith(('.db','.pem','.key','.zip')):
            raise RuntimeError('Forbidden file in staged snapshot: '+name)
    if staged:
        run([GIT,'commit','-m','Backup deployed VPS backend and web assets'])
    exists=subprocess.run([str(GH),'repo','view',REPO,'--json','nameWithOwner,visibility'],cwd=DEST,capture_output=True,text=True,env=github_env())
    if exists.returncode:
        run([GH,'repo','create',REPO,'--private','--description','Independent backup of the deployed HVNN VPS backend and web; credentials and database excluded'])
    else:
        info=json.loads(exists.stdout)
        if info.get('visibility')!='PRIVATE':
            raise RuntimeError('Refusing to push VPS backup to a non-private repository')
    url='https://github.com/'+REPO+'.git'
    remotes=run([GIT,'remote']).splitlines()
    if 'origin' in remotes:
        if run([GIT,'remote','get-url','origin']).strip()!=url:
            raise RuntimeError('Unexpected existing backup remote; no push attempted')
    else:
        run([GIT,'remote','add','origin',url])
    run([GIT,'push','--set-upstream','origin','main'])
    local=run([GIT,'rev-parse','HEAD']).strip()
    remote=run([GIT,'ls-remote','origin','refs/heads/main']).split()[0]
    if local!=remote:
        raise RuntimeError('Remote commit does not match local snapshot')
    print(json.dumps({'pushed':True,'repository':'https://github.com/'+REPO,'visibility':'private','commit':local,'snapshot_files':len(manifest['files']),'remote_verified':True}))


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--prepare',action='store_true')
    parser.add_argument('--push',action='store_true')
    args=parser.parse_args()
    if args.prepare:
        prepare()
    if args.push:
        publish()
    if not args.prepare and not args.push:
        parser.error('Choose --prepare or --push')
