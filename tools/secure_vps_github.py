"""Protect the VPS GitHub credential with Windows DPAPI outside Git.

Only --migrate reads gh's temporary plaintext fallback. Tokens never appear in
arguments, stdout or source. Child gh/git processes receive an ephemeral env.
"""
import argparse
import csv
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import re
import subprocess

GH = Path('C:/tools/hvnn-backup/gh/bin/gh.exe')
STORE = Path.home()/'AppData/Local/hvnn-backup/github-auth.dpapi'


class Blob(ctypes.Structure):
    _fields_=[('size',wintypes.DWORD),('data',ctypes.POINTER(ctypes.c_byte))]


def protect(raw, decrypt=False, machine=False):
    buffer=ctypes.create_string_buffer(raw)
    source=Blob(len(raw),ctypes.cast(buffer,ctypes.POINTER(ctypes.c_byte)))
    target=Blob()
    flags=1 | (4 if machine and not decrypt else 0)  # UI forbidden; optional local machine scope.
    crypt=ctypes.WinDLL('crypt32',use_last_error=True)
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    if decrypt:
        function=crypt.CryptUnprotectData
    else:
        function=crypt.CryptProtectData
    function.argtypes=[ctypes.POINTER(Blob),ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,wintypes.DWORD,ctypes.POINTER(Blob)]
    function.restype=wintypes.BOOL
    kernel.LocalFree.argtypes=[ctypes.c_void_p]
    if not function(ctypes.byref(source),None,None,None,None,flags,ctypes.byref(target)):
        raise OSError(ctypes.get_last_error(),'Windows DPAPI operation failed')
    try:
        return ctypes.string_at(target.data,target.size)
    finally:
        kernel.LocalFree(target.data)


def github_env():
    if not STORE.is_file():
        return dict(os.environ)
    env=dict(os.environ)
    env['GH_TOKEN']=protect(STORE.read_bytes(),decrypt=True).decode('utf-8')
    env['PATH']='C:/tools/hvnn-backup/git/cmd;'+env.get('PATH','')
    return env


def restrict(path):
    result=subprocess.run(['whoami','/user','/fo','csv','/nh'],capture_output=True,text=True,check=True)
    sid=next(csv.reader(result.stdout.splitlines()))[1]
    result=subprocess.run(['icacls',str(path),'/inheritance:r','/grant:r','*'+sid+':(F)','*S-1-5-18:(F)'],capture_output=True,text=True)
    if result.returncode:
        raise RuntimeError('Could not restrict credential ACL')


def migrate():
    config=Path(os.environ['APPDATA'])/'GitHub CLI/hosts.yml'
    source=config.read_text(encoding='utf-8')
    pattern=re.compile(r'(?m)^[ \t]*oauth_token:[ \t]*[\"\x27]?([^ \t\r\n\"\x27]+)[\"\x27]?[ \t]*\r?\n?')
    tokens=set(pattern.findall(source))
    if not tokens:
        if STORE.exists():
            print(json.dumps({'encrypted_store_exists':True,'plaintext_token_fields':False}))
            return
        raise RuntimeError('No credential available to migrate')
    if len(tokens)!=1:
        raise RuntimeError('Multiple account credentials require separate migration')
    token=next(iter(tokens)).encode()
    scope='current-user'
    try:
        encrypted=protect(token)
    except OSError:
        # SSH Windows sessions can lack a user master key. Local-machine DPAPI
        # plus a restrictive user+SYSTEM ACL keeps the blob outside plaintext.
        encrypted=protect(token,machine=True)
        scope='local-machine-with-user-acl'
    STORE.parent.mkdir(parents=True,exist_ok=True)
    restrict(STORE.parent)
    STORE.write_bytes(encrypted)
    restrict(STORE)
    if protect(STORE.read_bytes(),decrypt=True)!=token:
        raise RuntimeError('Encrypted credential verification failed')
    result=subprocess.run([str(GH),'api','user','--jq','.login'],env=github_env(),capture_output=True,text=True,timeout=30)
    if result.returncode or result.stdout.strip()!='grpiot01':
        raise RuntimeError('Encrypted credential could not authenticate the expected account')
    clean=pattern.sub('',source)
    config.write_text(clean,encoding='utf-8')
    assert not pattern.search(config.read_text(encoding='utf-8'))
    print(json.dumps({'account':'grpiot01','protected_with':'Windows DPAPI','scope':scope,'acl_restricted':True,'plaintext_token_removed':True,'authentication_verified':True}))


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--migrate',action='store_true')
    parser.add_argument('--verify',action='store_true')
    args=parser.parse_args()
    if args.migrate:
        migrate()
    if args.verify:
        result=subprocess.run([str(GH),'api','user','--jq','.login'],env=github_env(),capture_output=True,text=True,timeout=30)
        if result.returncode:
            raise RuntimeError('Protected authentication failed')
        print(json.dumps({'account':result.stdout.strip(),'protected_authentication':True}))
