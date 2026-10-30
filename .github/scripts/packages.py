"""Audit/clean this repository's container versions without deleting index children."""
import hashlib, json, os, urllib.request, urllib.error
from pathlib import Path
root=Path('package-audit');root.mkdir(exist_ok=True)
base='https://api.github.com/users/Wuty-zju/packages/container/go2rtc'
def api(path='',method='GET'):
 request=urllib.request.Request(base+path,method=method,headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'})
 with urllib.request.urlopen(request,timeout=30) as response:
  data=response.read();return json.loads(data) if data else None
package=api();assert package['repository']['full_name'].lower()=='wuty-zju/go2rtc'
versions=[]
for page in range(1,101):
 items=api('/versions?per_page=100&page='+str(page));versions.extend(items)
 if len(items)<100:break
else:raise RuntimeError('Incomplete package pagination')
(root/'before.json').write_text(json.dumps(versions,indent=2)+'\n')
print('Versions:',len(versions))
if os.environ['MODE']=='cleanup':
 with urllib.request.urlopen('https://ghcr.io/token?scope=repository:wuty-zju/go2rtc:pull') as r:token=json.load(r)['token']
 keep=set()
 def visit(ref):
  request=urllib.request.Request('https://ghcr.io/v2/wuty-zju/go2rtc/manifests/'+ref,headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json'})
  with urllib.request.urlopen(request,timeout=30) as r:digest=r.headers['Docker-Content-Digest'];raw=r.read()
  assert digest=='sha256:'+hashlib.sha256(raw).hexdigest()
  data=json.loads(raw)
  if digest in keep:return
  keep.add(digest)
  for child in data.get('manifests',[]):visit(child['digest'])
 distribution=json.loads(Path('.github/distribution.json').read_text())
 stable=distribution['versions']['master'];assert not stable.endswith('_dev')
 tags=[name+suffix for name in ['master','dev','latest',stable] for suffix in ['', '-hardware','-rockchip']]
 for tag in tags:visit(tag)
 # Preserve tagged history and its untagged platform manifests, including older releases.
 for version in versions:
  if version['metadata']['container']['tags']:visit(version['name'])
 assert len(keep)>=20
 deleted=[]
 try:
  for version in versions:
   if version['name'] not in keep:
    api('/versions/'+str(version['id']),'DELETE');deleted.append({'id':version['id'],'digest':version['name'],'tags':version['metadata']['container']['tags']})
 finally:(root/'cleanup.json').write_text(json.dumps({'protected_tags':tags,'protected_digests':sorted(keep),'deleted':deleted},indent=2)+'\n')
 print('Deleted obsolete versions:',len(deleted),'protected manifests:',len(keep))
