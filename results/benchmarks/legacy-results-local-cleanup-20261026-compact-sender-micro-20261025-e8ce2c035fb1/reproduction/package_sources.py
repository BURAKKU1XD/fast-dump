import difflib
import hashlib
import json
from pathlib import Path
import subprocess

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]
PATCHES = RUN/'patches'
PATCHES.mkdir(exist_ok=True)

def inventory(root, paths=None):
    if paths is None:
        paths = [p.relative_to(root).as_posix() for p in root.rglob('*')
                 if p.is_file() and '__pycache__' not in p.parts
                 and not p.relative_to(root).parts[0] in ['results','target']]
    return {p: hashlib.sha256((root/p).read_bytes()).hexdigest() for p in sorted(paths) if (root/p).is_file()}

def digest(files):
    return hashlib.sha256(json.dumps(files,sort_keys=True).encode()).hexdigest()

def patch(left_root,left,right_root,right,path):
    changed=sorted(p for p in left.keys()|right.keys() if left.get(p)!=right.get(p))
    lines=[]
    for name in changed:
        before=(left_root/name).read_text(encoding='utf-8').splitlines(keepends=True) if name in left else []
        after=(right_root/name).read_text(encoding='utf-8').splitlines(keepends=True) if name in right else []
        lines.extend(difflib.unified_diff(before,after,fromfile='a/'+name if name in left else '/dev/null',
                     tofile='b/'+name if name in right else '/dev/null'))
    path.write_text(''.join(lines),encoding='utf-8',newline='\n')
    return changed

paths=[p for p in subprocess.check_output(['git','ls-files'],cwd=REPO,text=True).splitlines() if not p.startswith('results/')]
original=inventory(REPO,paths)
base_root=RUN/'sources/baseline'
base=inventory(base_root)
common_changed=patch(REPO,original,base_root,base,PATCHES/'common-harness.patch')
manifests={'baseline_original':{'source_sha256':digest(original),'files':original,'changed_paths':[]},
           'baseline':{'source_sha256':digest(base),'files':base,'changed_paths':common_changed}}
for candidate in ['m1-scan-tail','m2-subtree-move','m3-control-capacity']:
    root=RUN/'sources'/candidate
    files=inventory(root)
    changed=patch(base_root,base,root,files,PATCHES/(candidate+'.patch'))
    manifests[candidate]={'source_sha256':digest(files),'files':files,'changed_paths':changed}
    print(candidate,changed,digest(files))
(RUN/'source-manifests.json').write_text(json.dumps(manifests,indent=2)+'\n')
