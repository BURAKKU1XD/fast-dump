import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]
FIXTURES = json.loads((RUN/'fixtures.json').read_text())['paths']
CONTROL_ITERATIONS = {'commit': 1_000_000, 'plan-short-1': 1_000_000,
    'plan-long-1': 1_000_000, 'plan-short-256': 16384, 'plan-long-256': 8192,
    'plan-short-4096': 1024, 'plan-long-4096': 512, 'noop': 1_000_000}

def save(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8')

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def case(label, candidate, fixture, stage='e2e', repeats=5, allocations=False):
    output = RUN/'measurements'/label
    if output.exists():
        raise RuntimeError('refusing overwrite: '+str(output))
    suffix = '-alloc' if allocations else ''
    executable = {'e2e':'loopback_send_dump', 'scan':'file_pipeline', 'noop':'file_pipeline', 'control':'control_messages'}[stage]+'.exe'
    binaries = {'before':RUN/'artifacts'/('baseline'+suffix)/executable,
                'after':RUN/'artifacts'/(candidate+suffix)/executable}
    hashes = {side:sha(path) for side,path in binaries.items()}
    identical = hashes['before']==hashes['after']
    if candidate != 'baseline' and identical:
        raise RuntimeError('unexpected identical binaries')
    logs = RUN/'driver-logs'
    logs.mkdir(exist_ok=True)
    started = time.time()
    print('START', label, flush=True)
    if stage == 'control':
        command, result = control_case(output, binaries, hashes, fixture, repeats, allocations)
    else:
        script = 'bench-paired.py' if stage=='e2e' else 'bench-files-paired.py'
        command = [sys.executable,'-B',str(REPO/'scripts'/script),'--before',str(binaries['before']),
            '--after',str(binaries['after']),'--output',str(output),'--source',FIXTURES[fixture],
            '--repeats',str(repeats),'--readers','8']
        if stage=='e2e':
            command += ['--threads','4','--frames','64','--buffers','1024',
                        '--executor','default','--network-profile','loopback-localhost']
        else:
            command += ['--stages',stage]
        if allocations:
            command += ['--allocations']
        if identical:
            command += ['--allow-identical-binaries']
        env = os.environ.copy()
        env['FAST_DUMP_BENCH_TEMP_DIR'] = str(RUN/'scratch')
        with (logs/(label+'.log')).open('wb') as log:
            result = subprocess.run(command,cwd=REPO,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
    metadata = {'label':label,'candidate':candidate,'fixture':fixture,'stage':stage,
        'allocations':allocations,'command':command,'exit_code':result,'elapsed_s':time.time()-started,
        'binary_hashes':hashes,'scope':'sender_receiver_process' if stage=='e2e' else 'isolated_sender_stage',
        'cache':'uncontrolled; every first sample retained; verification can warm cache'}
    save(logs/(label+'.json'), metadata)
    if result:
        raise RuntimeError('failed: '+label)
    print('END',label,f'{metadata["elapsed_s"]:.1f}s',flush=True)

def control_case(output,binaries,hashes,scenario,repeats,allocations):
    output.mkdir(parents=True)
    iterations = 128 if allocations else CONTROL_ITERATIONS[scenario]
    common = ['--case',scenario,'--iterations',str(iterations),'--repeats','1']
    if allocations:
        common += ['--allocations']
    manifest = {'complete':False,'comparison_kind':'A/A' if hashes['before']==hashes['after'] else 'A/B',
        'binary_sha256':hashes,'repeats':repeats,'allocations_diagnostic':allocations,
        'harness':'control_messages','common_arguments':common,
        'policy':'Alternating AB/BA; all samples retained; preparation and verification outside timing.',
        'invocations':[]}
    rows=[]
    identity=None
    for pair in range(1,repeats+1):
        for side in (['before','after'] if pair%2 else ['after','before']):
            child=output/'runs'/f'{pair:02}-{side}'
            child.mkdir(parents=True)
            command=[str(binaries[side]),*common]
            record={'pair':pair,'side':side,'command':command,'log':str(child/'run.log')}
            manifest['invocations'].append(record)
            save(output/'paired-run.json',manifest)
            with (child/'run.log').open('wb') as log:
                result=subprocess.run(command,cwd=REPO,stdout=log,stderr=subprocess.STDOUT).returncode
            record['exit_code']=result
            if result:
                save(output/'paired-run.json',manifest)
                return [str(binaries['before']),*common],result
            lines=[line for line in (child/'run.log').read_text(encoding='utf-8').splitlines()
                   if line.startswith('[control-perf] ')]
            if len(lines)!=1:
                raise RuntimeError(('unexpected control result',child,lines))
            measured=dict(re.findall(r'([a-z_0-9]+)=([^\s]+)',lines[0]))
            if measured.get('verified')!='true':
                raise RuntimeError(('unverified control result',child))
            contract=tuple(measured.get(k) for k in ['case','iterations','entries','bytes','fingerprint','packet_bytes','body_bytes',
                'allocator_instrumented','allocations_enabled','counters_schema','cycle_source','io_source'])
            if identity is None:
                identity=contract
            if identity!=contract:
                raise RuntimeError(('control contract mismatch',child,identity,contract))
            if measured.get('case')!=scenario or measured.get('iterations')!=str(iterations):
                raise RuntimeError('control scenario mismatch')
            if any(measured.get(k)!=str(allocations).lower() for k in ['allocator_instrumented','allocations_enabled']):
                raise RuntimeError('allocator instrumentation mismatch')
            measured.update(stage='control',readers='0')
            record.update(complete=True,measurement=measured)
            rows.append({'pair':pair,'side':side,**measured,'log':str(child/'run.log')})
            save(output/'paired-run.json',manifest)
    if hashes!={side:sha(path) for side,path in binaries.items()}:
        raise RuntimeError('binary changed during run')
    with (output/'samples.csv').open('w',newline='',encoding='utf-8') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    manifest['complete']=True
    save(output/'paired-run.json',manifest)
    return [str(binaries['before']),*common],0

def suite(name):
    if name=='scan':
        case('aa-scan-noop','baseline','wide-tree','noop')
        for fixture in ['manySmallFiles','wide-tree']:
            case('aa-scan-'+fixture,'baseline',fixture,'scan')
            for candidate in ['m1-scan-tail','m2-subtree-move']:
                case(candidate+'-scan-'+fixture,candidate,fixture,'scan',10)
                case(candidate+'-alloc-'+fixture,candidate,fixture,'scan',5,True)
    elif name=='control':
        for scenario in CONTROL_ITERATIONS:
            case('aa-control-'+scenario,'baseline',scenario,'control')
            if scenario!='noop':
                case('m3-control-'+scenario,'m3-control-capacity',scenario,'control',10)
                case('m3-alloc-'+scenario,'m3-control-capacity',scenario,'control',5,True)
    elif name=='e2e':
        for fixture in ['manySmallFiles','mixed','bigFiles']:
            case('aa-e2e-'+fixture,'baseline',fixture)
            for candidate in ['m1-scan-tail','m2-subtree-move','m3-control-capacity']:
                case(candidate+'-e2e-'+fixture,candidate,fixture)

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('suite',choices=['scan','control','e2e','all'])
    args=parser.parse_args()
    for name in ['scan','control','e2e'] if args.suite=='all' else [args.suite]:
        suite(name)
