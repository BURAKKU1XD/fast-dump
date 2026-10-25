import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]

def load(path):
    return json.loads(path.read_text(encoding='utf-8'))

def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

def git(*args):
    return subprocess.check_output(['git', *args], cwd=REPO, text=True).strip()

head = git('rev-parse', 'HEAD')
counter = git('rev-parse', 'HEAD^')
base = '2a7c8eeeb70f9046a4629168d66d4fc0123f247e'
assert git('status', '--porcelain', '--untracked-files=no') == ''
assert set(git('diff', '--name-only', base, head).splitlines()) == {'src/sender/plan.rs', 'src/network/protocol/wire.rs'}
assert git('diff-tree', '--no-commit-id', '--name-only', '-r', head) == 'src/network/protocol/wire.rs'
assert git('diff-tree', '--no-commit-id', '--name-only', '-r', counter) == 'src/sender/plan.rs'
source = load(RUN / 'commit-source.json')
assert all(hashlib.sha256((REPO / p).read_bytes()).hexdigest() == h for p, h in source['files'].items())
analysis = load(RUN / 'analysis.json')
runs = analysis['measurement_runs']
assert len(runs) == 41 and all(r['valid_evidence'] and not r['errors'] for r in runs)
assert sum(len(r['observations']) for r in runs) == 530
e2e = sum(len(r['observations']) for r in runs if r['metadata']['phase'] == 'e2e')
assert e2e == 120
decision = load(RUN / 'commit-decision.json')
decision.update(status='accepted', commit=head)
save(RUN / 'commit-decision.json', decision)
save(RUN / 'experiment.json', {
    'schema': 1, 'status': 'completed', 'baseline_commit': base, 'final_commit': head,
    'started_utc': min(r['metadata']['started_utc'] for r in runs),
    'completed_utc': datetime.now(timezone.utc).isoformat(),
    'accepted_commits': {'counter': counter, 'commit': head},
    'rejected_candidates': ['arc', 'progress'],
    'counts': {'series': 41, 'observations': 530, 'e2e_observations': 120},
    'performance_scope': 'Windows, local targeted stages and localhost sender+receiver; Linux correctness only',
    'main_matches_verified_final_snapshot': True,
    'compiler': subprocess.check_output(['rustc', '-vV'], text=True),
})
save(RUN / 'adoption-decisions.json', {
    'conclusion': 'Из четырёх вариантов внедрены два: счётчик terminal skips и стековое кодирование BundleCommit. Каждый принят отдельным коммитом; остальные изменения откатили.',
    'decisions': [
        {'candidate': 'arc', 'verdict': 'Отклонён: эффект не доказан',
         'evidence': 'Read manySmallFiles: 278,691 → 281,978 мс (+1,18% времени); CPU/cycles также без подтверждённого выигрыша. Целевые тесты прошли; E2E не запускался после неуспешного screening.'},
        {'candidate': 'progress', 'verdict': 'Отклонён: регрессия контроля без progress',
         'evidence': 'Переход состояния: время −5,79%, cycles −6,23%. None: время +19,34%, cycles +19,61%, оба за A/A. Тесты семантики прошли; observed E2E не запускался.'},
        {'candidate': 'counter', 'verdict': 'Внедрён', 'commit': counter,
         'evidence': 'Resume с 4096 skips: время −32,38%, на запуск 4097 успешных allocations меньше. Новые уникальные skips: время −31,33%; на 4096 записей 4097 allocations и 10 reallocations меньше. Полные Windows/Linux checks и 60 E2E архивов прошли.'},
        {'candidate': 'commit', 'verdict': 'Внедрён', 'commit': head,
         'evidence': 'Кодирование BundleCommit в заранее выделенный Vec sink: время −92,12% при 1 млн и −94,90% при 16 млн итераций. На сообщение 1 allocation + 3 reallocations → 0; wire bytes идентичны. Windows/Linux checks и 60 E2E архивов прошли.'},
    ],
    'limitations': [
        'Общее ускорение передачи не доказано. Проценты 31–32% относятся к обработке skips, 92–95% — только к кодированию BundleCommit.',
        'Counter E2E: mixed elapsed номинально +6,14% (CI slowdown 0,91..11,53%, A/A 23,50%); manySmall cycles +2,43% (CI 0,70..4,10%, A/A 4,22%). Mixed time-to-first-bundle +10,97% (CI 4,90..17,74%, A/A 11,50%). Это сохранённые неопределённости.',
        'Commit E2E: mixed CPU номинально +8,67% (CI slowdown 0,13..17,39%, A/A 3,51%), cycles не подтверждают ухудшение; bigFiles elapsed +4,31% с CI, включающим ноль. Отсутствие небольших регрессий не доказано.',
        'Две оптимизации измерены последовательно относительно последнего принятого commit. Проценты нельзя складывать; прямое сравнение исходной базы с суммой изменений не выполнялось.',
        'Полная локальная проверка использовала установленные Windows/Linux toolchains. Отдельная матрица MSRV 1.89 в этой сессии не запускалась.',
    ],
})
print(json.dumps({'final_commit': head, 'counter_commit': counter, 'series': 41, 'observations': 530, 'verified_archives': 120}))
