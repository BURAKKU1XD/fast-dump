import hashlib
import json
from pathlib import Path
import random

ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / 'fixtures'
MIB = 1024 * 1024
records = []


def write_file(root, relative, size, seed, random_only=False):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    generator = random.Random(seed)
    pattern = generator.randbytes(65536)
    digest = hashlib.sha256()
    with path.open('xb') as output:
        remaining = size
        index = 0
        while remaining:
            length = min(MIB, remaining)
            if random_only or index % 3 == 0:
                block = generator.randbytes(length)
            else:
                block = (pattern * ((length + len(pattern) - 1) // len(pattern)))[:length]
            output.write(block)
            digest.update(block)
            remaining -= length
            index += 1
    records.append({'fixture': root.name, 'path': relative, 'bytes': size,
                    'sha256': digest.hexdigest()})


medium = FIXTURES / 'medium-prefix'
for group in range(16):
    size = [9, 16, 32, 64][group % 4] * MIB
    write_file(medium, f'{group:03}/000-medium.bin', size, 12000 + group)
    for small in range(16):
        write_file(medium, f'{group:03}/{small+1:03}-small.bin', 4096,
                   24000 + group * 16 + small)

startup = FIXTURES / 'startup-tree'
for index in range(1024):
    write_file(startup, f'a-first/{index:05}.bin', 8192, 35000 + index)
for directory in range(128):
    for index in range(128):
        write_file(startup, f'z-later/{directory:03}/deep/nested/{index:04}.bin',
                   2048, 50000 + directory * 128 + index)

write_file(FIXTURES / 'incompressible', 'random-256mib.bin', 256 * MIB,
           0x2FD2026, random_only=True)

manifest = {'generation': 'deterministic Python Random seed; fixed bytes, no sparse files',
            'files': records}
(ROOT / 'fixture-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
for name in sorted({row['fixture'] for row in records}):
    rows = [row for row in records if row['fixture'] == name]
    print(name, len(rows), sum(row['bytes'] for row in rows), flush=True)
