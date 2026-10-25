import hashlib
import json
from pathlib import Path

RUN = Path(__file__).resolve().parent
old = RUN.parent / 'sender-20261025/fixtures'
root = RUN / 'fixtures/wide-tree'
root.mkdir(parents=True, exist_ok=False)
total = 0
digest = hashlib.sha256()
for directory in range(512):
    folder = root / f'directory-{directory:04}'
    folder.mkdir()
    for index in range(8):
        path = folder / f'file-{index:02}.bin'
        size = 127 + ((directory * 131 + index * 97) % 897)
        contents = bytes((directory + index + offset) % 256 for offset in range(size))
        path.write_bytes(contents)
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(hashlib.sha256(contents).digest())
        total += size
fixtures = {name: str(old / name) for name in ['manySmallFiles', 'bigFiles', 'mixed']}
assert all(Path(path).is_dir() for path in fixtures.values())
fixtures['wide-tree'] = str(root)
(RUN / 'fixtures.json').write_text(json.dumps({
    'paths': fixtures, 'wide_tree': {'directories': 512, 'files': 4096,
    'bytes': total, 'sha256_path_and_content_digest': digest.hexdigest()},
    'prior_fixture_source': str(old),
    'cache': 'uncontrolled; verification can warm later samples'
}, indent=2)+'\n')
print('created wide-tree', total, 'bytes')
