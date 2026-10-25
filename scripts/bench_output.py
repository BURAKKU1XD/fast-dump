"""Persistent local output paths shared by benchmark drivers."""

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def default_output(suite):
    root = Path(os.environ.get('FAST_DUMP_RESULTS_RUN_DIR',
                               Path(__file__).resolve().parents[1] / 'results/local/benchmarks'))
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    # Каталог создаёт драйвер после проверки аргументов и неизменности fixture.
    return root / f'{stamp}-{suite}-{uuid4().hex[:8]}'
