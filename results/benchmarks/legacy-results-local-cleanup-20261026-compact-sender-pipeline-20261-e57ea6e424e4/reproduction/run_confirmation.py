"""Fixed second batch: retain all primary observations and check regression fixtures."""
from run_measurements import run_case

PRIMARY = [
    ('exp2-small', 'exp2-metadata', 'manySmallFiles', 'e2e'),
    ('exp2-read-small', 'exp2-metadata', 'manySmallFiles', 'read'),
    ('exp3-small', 'exp3-refill', 'manySmallFiles', 'e2e'),
    ('exp3-startup', 'exp3-refill', 'startup-tree', 'e2e'),
    ('exp4-big', 'exp4-double-buffer', 'bigFiles', 'e2e'),
    ('exp4-random', 'exp4-double-buffer', 'incompressible', 'e2e'),
    ('exp5-medium', 'exp5-prefix', 'medium-prefix', 'e2e'),
    ('exp5-read-medium', 'exp5-prefix', 'medium-prefix', 'read'),
]

for label, candidate, fixture, stage in PRIMARY:
    run_case(label + '-confirm', candidate, fixture, stage)

run_case('aa-mixed', 'baseline', 'mixed', identical=True)
for label, candidate, fixture in [
    ('exp2-big-control', 'exp2-metadata', 'bigFiles'),
    ('exp2-mixed-control', 'exp2-metadata', 'mixed'),
    ('exp5-small-control', 'exp5-prefix', 'manySmallFiles'),
    ('exp5-big-control', 'exp5-prefix', 'bigFiles'),
    ('exp5-mixed-control', 'exp5-prefix', 'mixed'),
]:
    run_case(label, candidate, fixture)
