# V2 vs V3 Test Table

Это сводка по тем `v2 vs v3` тестам, которые ещё можно честно восстановить из
репозитория после полного cutover на single-stack `v3`.

Важно:

- живого `v2` рантайма в mainline уже нет, поэтому это не свежий rerun;
- ниже приведён исторический comparative baseline из
  `docs/PERFOMANCE_V3.md` (`2026-04-05`);
- все строки в сравнительной таблице относятся к одному workload:
  `manySmallFiles`, `threads=1`, `frame=16 MiB`, `buffer=256 KiB`, `repeats=1`;
- `Delta total`:
  - `+` значит `v3` быстрее `v2`;
  - `-` значит `v3` медленнее `v2`.

## Historical Apples-to-Apples Comparison

| Dataset | Network | V2 config | V2 total ms | V2 MiB/s | V3 config | V3 total ms | V3 MiB/s | Delta total | Delta throughput | Verdict |
| --- | --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |
| `manySmallFiles` | `loopback-localhost` | defaults | `4820.50` | `18.23` | old defaults `256 / 4096 / 1024` | `3900.76` | `22.53` | `+19.08%` | `+23.59%` | `v3` faster |
| `manySmallFiles` | `loopback-localhost` | defaults | `4820.50` | `18.23` | tuned `512 / 65536 / 8192` | `3716.90` | `23.65` | `+22.89%` | `+29.73%` | best `v3` result in this snapshot |
| `manySmallFiles` | `wan-high-rtt` | defaults | `37411.22` | `2.35` | old defaults `256 / 4096 / 1024` | `72555.43` | `1.21` | `-93.94%` | `-48.51%` | `v3` much slower |
| `manySmallFiles` | `wan-high-rtt` | defaults | `37411.22` | `2.35` | tuned `512 / 65536 / 8192` | `43114.33` | `2.04` | `-15.24%` | `-13.19%` | still slower than `v2` |
| `manySmallFiles` | `bandwidth-limited` | defaults | `18886.96` | `4.65` | old defaults `256 / 4096 / 1024` | `28508.81` | `3.08` | `-50.94%` | `-33.76%` | `v3` slower |
| `manySmallFiles` | `bandwidth-limited` | defaults | `18886.96` | `4.65` | tuned `512 / 65536 / 8192` | `21556.48` | `4.08` | `-14.13%` | `-12.26%` | closer, but still slower |

## Current Mainline V3 Reference

Это уже не `v2 vs v3`, а просто текущий live baseline для mainline `v3`
(`2026-04-08`, `threads=1`, `frame=64 MiB`, `buffer=1024 KiB`,
`manifest_chunk=512 KiB`, `manifest_ack_records=4096`,
`manifest_ack_kb=8192`, `repeats=3`).

| Dataset | Total ms | MiB/s |
| --- | ---: | ---: |
| `bigFiles` | `839.64` | `1524.51` |
| `manySmallFiles` | `4185.19` | `21.01` |
| `mixed` | `2168.71` | `434.92` |

## Source Notes

- Historical comparative rows: `git show HEAD:docs/PERFOMANCE_V3.md`
- Current `v3` reference: `target/bench-blazing-manifest-records-confirm-20260408/results_summary.csv`
