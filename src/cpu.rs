use std::sync::OnceLock;

pub(crate) fn available_physical_cores() -> usize {
    static CORES: OnceLock<usize> = OnceLock::new();
    // Это снимок топологии при первом запуске: повторные настройки сессий не
    // должны заново обходить sysfs или запрашивать группы процессоров Windows.
    *CORES.get_or_init(|| {
        platform_physical_cores()
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from))
            .max(1)
    })
}

#[cfg(windows)]
fn platform_physical_cores() -> Option<usize> {
    use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, GetLastError};
    use windows_sys::Win32::System::SystemInformation::{
        GetLogicalProcessorInformationEx, RelationProcessorCore,
    };

    // Выравнивание u64 подходит для записей WinAPI; их длина переменная,
    // поэтому нельзя обходить ответ как массив Rust-структур фиксированного размера.
    let mut storage = Vec::<u64>::new();
    for _ in 0..4 {
        let capacity = u32::try_from(storage.len().checked_mul(size_of::<u64>())?).ok()?;
        let mut length = capacity;
        let buffer = if storage.is_empty() {
            std::ptr::null_mut()
        } else {
            storage.as_mut_ptr().cast()
        };
        // SAFETY: буфер выровнен, инициализирован и вмещает capacity байт;
        // при нулевой ёмкости API только возвращает необходимый размер.
        let success =
            unsafe { GetLogicalProcessorInformationEx(RelationProcessorCore, buffer, &mut length) };
        if success != 0 {
            if length > capacity {
                return None;
            }
            // SAFETY: WinAPI успешно записал length байт в storage; предыдущая
            // проверка ограничивает срез фактической ёмкостью буфера.
            let bytes = unsafe {
                std::slice::from_raw_parts(storage.as_ptr().cast::<u8>(), length as usize)
            };
            return count_windows_core_records(bytes);
        }
        // SAFETY: GetLastError не принимает указателей и вызывается сразу после
        // неудачного WinAPI-вызова; length определён только при этой ошибке.
        if unsafe { GetLastError() } != ERROR_INSUFFICIENT_BUFFER || length <= capacity {
            return None;
        }
        let words = (length as usize).div_ceil(size_of::<u64>());
        storage.try_reserve_exact(words - storage.len()).ok()?;
        storage.resize(words, 0);
    }
    // При непрерывном изменении топологии используем штатную оценку параллелизма.
    None
}

#[cfg(any(windows, test))]
fn count_windows_core_records(mut bytes: &[u8]) -> Option<usize> {
    let mut cores = 0usize;
    while !bytes.is_empty() {
        let header = bytes.get(..8)?;
        let relationship = i32::from_ne_bytes(header[..4].try_into().ok()?);
        let length = u32::from_ne_bytes(header[4..].try_into().ok()?) as usize;
        // RelationProcessorCore == 0. Считаем все записи, а не биты одной
        // affinity-маски: так не теряются ядра за пределами первой группы из 64 CPU.
        if relationship != 0 || length < header.len() {
            return None;
        }
        bytes = bytes.get(length..)?;
        cores = cores.checked_add(1)?;
    }
    (cores > 0).then_some(cores)
}

#[cfg(target_os = "linux")]
fn platform_physical_cores() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/thread-self/status").ok()?;
    let count = count_linux_cores(&status, |cpu| {
        std::fs::read_to_string(format!(
            "/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
        ))
        .ok()
    })?;
    // Маска affinity ограничивает доступные ядра, а available_parallelism
    // дополнительно учитывает квоту CPU контейнера, когда она доступна.
    Some(std::thread::available_parallelism().map_or(count, |limit| count.min(limit.get())))
}

#[cfg(not(any(windows, target_os = "linux")))]
fn platform_physical_cores() -> Option<usize> {
    None
}

#[cfg(any(target_os = "linux", test))]
fn count_linux_cores(
    status: &str,
    mut siblings_for_cpu: impl FnMut(usize) -> Option<String>,
) -> Option<usize> {
    let allowed = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))?;
    let allowed = parse_cpu_ranges(allowed)?;
    let mut cores = std::collections::BTreeSet::new();
    for (first, last) in allowed {
        for cpu in first..=last {
            let siblings = parse_cpu_ranges(&siblings_for_cpu(cpu)?)?;
            if !siblings
                .iter()
                .any(|&(start, end)| start <= cpu && cpu <= end)
            {
                return None;
            }
            // Минимальный номер sibling уникален для ядра независимо от SMT,
            // сокета и типа ядра; недоступные siblings сами читателей не добавляют.
            cores.insert(siblings.first()?.0);
        }
    }
    (!cores.is_empty()).then_some(cores.len())
}

#[cfg(any(target_os = "linux", test))]
fn parse_cpu_ranges(text: &str) -> Option<Vec<(usize, usize)>> {
    let mut ranges = Vec::new();
    for item in text.trim().split(',') {
        let (first, last) = if let Some((first, last)) = item.split_once('-') {
            (first.parse::<usize>().ok()?, last.parse::<usize>().ok()?)
        } else {
            let cpu = item.parse::<usize>().ok()?;
            (cpu, cpu)
        };
        if first > last
            || ranges
                .last()
                .is_some_and(|&(_, previous)| first <= previous)
        {
            return None;
        }
        ranges.push((first, last));
    }
    Some(ranges)
}

#[cfg(test)]
mod tests {
    use super::{count_linux_cores, count_windows_core_records, parse_cpu_ranges};

    #[test]
    fn windows_counts_all_core_records_beyond_one_processor_group() {
        let mut records = Vec::new();
        for index in 0..176 {
            let length = if index % 2 == 0 { 48u32 } else { 64u32 };
            records.extend_from_slice(&0i32.to_ne_bytes());
            records.extend_from_slice(&length.to_ne_bytes());
            records.resize(records.len() + length as usize - 8, 0);
        }
        assert_eq!(count_windows_core_records(&records), Some(176));
    }

    #[test]
    fn windows_rejects_incomplete_or_invalid_records() {
        assert_eq!(count_windows_core_records(&[]), None);
        assert_eq!(count_windows_core_records(&[0; 7]), None);
        for (relationship, length) in [(0i32, 0u32), (0, 7), (0, 9), (1, 8)] {
            let mut record = relationship.to_ne_bytes().to_vec();
            record.extend_from_slice(&length.to_ne_bytes());
            assert_eq!(count_windows_core_records(&record), None);
        }
    }

    #[test]
    fn linux_counts_hybrid_cores_without_assuming_two_threads_each() {
        let topology = ["0,4", "1,5", "2", "3", "0,4", "1,5"];
        assert_eq!(
            count_linux_cores("Cpus_allowed_list:\t0-5\n", |cpu| {
                Some(topology[cpu].to_owned())
            }),
            Some(4)
        );
    }

    #[test]
    fn linux_counts_only_cores_intersecting_sparse_affinity() {
        assert_eq!(
            count_linux_cores("Name:\tfast-dump\nCpus_allowed_list:\t2,178-179\n", |cpu| {
                Some(match cpu {
                    2 | 178 => "2,178".to_owned(),
                    179 => "3,179".to_owned(),
                    _ => panic!("queried disallowed CPU {cpu}"),
                })
            }),
            Some(2)
        );
    }

    #[test]
    fn linux_incomplete_topology_requires_fallback() {
        assert_eq!(count_linux_cores("Name:\tfast-dump\n", |_| None), None);
        assert_eq!(count_linux_cores("Cpus_allowed_list:\t0-1", |_| None), None);
        assert_eq!(
            count_linux_cores("Cpus_allowed_list:\t0-1", |cpu| {
                Some(if cpu == 0 { "0" } else { "bad" }.to_owned())
            }),
            None
        );
        assert_eq!(
            count_linux_cores("Cpus_allowed_list:\t3", |_| Some("0-2".to_owned())),
            None
        );
    }

    #[test]
    fn cpu_lists_reject_empty_overlapping_reversed_or_malformed_ranges() {
        assert_eq!(
            parse_cpu_ranges("\t0-2,64,176-179\n"),
            Some(vec![(0, 2), (64, 64), (176, 179)])
        );
        for invalid in ["", "1,1", "0-2,2", "3-1", "0-", "-1", "0,,2", "x", "0-1-2"] {
            assert_eq!(parse_cpu_ranges(invalid), None, "{invalid:?}");
        }
    }
}
