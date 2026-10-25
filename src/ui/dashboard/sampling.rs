use super::{Dashboard, HISTORY_POINTS, HISTORY_SECONDS, RATE_SECONDS};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

impl Dashboard {
    pub(super) fn sample(&mut self, now: Instant) {
        let Some((id, total, raw_total, finished)) =
            self.sessions.get(self.selected).map(|session| {
                (
                    session.id,
                    session.transferred_bytes,
                    session.raw_processed_bytes,
                    session.phase.is_finished(),
                )
            })
        else {
            return;
        };
        match self.last_sample {
            Some((previous_id, at, bytes)) if previous_id == id => {
                let elapsed = now.saturating_duration_since(at).as_secs_f64();
                // Клавиши могут вызвать redraw чаще таймера; samples остаются
                // ограничены пятью в секунду и не превращают ввод в ложные пики.
                if elapsed < 0.19 {
                    return;
                }
                self.add_buckets(at, now, total.saturating_sub(bytes) as f64 / elapsed);
                self.rate_samples.push_back((now, total));
                self.update_rate(now, total);
                self.source_rate_samples.push_back((now, raw_total));
                self.source_rate =
                    Self::sampled_rate(&mut self.source_rate_samples, now, raw_total);
            }
            _ => {
                self.history.clear();
                self.rate_samples.clear();
                self.rate_samples.push_back((now, total));
                self.source_rate_samples.clear();
                self.source_rate_samples.push_back((now, raw_total));
                self.bucket = Some((now, 0.0));
                self.rate = 0.0;
                self.source_rate = 0.0;
            }
        }
        if finished {
            self.source_rate = 0.0;
        }
        self.last_sample = Some((id, now, total));
        while self.history.len() > HISTORY_POINTS
            || self.history.front().is_some_and(|(at, _)| {
                now.saturating_duration_since(*at) > Duration::from_secs(HISTORY_SECONDS)
            })
        {
            self.history.pop_front();
        }
    }

    pub(super) fn update_rate(&mut self, now: Instant, total: u64) {
        self.rate = Self::sampled_rate(&mut self.rate_samples, now, total);
    }

    pub(super) fn sampled_rate(
        samples: &mut VecDeque<(Instant, u64)>,
        now: Instant,
        total: u64,
    ) -> f64 {
        let cutoff = now
            .checked_sub(Duration::from_secs(RATE_SECONDS))
            .unwrap_or(now);
        // Оставляем один sample перед границей окна. Интерполяция этого короткого
        // интервала удерживает окно ровно в 5 s даже при нерегулярном redraw.
        while samples.len() > 2 && samples[1].0 <= cutoff {
            samples.pop_front();
        }
        let (first_at, first_bytes) = samples[0];
        let mut transferred = total.saturating_sub(first_bytes) as f64;
        let started = first_at.max(cutoff);
        if first_at < cutoff
            && let Some(&(next_at, next_bytes)) = samples.get(1)
        {
            let interval = next_at.duration_since(first_at).as_secs_f64();
            let excluded = cutoff.duration_since(first_at).as_secs_f64() / interval;
            transferred -= next_bytes.saturating_sub(first_bytes) as f64 * excluded;
        }
        let elapsed = now.saturating_duration_since(started).as_secs_f64();
        if elapsed > 0.0 {
            transferred.max(0.0) / elapsed
        } else {
            0.0
        }
    }

    pub(super) fn add_buckets(&mut self, at: Instant, now: Instant, rate: f64) {
        let mut cursor = at;
        if now.saturating_duration_since(at) >= Duration::from_secs(HISTORY_SECONDS) {
            // После долгой приостановки UI обрабатываем только видимую минуту,
            // а не создаём по bucket на каждую секунду вне экрана.
            self.history.clear();
            cursor = now - Duration::from_secs(HISTORY_SECONDS);
            self.bucket = Some((cursor, 0.0));
        }
        let (mut started, mut bytes) = self.bucket.unwrap_or((at, 0.0));
        while cursor < now {
            let boundary = started + Duration::from_secs(1);
            let end = now.min(boundary);
            bytes += rate * end.saturating_duration_since(cursor).as_secs_f64();
            if end == boundary {
                if self.history.len() == HISTORY_POINTS {
                    self.history.pop_front();
                }
                self.history.push_back((boundary, bytes));
                started = boundary;
                bytes = 0.0;
            }
            cursor = end;
        }
        self.bucket = Some((started, bytes));
    }
}
