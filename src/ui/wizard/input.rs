use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::text::Span;

#[derive(Default)]
pub(super) struct Input {
    pub(super) value: String,
    pub(super) cursor: usize,
}

impl Input {
    pub(super) fn new(value: &str) -> Self {
        Self {
            value: value.into(),
            cursor: value.chars().count(),
        }
    }

    pub(super) fn byte_at(&self, cursor: usize) -> usize {
        self.value
            .char_indices()
            .nth(cursor)
            .map_or(self.value.len(), |(index, _)| index)
    }

    pub(super) fn insert(&mut self, value: &str) {
        let value = value
            .chars()
            .filter(|ch| !ch.is_control())
            .collect::<String>();
        // Секреты и пути не попадают в event log. Ограничение относится только
        // к редактируемому UI-полю и предотвращает неограниченную вставку из clipboard.
        if self.value.len().saturating_add(value.len()) > 32 * 1024 {
            return;
        }
        let at = self.byte_at(self.cursor);
        self.value.insert_str(at, &value);
        self.cursor += value.chars().count();
    }

    pub(super) fn key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char(ch)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.insert(&ch.to_string())
            }
            KeyCode::Backspace if self.cursor > 0 => {
                let start = self.byte_at(self.cursor - 1);
                let end = self.byte_at(self.cursor);
                self.value.replace_range(start..end, "");
                self.cursor -= 1;
            }
            KeyCode::Delete if self.cursor < self.value.chars().count() => {
                let start = self.byte_at(self.cursor);
                let end = self.byte_at(self.cursor + 1);
                self.value.replace_range(start..end, "");
            }
            KeyCode::Left => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Right => self.cursor = (self.cursor + 1).min(self.value.chars().count()),
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.value.chars().count(),
            _ => {}
        }
    }

    pub(super) fn display(&self, masked: bool, active: bool) -> String {
        let chars = if masked {
            self.value.chars().map(|_| '•').collect::<Vec<_>>()
        } else {
            self.value.chars().collect::<Vec<_>>()
        };
        let mut text = String::new();
        for (index, ch) in chars.iter().enumerate() {
            if active && index == self.cursor {
                text.push('▏');
            }
            text.push(*ch);
        }
        if active && self.cursor == chars.len() {
            text.push('▏');
        }
        text
    }
}

pub(super) fn field_window(value: &str, cursor: Option<usize>, width: usize) -> String {
    if Span::raw(value).width() <= width {
        return value.to_owned();
    }
    let characters: Vec<_> = value.chars().collect();
    let mut start = cursor.unwrap_or(0).min(characters.len());
    let mut used = 0;
    while start > 0 {
        let next = Span::raw(characters[start - 1].to_string()).width();
        if used + next > width.saturating_sub(4) {
            break;
        }
        used += next;
        start -= 1;
    }
    let mut result = if start > 0 {
        "…".to_owned()
    } else {
        String::new()
    };
    used = usize::from(start > 0);
    for character in characters.iter().skip(start) {
        let next = Span::raw(character.to_string()).width();
        if used + next > width.saturating_sub(1) {
            result.push('…');
            break;
        }
        result.push(*character);
        used += next;
    }
    result
}
