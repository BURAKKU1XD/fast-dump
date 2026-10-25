use super::theme::Theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

pub(super) fn panel<'a>(title: &'a str, theme: Theme, active: bool) -> Block<'a> {
    Block::default()
        .borders(Borders::ALL)
        .title(title)
        .style(theme.panel())
        .border_style(Style::default().fg(if active { theme.cyan } else { theme.muted }))
}

pub(super) fn render_dialog(frame: &mut Frame<'_>, theme: Theme, title: &str, lines: &[&str]) {
    let screen = frame.area();
    let width = screen.width.saturating_sub(4).min(72);
    let height = screen.height.saturating_sub(2).min(lines.len() as u16 + 2);
    let area = Rect::new(
        screen.x + screen.width.saturating_sub(width) / 2,
        screen.y + screen.height.saturating_sub(height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(
            lines
                .iter()
                .map(|line| Line::from(*line))
                .collect::<Vec<_>>(),
        )
        .block(panel(title, theme, true))
        .style(theme.panel())
        .wrap(Wrap { trim: false }),
        area,
    );
}

pub(super) fn safe_text(text: &str) -> String {
    text.chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

pub(super) fn bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut amount = value as f64;
    let mut unit = 0;
    while amount >= 1024.0 && unit + 1 < UNITS.len() {
        amount /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{amount:.1} {}", UNITS[unit])
    }
}
