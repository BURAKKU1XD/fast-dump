use ratatui::style::{Color, Modifier, Style};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ColorMode {
    TrueColor,
    Indexed,
    Basic,
    Monochrome,
}

#[derive(Clone, Copy)]
pub(super) struct Theme {
    pub background: Color,
    pub panel: Color,
    pub text: Color,
    pub muted: Color,
    pub cyan: Color,
    pub violet: Color,
    pub green: Color,
    pub amber: Color,
    pub red: Color,
    pub monochrome: bool,
}

impl Theme {
    pub fn detect() -> Self {
        let term = std::env::var("TERM").unwrap_or_default();
        let color_term = std::env::var("COLORTERM").unwrap_or_default();
        let mode = if std::env::var_os("NO_COLOR").is_some() {
            ColorMode::Monochrome
        } else if matches!(color_term.as_str(), "truecolor" | "24bit")
            || term.ends_with("-direct")
            || std::env::var_os("WT_SESSION").is_some()
        {
            ColorMode::TrueColor
        } else if term.contains("256color") {
            ColorMode::Indexed
        } else {
            ColorMode::Basic
        };
        Self::new(mode)
    }

    pub fn new(mode: ColorMode) -> Self {
        let colors = match mode {
            ColorMode::TrueColor => [
                Color::Rgb(15, 23, 42),
                Color::Rgb(30, 41, 59),
                Color::Rgb(241, 245, 249),
                Color::Rgb(148, 163, 184),
                Color::Rgb(34, 211, 238),
                Color::Rgb(167, 139, 250),
                Color::Rgb(74, 222, 128),
                Color::Rgb(251, 191, 36),
                Color::Rgb(251, 113, 133),
            ],
            ColorMode::Indexed => [
                Color::Indexed(234),
                Color::Indexed(235),
                Color::Indexed(255),
                Color::Indexed(246),
                Color::Indexed(44),
                Color::Indexed(141),
                Color::Indexed(84),
                Color::Indexed(221),
                Color::Indexed(204),
            ],
            ColorMode::Basic => [
                Color::Black,
                Color::Black,
                Color::White,
                Color::Gray,
                Color::Cyan,
                Color::Magenta,
                Color::Green,
                Color::Yellow,
                Color::Red,
            ],
            ColorMode::Monochrome => [Color::Reset; 9],
        };
        Self {
            background: colors[0],
            panel: colors[1],
            text: colors[2],
            muted: colors[3],
            cyan: colors[4],
            violet: colors[5],
            green: colors[6],
            amber: colors[7],
            red: colors[8],
            monochrome: mode == ColorMode::Monochrome,
        }
    }

    pub fn base(self) -> Style {
        Style::default().fg(self.text).bg(self.background)
    }

    pub fn panel(self) -> Style {
        Style::default().fg(self.text).bg(self.panel)
    }

    pub fn muted(self) -> Style {
        self.panel().fg(self.muted)
    }

    pub fn accent(self) -> Style {
        self.panel().fg(self.cyan).add_modifier(Modifier::BOLD)
    }

    pub fn selected(self) -> Style {
        if self.monochrome {
            self.panel().add_modifier(Modifier::REVERSED)
        } else {
            self.panel().fg(self.cyan).add_modifier(Modifier::BOLD)
        }
    }
}
