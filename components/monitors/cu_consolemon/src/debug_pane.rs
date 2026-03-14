use crate::UI;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::prelude::Stylize;
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph};
use std::collections::VecDeque;
use std::io::Read;
use std::sync::atomic::{AtomicU16, Ordering};

#[derive(Debug)]
pub struct DebugLog {
    pub(crate) debug_log: VecDeque<StyledLine>,
    pub(crate) max_rows: AtomicU16,
}

#[derive(Clone, Debug)]
pub struct StyledRun {
    /// Inclusive start / exclusive end in character indices
    pub start: usize,
    pub end: usize,
    pub style: Style,
}

#[derive(Clone, Debug)]
pub struct StyledLine {
    pub text: String,
    pub runs: Vec<StyledRun>,
}

impl DebugLog {
    pub fn new(max_lines: u16) -> Self {
        Self {
            debug_log: VecDeque::new(),
            max_rows: AtomicU16::new(max_lines),
        }
    }

    pub fn push_line(&mut self, line: StyledLine) {
        if line.text.is_empty() {
            return;
        }
        self.debug_log.push_back(line);
        let max_row = self.max_rows.load(Ordering::SeqCst) as usize;
        while self.debug_log.len() > max_row {
            self.debug_log.pop_front();
        }
    }

    pub fn lines(&self) -> Vec<StyledLine> {
        self.debug_log.iter().cloned().collect()
    }
}

pub trait UIExt {
    fn update_debug_output(&mut self);

    fn draw_debug_output(&mut self, f: &mut Frame, area: Rect);
}

impl UIExt for UI {
    fn update_debug_output(&mut self) {
        if let Some(debug_output) = self.debug_output.as_mut() {
            if let Some(stdout_redirect) = self.stdout_redirect.as_mut() {
                let mut buffer = String::new();
                let _ = stdout_redirect.read_to_string(&mut buffer);
                if !buffer.is_empty() {
                    for line in buffer.lines() {
                        debug_output.push_line(StyledLine {
                            text: line.to_string(),
                            runs: vec![],
                        });
                    }
                }
            }
            if let Some(error_redirect) = self.error_redirect.as_mut() {
                let mut error_buffer = String::new();
                let _ = error_redirect.read_to_string(&mut error_buffer);
                if !error_buffer.is_empty() {
                    for line in error_buffer.lines() {
                        debug_output.push_line(StyledLine {
                            text: line.to_string(),
                            runs: vec![StyledRun {
                                start: 0,
                                end: line.chars().count(),
                                style: Style::default().fg(Color::Red),
                            }],
                        });
                    }
                }
            }
        }
    }

    fn draw_debug_output(&mut self, f: &mut Frame, area: Rect) {
        if let Some(debug_output) = self.debug_output.as_mut() {
            let block = Block::default()
                .title(" Output ")
                .title_bottom(format!("{} log entries", debug_output.debug_log.len()))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded);
            let inner = block.inner(area);
            self.debug_output_area = Some(inner);
            self.debug_output_lines = debug_output.lines();
            self.debug_output_visible_offset = self
                .debug_output_lines
                .len()
                .saturating_sub(inner.height as usize);
            if let Some((start, end)) = self.debug_selection.range()
                && (start.row >= self.debug_output_lines.len()
                    || end.row >= self.debug_output_lines.len())
            {
                self.debug_selection.clear();
            }

            let p = Paragraph::new(self.build_debug_output_text(inner)).block(block);
            f.render_widget(p, area);
        } else {
            self.debug_output_area = None;
            self.debug_output_visible_offset = 0;
            self.debug_output_lines.clear();
            self.debug_selection.clear();

            let p = Paragraph::new("Debug pane feature is disabled".italic());
            f.render_widget(p, area);
        }
    }
}
