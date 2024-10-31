use dataset_handler::{create_reader_from_path, get_reader_metadata};
use eframe::{egui, App, Frame};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::io::Write;

mod constants;
mod dataset_handler;
use constants::PATHS;
//21 - 22 - 23 - 
const CACHE_SIZE: usize = 1000;
const LAST_POSITION_FILE: &str = "last_position.json";
const GLOBAL_JSON_FILE: &str = "global_samples.json";
const TARGET_DATASET: &str = PATHS[3];

#[derive(Serialize, Deserialize)]
struct LastPosition {
    current_row: usize,
}

#[derive(Serialize, Deserialize)]
struct GlobalSample {
    index: usize,
    dataset: String,
    sample: Vec<(String, String)>,
}

struct ParquetViewer {
    reader: Arc<SerializedFileReader<File>>,
    metadata: ParquetMetaData,
    current_row: usize,
    total_rows: i64,
    cached_rows: Vec<Vec<(String, String)>>,
    cache_start: usize,
    should_exit: bool,
    jump_to_row: String,
    current_dataset: String,
}

impl ParquetViewer {
    fn new() -> Self {
        let reader = Arc::new(create_reader_from_path(TARGET_DATASET));
        let metadata = get_reader_metadata(&reader);
        let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();

        let current_row = Self::load_last_position().unwrap_or(0);

        Self {
            reader,
            metadata,
            current_row,
            total_rows,
            cached_rows: Vec::new(),
            cache_start: 0,
            should_exit: false,
            jump_to_row: String::new(),
            current_dataset: TARGET_DATASET.to_string(),
        }
    }

    fn load_last_position() -> Option<usize> {
        if Path::new(LAST_POSITION_FILE).exists() {
            let file = File::open(LAST_POSITION_FILE).ok()?;
            let last_position: LastPosition = serde_json::from_reader(file).ok()?;
            Some(last_position.current_row)
        } else {
            None
        }
    }

    fn save_last_position(&self) {
        let last_position = LastPosition {
            current_row: self.current_row,
        };
        let file = File::create(LAST_POSITION_FILE).unwrap();
        serde_json::to_writer(file, &last_position).unwrap();
        println!("Saved last position: {}", self.current_row);
    }

    fn ensure_cache(&mut self) {
        let cache_start = self.current_row - (self.current_row % CACHE_SIZE);
        if cache_start != self.cache_start || self.cached_rows.is_empty() {
            self.update_cache(cache_start);
        }
    }

    fn update_cache(&mut self, start: usize) {
        self.cached_rows.clear();
        self.cache_start = start;
        let end = (start + CACHE_SIZE).min(self.total_rows as usize);

        let file_metadata = self.metadata.file_metadata();
        let schema = file_metadata.schema();

        let mut current_row = 0;
        for (group_index, row_group) in self.metadata.row_groups().iter().enumerate() {
            let group_rows = row_group.num_rows() as usize;
            if current_row + group_rows > start {
                let row_group_reader = self.reader.get_row_group(group_index).unwrap();
                let mut row_iter = row_group_reader.get_row_iter(Some(schema.clone())).unwrap();

                let skip = start.saturating_sub(current_row);
                for _ in 0..skip {
                    row_iter.next();
                }

                while self.cached_rows.len() < CACHE_SIZE && current_row < end {
                    if let Some(Ok(row)) = row_iter.next() {
                        let mut row_data = Vec::new();
                        for (i, field) in schema.get_fields().iter().enumerate() {
                            let value = match row.get_string(i) {
                                Ok(s) => s.to_string(),
                                Err(_) => format!("{:?}", row.get_map(i)),
                            };
                            row_data.push((field.name().to_string(), value));
                        }
                        self.cached_rows.push(row_data);
                    }
                    current_row += 1;
                }

                if current_row >= end {
                    break;
                }
            }
            current_row += group_rows;
        }
    }

    fn get_row_data(&mut self, index: usize) -> Vec<(String, String)> {
        self.ensure_cache();
        let cache_index = index - self.cache_start;
        self.cached_rows[cache_index].clone()
    }

    fn save_to_global_json(&mut self) {
        let sample = GlobalSample {
            index: self.current_row,
            dataset: self.current_dataset.clone(),
            sample: self.get_row_data(self.current_row),
        };

        let json = serde_json::to_string(&sample).unwrap();

        let file_exists = Path::new(GLOBAL_JSON_FILE).exists();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(GLOBAL_JSON_FILE)
            .unwrap();

        if file_exists {
            let metadata = file.metadata().unwrap();
            if metadata.len() > 0 {
                writeln!(file, ",").unwrap();
            }
        } else {
            writeln!(file, "[").unwrap();
        }

        write!(file, "{}", json).unwrap();

        println!("Saved sample to global JSON file");
    }

    fn close_global_json_file(&self) {
        if Path::new(GLOBAL_JSON_FILE).exists() {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .append(true)
                .open(GLOBAL_JSON_FILE)
                .unwrap();

            writeln!(file, "]").unwrap();
            println!("Closed global JSON file");
        }
    }
}

impl App for ParquetViewer {
    fn update(&mut self, ctx: &egui::Context, frame: &mut Frame) {
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("YUM DATASETS");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Exit").clicked() {
                        self.should_exit = true;
                    }
                });
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("Previous").clicked() && self.current_row > 0 {
                    self.current_row -= 1;
                }
                ui.label(format!(
                    "Row {} of {}",
                    self.current_row + 1,
                    self.total_rows
                ));
                if ui.button("Next").clicked() && (self.current_row as i64) < self.total_rows - 1 {
                    self.current_row += 1;
                }

                ui.add_space(20.0);

                ui.label("Jump to row:");
                let response = ui.text_edit_singleline(&mut self.jump_to_row);
                if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                    if let Ok(row) = self.jump_to_row.parse::<usize>() {
                        if row > 0 && (row as i64) <= self.total_rows {
                            self.current_row = row - 1;
                        }
                    }
                    self.jump_to_row.clear();
                }
                if ui.button("Go").clicked() {
                    if let Ok(row) = self.jump_to_row.parse::<usize>() {
                        if row > 0 && (row as i64) <= self.total_rows {
                            self.current_row = row - 1;
                        }
                    }
                    self.jump_to_row.clear();
                }

                ui.add_space(20.0);

                if ui.button("Save to Global JSON").clicked() {
                    self.save_to_global_json();
                }
            });

            ui.separator();

            egui::ScrollArea::both().show(ui, |ui| {
                let row_data = self.get_row_data(self.current_row);
                for (column, value) in row_data {
                    ui.vertical(|ui| {
                        ui.label(format!("{}: ", column));
                        ui.add(
                            egui::TextEdit::multiline(&mut value.as_str())
                                .desired_width(f32::INFINITY)
                                .font(egui::TextStyle::Monospace),
                        );
                    });
                    ui.add_space(4.0);
                }
            });
        });

        if self.should_exit {
            frame.close();
        }
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        self.save_last_position();
        self.close_global_json_file();
    }
}

fn main() {
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Yum-Labs",
        options,
        Box::new(|_cc| Box::new(ParquetViewer::new())),
    )
    .unwrap();
}
