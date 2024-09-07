#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release
#![allow(rustdoc::missing_crate_level_docs)] // it's an example

use std::borrow::Borrow;

use eframe::egui;
use egui::{CentralPanel, Context};
use egui_extras::{Column, TableBuilder};
use rusqlite::Connection;
use storage::{init_db, local_storage, Host};
mod storage;

fn main() -> eframe::Result {
    env_logger::init(); // Log to stderr (if you run with `RUST_LOG=debug`).
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([900.0, 840.0]),
        ..Default::default()
    };
    eframe::run_native(
        "TCPFS Client",
        options,
        Box::new(|cc| {
            // This gives us image support:
            egui_extras::install_image_loaders(&cc.egui_ctx);

            Ok(Box::<App>::default())
        }),
    )
}

struct App {
    con: Connection,
    selected_host: Host,
}

impl Default for App {
    fn default() -> Self {
        let con = local_storage();
        let selected_host = Host::default(); // Initialize the selected host with default values
        Self { con, selected_host }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        CentralPanel::default().show(ctx, |ui| {
            ui.label("Enter Host Details:");
            ui.horizontal(|ui| {
                // Edit the `host` field
                if let Some(ref mut host) = self.selected_host.host {
                    ui.label("Host:");
                    ui.text_edit_singleline(host);
                }

                // Edit the `port` field
                if let Some(ref mut port) = self.selected_host.port {
                    ui.label("Port:");
                    ui.text_edit_singleline(port);
                }

                // Edit the `bucket_id` field
                if let Some(ref mut bucket_id) = self.selected_host.bucket_id {
                    ui.label("Bucket ID:");
                    ui.text_edit_singleline(bucket_id);
                }
            });
            if ui.button("Save").clicked() {
                storage::create_host(&self.con, &self.selected_host).unwrap();
            };
            // Display the current values
            ui.label(format!(
                "Current Host: {:?}",
                self.selected_host.host.as_ref().unwrap_or(&String::new())
            ));
            ui.label(format!(
                "Current Port: {:?}",
                self.selected_host.port.as_ref().unwrap_or(&String::new())
            ));
            ui.label(format!(
                "Current Bucket ID: {:?}",
                self.selected_host
                    .bucket_id
                    .as_ref()
                    .unwrap_or(&String::new())
            ));
            TableBuilder::new(ui)
                .column(Column::auto())
                .column(Column::auto())
                .column(Column::auto())
                .header(10.0, |mut header| {
                    header.col(|ui| {
                        ui.heading("Bucket");
                    });
                    header.col(|ui| {
                        ui.heading("Host");
                    });
                    header.col(|ui| {
                        ui.heading("Port");
                    });
                })
                .body(|mut body| {
                    let saved_hosts = storage::get_all_hosts(&self.con).unwrap();
                    for h in saved_hosts.iter() {
                        let hh = h.clone();
                        body.row(10.0, |mut row| {
                            row.col(|ui| {
                                if ui.label(hh.bucket_id.unwrap_or_default()).clicked() {
                                    self.selected_host = hh.clone();
                                };
                            });
                            row.col(|ui| {
                                ui.label(hh.host.unwrap_or_default());
                            });
                            row.col(|ui| {
                                ui.label(hh.port.unwrap_or_default());
                            });
                        });
                    }
                });
        });
    }
}
