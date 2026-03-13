use crate::{copperlists_reader, keyframes_reader, structlog_reader};
use cu29::prelude::UnifiedLoggerRead;
use cu29::prelude::*;
use cu29::{CopperListTuple, CuResult};
use num_format::{Locale, ToFormattedString};
use std::io::Cursor;

pub(crate) fn check<P>(dl: &mut UnifiedLoggerRead, verbose: u8) -> Option<CuResult<()>>
where
    P: CopperListTuple + MatchingTasks,
{
    let header = dl.raw_main_header();

    if verbose > 0 {
        println!("Main header: \n{header}");
    }
    let mut overall_first_ts: OptionCuTime = OptionCuTime::none();
    let mut last_ts: OptionCuTime = OptionCuTime::none();
    let mut last_cl = 0;
    let mut keyframes = 0;
    let mut useful_size: usize = 0;
    let mut structured_log_size: usize = 0;
    let mut cls_size: usize = 0;
    let mut kfs_size: usize = 0;
    let mut sl_entries: usize = 0;

    let result = loop {
        // for _ in 0..4 {
        let section = dl.raw_read_section();
        match section {
            Ok((header, content)) => {
                useful_size += content.len();

                if verbose > 0 {
                    println!("Section: \n{header}");
                }

                match header.entry_type {
                    UnifiedLogType::StructuredLogLine => {
                        structured_log_size += content.len();
                        let mut reader: Cursor<Vec<u8>> = Cursor::new(content);
                        let iter = structlog_reader(&mut reader);
                        for entry in iter {
                            sl_entries += 1;
                            if entry.is_err() {
                                println!("Struct log #{sl_entries} is corrupted: {entry:?}");
                            }
                        }
                    }
                    UnifiedLogType::CopperList => {
                        cls_size += content.len();

                        let task_ids = P::get_all_task_ids();
                        let mut reader: Cursor<Vec<u8>> = Cursor::new(content);
                        let iter = copperlists_reader::<P>(&mut reader);
                        let mut first_cl = 0;
                        let mut first_ts: OptionCuTime = OptionCuTime::none();
                        for entry in iter {
                            last_cl = entry.id;
                            if first_ts.is_none() {
                                first_cl = entry.id;
                                let msgs = entry.cumsgs();
                                let first_msg =
                                    msgs.first().expect("Empty copperlist");
                                first_ts = first_msg.metadata().process_time().start;
                                if first_ts.is_none() {
                                    let task_name =
                                        task_ids.first().copied().unwrap_or("<unknown>");
                                    println!(
                                        "  Warning: CL#{} msg[0] ({task_name}) has no process_time.start",
                                        entry.id
                                    );
                                    for (i, (msg, name)) in
                                        msgs.iter().zip(task_ids.iter()).enumerate()
                                    {
                                        let pt = msg.metadata().process_time();
                                        println!(
                                            "    msg[{i}] task={name} start={} end={}",
                                            pt.start, pt.end
                                        );
                                    }
                                }
                                if overall_first_ts.is_none() {
                                    overall_first_ts = first_ts;
                                }
                            }
                            let msgs = entry.cumsgs();
                            let last_msg = *msgs.last().expect("Empty copperlist");
                            let candidate = last_msg.metadata().process_time().end;
                            if candidate.is_none() && verbose > 0 {
                                let task_name =
                                    task_ids.last().copied().unwrap_or("<unknown>");
                                println!(
                                    "  Warning: CL#{} last msg ({task_name}) has no process_time.end",
                                    entry.id
                                );
                            }
                            last_ts = candidate;
                        }
                        if verbose > 0 {
                            println!(
                                "    CopperLists => OK (id range: [{first_cl}-{last_cl}] timerange: [{first_ts}-{last_ts}])"
                            );
                        }
                    }
                    UnifiedLogType::FrozenTasks => {
                        kfs_size += content.len();
                        let mut reader: Cursor<Vec<u8>> = Cursor::new(content);
                        let iter = keyframes_reader(&mut reader);
                        for entry in iter {
                            keyframes += 1;
                            if verbose > 0 {
                                println!(
                                    "    Keyframe CL/ts: {}/{} ",
                                    entry.culistid, entry.timestamp
                                );
                            }
                        }
                    }
                    UnifiedLogType::LastEntry => {
                        if verbose > 0 {
                            println!("Last Entry / EOF.");
                            println!();
                        }
                        break Ok(());
                    }
                    UnifiedLogType::Empty => {
                        println!("Error: Found an empty / Uninitialized section");
                    }
                }
            }
            Err(e) => {
                println!("Failed to read section: {e}");
                break Err(e);
            }
        }
    };
    if result.is_ok() {
        println!("The log checked out OK.");
    } else {
        println!("** The log is corrupted.");
    }

    let l = &Locale::en;
    println!("        === Statistics ===");

    match (
        Option::<CuTime>::from(overall_first_ts),
        Option::<CuTime>::from(last_ts),
    ) {
        (Some(t_first), Some(t_last)) => {
            let total_time: CuDuration = t_last - t_first;
            let cl_rate = if last_cl != 0 {
                let cl_time = total_time / last_cl;
                1_000_000_000f64 / (cl_time.as_nanos() as f64)
            } else {
                0.0
            };
            let kf_rate = if keyframes != 0 {
                let kf_time = total_time / keyframes as u64;
                1_000_000_000f64 / (kf_time.as_nanos() as f64)
            } else {
                0.0
            };
            let bytes_per_sec = useful_size as f64 * 1e9 / total_time.as_nanos() as f64;
            let mib_per_sec = bytes_per_sec / (1024.0 * 1024.0);

            println!("  Total time       -> {total_time}");
            println!(
                "  Total used size  -> {} bytes",
                useful_size.to_formatted_string(l)
            );
            println!("  Logging rate     -> {mib_per_sec:.02} MiB/s (effective)");
            println!();
            println!("  # of CL          -> {}", last_cl.to_formatted_string(l));
            println!(
                "  CL rate          -> {}.{:02} Hz",
                (cl_rate.trunc() as u64).to_formatted_string(&Locale::en),
                (cl_rate.fract() * 100.0).round() as u64
            );
            println!(
                "  CL total size    -> {} bytes",
                cls_size.to_formatted_string(l)
            );
            println!();
            println!("  # of Keyframes   -> {}", keyframes.to_formatted_string(l));
            println!("  KF rate          -> {kf_rate:.2} Hz");
            println!(
                "  KF total size    -> {} bytes",
                kfs_size.to_formatted_string(l)
            );
            println!();
            println!(
                "  # of SL entries  -> {}",
                sl_entries.to_formatted_string(l)
            );
            println!(
                "  SL total size    -> {} bytes",
                structured_log_size.to_formatted_string(l)
            );
        }
        (first, last) => {
            println!(
                "  Warning: no valid timestamps found (first={:?}, last={:?}) — no tasks ran or all cycles were skipped.",
                first.map(|t| t.as_nanos()),
                last.map(|t| t.as_nanos()),
            );
            println!(
                "  Total used size  -> {} bytes",
                useful_size.to_formatted_string(l)
            );
            println!("  # of CL          -> {}", last_cl.to_formatted_string(l));
            println!(
                "  CL total size    -> {} bytes",
                cls_size.to_formatted_string(l)
            );
            println!("  # of Keyframes   -> {}", keyframes.to_formatted_string(l));
            println!(
                "  KF total size    -> {} bytes",
                kfs_size.to_formatted_string(l)
            );
            println!(
                "  # of SL entries  -> {}",
                sl_entries.to_formatted_string(l)
            );
            println!(
                "  SL total size    -> {} bytes",
                structured_log_size.to_formatted_string(l)
            );
        }
    }

    None
}
