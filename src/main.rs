// Project: czech-surname-restorator
// File: src/main.rs
// Description: Streaming CSV command line processing with ordered parallel batches.
// Author: Jan Alexandr Kopřiva
// License: MIT

use chrono::Local;
use clap::Parser;
use czech_surname_restorator::{build_index, correct_name, index_key, Index};
use std::{
    collections::HashSet,
    error::Error,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
#[derive(Parser)]
#[command(
    about = "Restore diacritics in Czech surnames using a reference list.",
    disable_version_flag = true,
    infer_long_args = true,
    args_override_self = true
)]
struct Args {
    /// CSV file with the names to correct
    input: PathBuf,
    /// where to write the result (default: output_corrected.csv)
    #[arg(short, long, default_value = "output_corrected.csv")]
    output: PathBuf,
    /// reference surname list (default: surnames_all.csv)
    #[arg(short, long, default_value = "surnames_all.csv")]
    reference: PathBuf,
    /// column holding the full name
    #[arg(long, default_value = "Person_Name")]
    name_column: String,
    /// column holding the reference surnames
    #[arg(long, default_value = "column1")]
    reference_column: String,
    /// rows read at a time
    #[arg(long, default_value_t = 10_000, value_parser = positive)]
    chunk_size: usize,
    /// names per worker task
    #[arg(long, default_value_t = 1_000, value_parser = positive)]
    batch_size: usize,
    /// CPUs for workers (default: all)
    #[arg(long, value_parser = positive)]
    num_cpus: Option<usize>,
    /// also write logs here
    #[arg(long)]
    log_file: Option<PathBuf>,
}
fn positive(s: &str) -> std::result::Result<usize, String> {
    // Python's int() accepts Unicode decimal digits and underscores between digits.
    const DECIMAL_ZEROS: &[u32] = &[
        0x30, 0x660, 0x6f0, 0x7c0, 0x966, 0x9e6, 0xa66, 0xae6, 0xb66, 0xbe6, 0xc66, 0xce6, 0xd66,
        0xde6, 0xe50, 0xed0, 0xf20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80, 0x1a90,
        0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0, 0xa9f0, 0xaa50, 0xabf0,
        0xff10, 0x104a0, 0x10d30, 0x11066, 0x110f0, 0x11136, 0x111d0, 0x112f0, 0x11450, 0x114d0,
        0x11650, 0x116c0, 0x11730, 0x118e0, 0x11950, 0x11c50, 0x11d50, 0x11da0, 0x16a60, 0x16ac0,
        0x16b50, 0x1d7ce, 0x1d7d8, 0x1d7e2, 0x1d7ec, 0x1d7f6, 0x1e140, 0x1e2f0, 0x1e950, 0x1fbf0,
    ];
    let trimmed = s.trim().strip_prefix('+').unwrap_or(s.trim());
    let mut digits = String::new();
    let mut previous_digit = false;
    for c in trimmed.chars() {
        if c == '_' && previous_digit {
            previous_digit = false;
            continue;
        }
        let digit = DECIMAL_ZEROS
            .iter()
            .find_map(|&zero| (c as u32).checked_sub(zero).filter(|&n| n < 10));
        let Some(digit) = digit else {
            return Err("must be a positive integer".into());
        };
        digits.push(char::from(b'0' + digit as u8));
        previous_digit = true;
    }
    digits
        .parse::<usize>()
        .ok()
        .filter(|&n| n > 0 && previous_digit)
        .ok_or_else(|| "must be a positive integer".into())
}
struct Logger(Option<File>);
impl Logger {
    fn log(&mut self, level: &str, message: &str) {
        let line = format!(
            "{} - {level} - {message}\n",
            Local::now().format("%Y-%m-%d %H:%M:%S,%3f")
        );
        let _ = std::io::stderr().write_all(line.as_bytes());
        if let Some(file) = &mut self.0 {
            let _ = file.write_all(line.as_bytes());
        }
    }
}
fn is_na(s: &str) -> bool {
    matches!(
        s,
        "" | "#N/A"
            | "#N/A N/A"
            | "#NA"
            | "-1.#IND"
            | "-1.#QNAN"
            | "-NaN"
            | "-nan"
            | "1.#IND"
            | "1.#QNAN"
            | "<NA>"
            | "N/A"
            | "NA"
            | "NULL"
            | "NaN"
            | "None"
            | "n/a"
            | "nan"
            | "null"
    )
}
// Skip unquoted blank physical lines without discarding quoted whitespace fields.
struct BlankLines {
    input: BufReader<File>,
    pending: std::io::Cursor<Vec<u8>>,
    quoted: bool,
    field_start: bool,
    first: bool,
    after_quote: bool,
}
impl Read for BlankLines {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.pending.read(buffer)?;
            if n > 0 {
                return Ok(n);
            }
            let mut line = Vec::new();
            loop {
                let available = self.input.fill_buf()?;
                if available.is_empty() {
                    break;
                }
                let end = available.iter().position(|b| matches!(b, b'\r' | b'\n'));
                let count = end.map_or(available.len(), |i| i + 1);
                line.extend_from_slice(&available[..count]);
                self.input.consume(count);
                if end.is_some() {
                    break;
                }
            }
            if line.is_empty() {
                if self.quoted {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "CSV contains an unterminated quoted field",
                    ));
                }
                return Ok(0);
            }
            if self.first {
                self.first = false;
                if line.starts_with(&[0xef, 0xbb, 0xbf]) {
                    line.drain(..3);
                }
            }
            if !self.quoted
                && line
                    .iter()
                    .all(|c| matches!(c, b' ' | b'\t' | b'\r' | b'\n'))
            {
                continue;
            }
            for &byte in &line {
                if self.quoted {
                    if byte == b'"' {
                        self.quoted = false;
                        self.after_quote = true;
                    }
                } else if byte == b'"' && (self.field_start || self.after_quote) {
                    self.quoted = true;
                    self.field_start = false;
                    self.after_quote = false;
                } else {
                    self.field_start = matches!(byte, b',' | b'\n' | b'\r');
                    self.after_quote = false;
                }
            }
            self.pending = std::io::Cursor::new(line);
        }
    }
}
fn reader(path: &Path) -> Result<csv::Reader<BlankLines>> {
    let source = BlankLines {
        input: BufReader::new(File::open(path)?),
        pending: std::io::Cursor::new(Vec::new()),
        quoted: false,
        field_start: true,
        first: true,
        after_quote: false,
    };
    Ok(csv::ReaderBuilder::new().flexible(true).from_reader(source))
}
fn headers(reader: &mut csv::Reader<BlankLines>) -> Result<Vec<String>> {
    let raw = reader.headers()?.clone();
    if raw.is_empty() {
        return Err("No columns to parse from file".into());
    }
    let original: Vec<String> = raw
        .iter()
        .enumerate()
        .map(|(i, s)| {
            if s.is_empty() {
                format!("Unnamed: {i}")
            } else {
                s.into()
            }
        })
        .collect();
    let mut used = HashSet::new();
    let mut result = vec![String::new(); original.len()];
    for i in (0..original.len())
        .filter(|&i| !raw[i].is_empty())
        .chain((0..original.len()).filter(|&i| raw[i].is_empty()))
    {
        let s = &original[i];
        let mut name = s.clone();
        let mut n = 1;
        while used.contains(&name) || (name != *s && original.contains(&name)) {
            name = format!("{s}.{n}");
            n += 1;
        }
        used.insert(name.clone());
        result[i] = name;
    }
    Ok(result)
}
fn python_whitespace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}
fn split_name(name: &str) -> (String, String) {
    if is_na(name) {
        return (String::new(), String::new());
    }
    let given = if name.chars().next().is_some_and(python_whitespace) {
        ""
    } else {
        name.split(python_whitespace)
            .find(|s| !s.is_empty())
            .unwrap_or("")
    };
    // Python's dollar anchor also matches immediately before a final newline.
    let tail = name.strip_suffix('\n').unwrap_or(name);
    let surname = if tail.chars().next_back().is_some_and(python_whitespace) {
        ""
    } else {
        tail.split(python_whitespace)
            .rfind(|s| !s.is_empty())
            .unwrap_or("")
    };
    (given.into(), surname.into())
}
fn parallel_correct(
    names: &[String],
    index: &Index,
    all: &[String],
    batch: usize,
    cpus: usize,
) -> Result<Vec<String>> {
    let batches: Vec<_> = names.chunks(batch).collect();
    let workers = cpus.min(batches.len());
    if workers == 0 {
        return Ok(Vec::new());
    }
    std::thread::scope(|scope| {
        let handles = (0..workers)
            .map(|worker| {
                let batches = &batches;
                std::thread::Builder::new().spawn_scoped(scope, move || {
                    (worker..batches.len())
                        .step_by(workers)
                        .map(|i| {
                            (
                                i,
                                batches[i]
                                    .iter()
                                    .map(|s| correct_name(s, index, all))
                                    .collect::<Vec<_>>(),
                            )
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect::<std::io::Result<Vec<_>>>()?;
        let mut result = vec![Vec::new(); batches.len()];
        for handle in handles {
            for (i, values) in handle.join().map_err(|_| "Surname worker panicked")? {
                result[i] = values;
            }
        }
        Ok(result.into_iter().flatten().collect())
    })
}
fn same_path(a: &Path, b: &Path) -> bool {
    // File identity also catches hard links, whose canonical paths differ.
    if same_file::is_same_file(a, b).unwrap_or(false) {
        return true;
    }
    let absolute = |path: &Path| {
        path.canonicalize().ok().or_else(|| {
            Some(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."))
                    .canonicalize()
                    .ok()?
                    .join(path.file_name()?),
            )
        })
    };
    matches!((absolute(a), absolute(b)), (Some(a), Some(b)) if a == b)
}
fn temp_path(output: &Path) -> PathBuf {
    let mut name = output.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}
fn run(args: &Args, log: &mut Logger) -> Result<bool> {
    if !args.input.exists() {
        log.log(
            "ERROR",
            &format!("Input file does not exist: {}", args.input.display()),
        );
        return Ok(false);
    }
    if !args.reference.exists() {
        log.log(
            "ERROR",
            &format!(
                "Reference file does not exist: {}",
                args.reference.display()
            ),
        );
        return Ok(false);
    }
    if same_path(&args.input, &args.output) || same_path(&args.reference, &args.output) {
        return Err("Output must not overwrite the input or reference file".into());
    }
    let started = Instant::now();
    let mut reference = reader(&args.reference)?;
    let columns = headers(&mut reference)?;
    let Some(column) = columns.iter().position(|s| s == &args.reference_column) else {
        log.log(
            "ERROR",
            &format!(
                "Reference {} has no column '{}'. Found: [{}]",
                args.reference.display(),
                args.reference_column,
                columns
                    .iter()
                    .map(|s| format!("'{s}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
        return Ok(false);
    };
    let mut names = Vec::new();
    for row in reference.records() {
        let row = row?;
        if row.len() > columns.len() {
            return Err("Reference CSV has more fields than its header".into());
        }
        if let Some(s) = row.get(column) {
            if !is_na(s) {
                names.push(s.to_owned());
            }
        }
    }
    names.sort();
    log.log(
        "INFO",
        &format!("Loaded {} reference surnames", names.len()),
    );
    let index = build_index(&names);
    log.log("INFO", &format!("Built an index with {} keys", index.len()));
    let mut seen = HashSet::new();
    let mut all = Vec::new();
    for name in &names {
        let key = index_key(name);
        if seen.insert(key.clone()) {
            all.extend(index[&key].iter().cloned());
        }
    }
    let mut input = reader(&args.input)?;
    let mut columns = headers(&mut input)?;
    let original_len = columns.len();
    let Some(name_col) = columns.iter().position(|s| s == &args.name_column) else {
        log.log(
            "ERROR",
            &format!("Input has no column '{}'", args.name_column),
        );
        return Ok(false);
    };
    let mut positions = Vec::new();
    for name in [
        "given_name",
        "surname",
        "corrected_surname",
        "corrected_name",
    ] {
        let pos = columns.iter().position(|s| s == name).unwrap_or_else(|| {
            columns.push(name.into());
            columns.len() - 1
        });
        positions.push(pos);
    }
    let mut output = None;
    let mut records = input.records();
    let mut total = 0;
    loop {
        let mut chunk = Vec::new();
        for row in records.by_ref().take(args.chunk_size) {
            let row = row?;
            if row.len() > original_len {
                return Err("Input CSV has more fields than its header".into());
            }
            // Only the name column is NA-normalised; other columns (e.g. Country=NA) stay as-is.
            let mut row: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, s)| {
                    if i == name_col && is_na(s) {
                        String::new()
                    } else {
                        s.into()
                    }
                })
                .collect();
            row.resize(columns.len(), String::new());
            chunk.push(row);
        }
        if output.is_none() {
            let mut writer = csv::WriterBuilder::new()
                .terminator(if cfg!(windows) {
                    csv::Terminator::CRLF
                } else {
                    csv::Terminator::Any(b'\n')
                })
                .from_path(temp_path(&args.output))?;
            writer.write_record(&columns)?;
            output = Some(writer);
        }
        let output = output.as_mut().expect("output initialized above");
        if chunk.is_empty() {
            if total == 0 {
                log.log("INFO", "Wrote 0 rows so far");
            }
            output.flush()?;
            std::fs::rename(temp_path(&args.output), &args.output)?;
            break;
        }
        for row in &mut chunk {
            let (given, surname) = split_name(&row[name_col]);
            row[positions[0]] = given;
            row[positions[1]] = if name_col == positions[0] {
                split_name(&row[name_col]).1
            } else {
                surname
            };
        }
        let surnames = chunk
            .iter()
            .map(|r| r[positions[1]].clone())
            .collect::<Vec<_>>();
        let corrected = parallel_correct(
            &surnames,
            &index,
            &all,
            args.batch_size,
            args.num_cpus
                .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from)),
        )?;
        for (mut row, corrected) in chunk.into_iter().zip(corrected) {
            row[positions[2]] = corrected;
            row[positions[3]] = if row[positions[0]].is_empty() || row[positions[2]].is_empty() {
                String::new()
            } else {
                format!("{} {}", row[positions[0]], row[positions[2]])
            };
            output.write_record(row)?;
            total += 1;
        }
        output.flush()?;
        log.log("INFO", &format!("Wrote {total} rows so far"));
    }
    log.log(
        "INFO",
        &format!(
            "Wrote {total} rows to {} in {:.2} s",
            args.output.display(),
            started.elapsed().as_secs_f64()
        ),
    );
    Ok(true)
}
fn main() {
    let args = Args::parse();
    if args.log_file.as_ref().is_some_and(|path| {
        [&args.input, &args.reference, &args.output]
            .iter()
            .any(|other| same_path(path, other))
    }) {
        let _ = writeln!(
            std::io::stderr(),
            "Log file must differ from the input, reference and output files"
        );
        std::process::exit(1);
    }
    let file = args
        .log_file
        .as_ref()
        .map(|p| OpenOptions::new().create(true).append(true).open(p))
        .transpose();
    let mut log = match file {
        Ok(f) => Logger(f),
        Err(e) => {
            let _ = writeln!(std::io::stderr(), "{e}");
            std::process::exit(1)
        }
    };
    match run(&args, &mut log) {
        Ok(true) => (),
        Ok(false) => std::process::exit(1),
        Err(e) => {
            let _ = std::fs::remove_file(temp_path(&args.output));
            log.log("ERROR", &e.to_string());
            std::process::exit(1);
        }
    }
}
