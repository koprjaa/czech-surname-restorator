// Project: czech-surname-restorator
// File: tests/cli.rs
// Description: Offline CLI, CSV byte format, logging and failure regression tests.
// Author: Jan Alexandr Kopřiva
// License: AGPL-3.0-or-later
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        let p = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "cli-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&p).unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        fs::copy(fixture.join("reference.csv"), p.join("surnames_all.csv")).unwrap();
        fs::copy(fixture.join("input.csv"), p.join("input.csv")).unwrap();
        Self(p)
    }
    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_czech-surname-restorator"))
            .current_dir(&self.0)
            .args(args)
            .output()
            .unwrap()
    }
    fn write(&self, path: &str, data: &str) {
        fs::write(self.0.join(path), data).unwrap();
    }
    fn output(&self) -> String {
        fs::read_to_string(self.0.join("output_corrected.csv"))
            .unwrap()
            .replace("\r\n", "\n")
    }
}
impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[test]
fn defaults_and_csv_bytes() {
    let w = Workspace::new();
    let out = w.run(&["input.csv"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty());
    let expected = String::from_utf8(include_bytes!("fixtures/expected.csv").to_vec()).unwrap();
    let platform_expected = if cfg!(windows) {
        expected.replace("\n", "\r\n")
    } else {
        expected
    };
    assert_eq!(
        fs::read(w.0.join("output_corrected.csv")).unwrap(),
        platform_expected.as_bytes()
    );
    assert_eq!(
        w.output().as_bytes(),
        include_bytes!("fixtures/expected.csv")
    );
    let log = String::from_utf8(out.stderr).unwrap();
    for msg in [
        "Loaded 5 reference surnames",
        "Built an index with 4 keys",
        "Wrote 7 rows so far",
        "Wrote 7 rows to output_corrected.csv in",
    ] {
        assert!(log.contains(msg), "{log}");
    }
}
#[test]
fn chunks_parallel_order_and_logging() {
    let w = Workspace::new();
    for _ in 0..2 {
        let out = w.run(&[
            "input.csv",
            "--chunk-size",
            "2",
            "--batch-size",
            "1",
            "--num-cpus",
            "2",
            "--log-file",
            "run.log",
        ]);
        assert!(out.status.success());
        assert_eq!(
            w.output().as_bytes(),
            include_bytes!("fixtures/expected.csv")
        );
    }
    let log = fs::read_to_string(w.0.join("run.log")).unwrap();
    assert_eq!(log.matches("Loaded 5").count(), 2);
    assert_eq!(log.matches("rows so far").count(), 8);
}
#[test]
fn custom_columns_and_existing_outputs() {
    let w = Workspace::new();
    w.write(
        "custom.csv",
        "name,corrected_name,surname,given_name,corrected_surname\nJan Dvorak,old,old,old,old\n",
    );
    w.write("ref.csv", "surname\nDvořák\n");
    let out = w.run(&[
        "custom.csv",
        "-r",
        "ref.csv",
        "-o",
        "result.csv",
        "--name-column",
        "name",
        "--reference-column",
        "surname",
    ]);
    assert!(out.status.success());
    assert_eq!(fs::read_to_string(w.0.join("result.csv")).unwrap().replace("\r\n", "\n"),"name,corrected_name,surname,given_name,corrected_surname\nJan Dvorak,Jan Dvořák,Dvorak,Jan,Dvořák\n");
}
#[test]
fn errors_and_help() {
    let w = Workspace::new();
    for args in [
        vec![],
        vec!["--unknown"],
        vec!["input.csv", "--chunk-size", "0"],
        vec!["input.csv", "--batch-size", "-1"],
        vec!["input.csv", "--num-cpus", "0"],
    ] {
        assert_eq!(w.run(&args).status.code(), Some(2));
    }
    let help = w.run(&["--help"]);
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("--reference-column"));
    let out = w.run(&["missing.csv"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("Input file does not exist: missing.csv"));
    assert_eq!(
        w.run(&["input.csv", "-r", "missing.csv"]).status.code(),
        Some(1)
    );
    assert_eq!(
        w.run(&["input.csv", "--reference-column", "absent"])
            .status
            .code(),
        Some(1)
    );
    assert_eq!(
        w.run(&["input.csv", "--name-column", "absent"])
            .status
            .code(),
        Some(1)
    );
}
#[test]
fn empty_short_bom_and_multiline() {
    let w = Workspace::new();
    w.write("input.csv", "Person_Name,note\n");
    let out = w.run(&["input.csv"]);
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("Wrote 0 rows so far"));
    assert_eq!(
        w.output(),
        "Person_Name,note,given_name,surname,corrected_surname,corrected_name\n"
    );
    w.write("input.csv", "");
    assert_eq!(w.run(&["input.csv"]).status.code(), Some(1));
    w.write(
        "input.csv",
        "\u{feff}Person_Name,note\r\nJan Dvorak\r\n\r\n\"Petr\nKucera\",\"x\ny\"\r\n",
    );
    assert!(w.run(&["input.csv"]).status.success());
    assert_eq!(w.output(),"Person_Name,note,given_name,surname,corrected_surname,corrected_name\nJan Dvorak,,Jan,Dvorak,Dvořák,Jan Dvořák\n\"Petr\nKucera\",\"x\ny\",Petr,Kucera,Kučera,Petr Kučera\n");
}
#[test]
fn alias_protection_and_ragged_rows() {
    let w = Workspace::new();
    let original = fs::read(w.0.join("input.csv")).unwrap();
    assert_eq!(
        w.run(&["input.csv", "-o", "input.csv"]).status.code(),
        Some(1)
    );
    assert_eq!(fs::read(w.0.join("input.csv")).unwrap(), original);
    assert_eq!(
        w.run(&["input.csv", "-o", "surnames_all.csv"])
            .status
            .code(),
        Some(1)
    );
    w.write("input.csv", "Person_Name\nJan Dvorak,extra\n");
    assert_eq!(w.run(&["input.csv"]).status.code(), Some(1));
}

#[test]
fn blank_lines_quoted_spaces_duplicates_and_source_alias() {
    let w = Workspace::new();
    w.write(
        "input.csv",
        " \t\nPerson_Name,note,note,note.1\n \t\nJan Dvorak,a,b,c\n\" \",quoted,,\n",
    );
    let out = w.run(&["input.csv"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(w.output(), "Person_Name,note,note.2,note.1,given_name,surname,corrected_surname,corrected_name\nJan Dvorak,a,b,c,Jan,Dvorak,Dvořák,Jan Dvořák\n ,quoted,,,,,,\n");
    w.write("input.csv", "given_name\nJan Dvorak\n");
    assert!(w
        .run(&["input.csv", "--name-column", "given_name"])
        .status
        .success());
    assert_eq!(
        w.output(),
        "given_name,surname,corrected_surname,corrected_name\nJan,Jan,Jan,Jan Jan\n"
    );
    w.write("input.csv", "Person_Name,\"Unnamed: 1\",\nJan Dvorak,a,b\n");
    assert!(w.run(&["input.csv"]).status.success());
    assert!(w.output().starts_with("Person_Name,Unnamed: 1,Unnamed: 2,"));
}

#[test]
fn malformed_utf8_and_unterminated_quote_fail() {
    let w = Workspace::new();
    w.write("input.csv", "Person_Name\n\"Jan Dvorak\n");
    assert_eq!(w.run(&["input.csv"]).status.code(), Some(1));
    fs::write(w.0.join("input.csv"), b"Person_Name\nJan \xff\n").unwrap();
    assert_eq!(w.run(&["input.csv"]).status.code(), Some(1));
}

#[test]
fn nul_preservation_and_empty_header_collision() {
    let w = Workspace::new();
    w.write("input.csv", "Person_Name,note\nJan Dvorak,a\0b\n");
    assert!(w.run(&["input.csv"]).status.success());
    assert!(w.output().contains("a\0b"));
    w.write("input.csv", "Person_Name,,Unnamed: 1\nJan Dvorak,a,b\n");
    assert!(w.run(&["input.csv"]).status.success());
    assert!(w
        .output()
        .starts_with("Person_Name,Unnamed: 1.1,Unnamed: 1,"));
}

#[test]
fn repeated_arguments_and_abbreviations() {
    let w = Workspace::new();
    assert!(w
        .run(&[
            "input.csv",
            "-o",
            "unused.csv",
            "-o",
            "result.csv",
            "--chunk-s",
            "2"
        ])
        .status
        .success());
    assert!(!w.0.join("unused.csv").exists());
    assert!(w.0.join("result.csv").exists());
}

#[test]
fn hard_link_outputs_preserve_both_sources() {
    for source in ["input.csv", "surnames_all.csv"] {
        let w = Workspace::new();
        let original = fs::read(w.0.join(source)).unwrap();
        fs::hard_link(w.0.join(source), w.0.join("alias.csv")).unwrap();
        let out = w.run(&["input.csv", "-o", "alias.csv"]);
        assert_eq!(out.status.code(), Some(1));
        assert_eq!(fs::read(w.0.join(source)).unwrap(), original);
    }
}

#[test]
fn log_aliases_are_rejected_before_any_write() {
    for source in ["input.csv", "surnames_all.csv", "output_corrected.csv"] {
        for hard_link in [false, true] {
            let w = Workspace::new();
            if source == "output_corrected.csv" {
                w.write(source, "existing output");
            }
            let original = fs::read(w.0.join(source)).unwrap();
            let log = if hard_link {
                fs::hard_link(w.0.join(source), w.0.join("alias.csv")).unwrap();
                "alias.csv"
            } else {
                source
            };
            assert_eq!(
                w.run(&["input.csv", "--log-file", log]).status.code(),
                Some(1)
            );
            assert_eq!(fs::read(w.0.join(source)).unwrap(), original);
        }
    }
    let w = Workspace::new();
    assert_eq!(
        w.run(&["input.csv", "--log-file", "./output_corrected.csv"])
            .status
            .code(),
        Some(1)
    );
    assert!(!w.0.join("output_corrected.csv").exists());
}

#[test]
fn invalid_first_chunk_preserves_existing_output() {
    let w = Workspace::new();
    for data in [
        b"Person_Name\n\"unterminated".as_slice(),
        b"Person_Name\nJan \xff\n",
        b"Person_Name\nJan Dvorak,extra\n",
    ] {
        fs::write(w.0.join("input.csv"), data).unwrap();
        w.write("output_corrected.csv", "existing output");
        assert_eq!(w.run(&["input.csv"]).status.code(), Some(1));
        assert_eq!(w.output(), "existing output");
    }
}

#[test]
fn python_integer_syntax_and_validation() {
    let w = Workspace::new();
    for value in ["1_0", " 2 ", "٢", "+2", "２", "१२"] {
        let out = w.run(&["input.csv", "--chunk-size", value]);
        assert!(
            out.status.success(),
            "{value:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            w.output().as_bytes(),
            include_bytes!("fixtures/expected.csv")
        );
    }
    for value in [
        "1__0",
        "_1",
        "1_",
        "+ 2",
        "",
        "-2",
        "0",
        "99999999999999999999999999999999999",
    ] {
        assert_eq!(
            w.run(&["input.csv", "--chunk-size", value]).status.code(),
            Some(2),
            "{value:?}"
        );
    }
}

#[test]
fn cr_blank_lines_and_quoted_cr_are_preserved() {
    let w = Workspace::new();
    w.write("input.csv", "Person_Name,note\r \t\rJan Dvorak,x\r");
    assert!(w.run(&["input.csv"]).status.success());
    assert_eq!(w.output(), "Person_Name,note,given_name,surname,corrected_surname,corrected_name\nJan Dvorak,x,Jan,Dvorak,Dvořák,Jan Dvořák\n");
    w.write("input.csv", "Person_Name,note\r\"Jan\r \t\rDvorak\",x\r");
    assert!(w.run(&["input.csv"]).status.success());
    assert!(w
        .output()
        .contains("\"Jan\r \t\rDvorak\",x,Jan,Dvorak,Dvořák,Jan Dvořák"));
}

#[test]
fn many_chunks_and_large_unicode_field() {
    let w = Workspace::new();
    let mut data = String::from("Person_Name,note\n");
    for i in 0..20_001 {
        data.push_str(&format!("Jan Dvorak,{i}\n"));
    }
    w.write("input.csv", &data);
    assert!(w
        .run(&[
            "input.csv",
            "--chunk-size",
            "5000",
            "--batch-size",
            "123",
            "--num-cpus",
            "2"
        ])
        .status
        .success());
    let text = w.output();
    assert_eq!(text.lines().count(), 20_002);
    assert!(text.ends_with("Jan Dvorak,20000,Jan,Dvorak,Dvořák,Jan Dvořák\n"));
    let name = "ř".repeat(200_000);
    w.write("surnames_all.csv", &format!("column1\n{name}\n"));
    w.write("input.csv", &format!("Person_Name\nJan {name}\n"));
    assert!(w.run(&["input.csv"]).status.success());
    let mut reader = csv::Reader::from_path(w.0.join("output_corrected.csv")).unwrap();
    let row = reader.records().next().unwrap().unwrap();
    assert_eq!(&row[3], name);
}

#[test]
fn io_errors_and_reference_failures_exit_cleanly() {
    let w = Workspace::new();
    for args in [
        vec!["input.csv", "-o", "missing/result.csv"],
        vec!["input.csv", "--log-file", "missing/run.log"],
        vec!["."],
        vec!["input.csv", "-r", "."],
    ] {
        let out = w.run(&args);
        assert_eq!(out.status.code(), Some(1));
        assert!(!String::from_utf8_lossy(&out.stderr).contains("panicked"));
    }
    for reference in [
        b"".as_slice(),
        b"other\nDvorak\n",
        b"column1\n\"unterminated",
        b"column1\n\xff\n",
    ] {
        fs::write(w.0.join("surnames_all.csv"), reference).unwrap();
        let out = w.run(&["input.csv"]);
        assert_eq!(out.status.code(), Some(1));
        assert!(!String::from_utf8_lossy(&out.stderr).contains("panicked"));
    }
    w.write("surnames_all.csv", "column1\n");
    assert!(w.run(&["input.csv"]).status.success());
    let mut reader = csv::Reader::from_path(w.0.join("output_corrected.csv")).unwrap();
    assert_eq!(&reader.records().next().unwrap().unwrap()[5], "Jan Dvorak");
}

#[cfg(unix)]
#[test]
fn symlink_aliases_are_rejected() {
    let w = Workspace::new();
    std::os::unix::fs::symlink(w.0.join("input.csv"), w.0.join("link.csv")).unwrap();
    let original = fs::read(w.0.join("input.csv")).unwrap();
    assert_eq!(
        w.run(&["input.csv", "-o", "link.csv"]).status.code(),
        Some(1)
    );
    assert_eq!(
        w.run(&["input.csv", "--log-file", "link.csv"])
            .status
            .code(),
        Some(1)
    );
    assert_eq!(fs::read(w.0.join("input.csv")).unwrap(), original);
}

#[cfg(unix)]
#[test]
fn closed_log_stream_does_not_panic() {
    use std::{
        os::{fd::OwnedFd, unix::net::UnixStream},
        process::Stdio,
    };
    let w = Workspace::new();
    for (args, code) in [
        (vec!["input.csv"], 0),
        (vec!["input.csv", "--log-file", "input.csv"], 1),
        (vec!["input.csv", "--log-file", "missing/run.log"], 1),
        (vec!["missing.csv"], 1),
    ] {
        let (reader, writer) = UnixStream::pair().unwrap();
        drop(reader);
        let out = Command::new(env!("CARGO_BIN_EXE_czech-surname-restorator"))
            .current_dir(&w.0)
            .args(args)
            .stderr(Stdio::from(OwnedFd::from(writer)))
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(code));
    }
    assert_eq!(
        w.output().as_bytes(),
        include_bytes!("fixtures/expected.csv")
    );
}

#[test]
fn frozen_python_cli_oracle() {
    fn decode_hex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect()
    }
    let mut fixtures =
        csv::Reader::from_reader(include_bytes!("fixtures/cli-oracle.csv").as_slice());
    for row in fixtures.records() {
        let row = row.unwrap();
        let w = Workspace::new();
        fs::write(w.0.join("input.csv"), decode_hex(&row[1])).unwrap();
        fs::write(w.0.join("surnames_all.csv"), decode_hex(&row[2])).unwrap();
        let out = w.run(&["input.csv"]);
        assert_eq!(
            out.status.code(),
            Some(row[5].parse::<i32>().unwrap()),
            "{}: {}",
            &row[0],
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.stdout.is_empty());
        if row[6] == *"-" {
            assert!(!w.0.join("output_corrected.csv").exists(), "{}", &row[0]);
        } else {
            let expected = decode_hex(&row[6]);
            // Compare records so Windows record terminators do not affect embedded newlines.
            let records = |bytes: &[u8]| {
                csv::ReaderBuilder::new()
                    .has_headers(false)
                    .flexible(true)
                    .from_reader(bytes)
                    .records()
                    .map(|r| r.unwrap())
                    .collect::<Vec<_>>()
            };
            let actual = fs::read(w.0.join("output_corrected.csv")).unwrap();
            assert_eq!(records(&actual), records(&expected), "{}", &row[0]);
        }
    }
}

#[test]
fn later_invalid_chunk_leaves_no_partial_output() {
    let w = Workspace::new();
    fs::write(
        w.0.join("input.csv"),
        b"Person_Name\nJan Dvorak\nPetr \xff\n",
    )
    .unwrap();
    let out = w.run(&["input.csv", "--chunk-size", "1"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(!w.0.join("output_corrected.csv").exists());
    assert!(!w.0.join("output_corrected.csv.tmp").exists());
}

#[test]
fn uppercase_surnames_are_restored_and_other_columns_keep_na() {
    let w = Workspace::new();
    w.write("surnames_all.csv", "column1\nNovák\n");
    w.write("in.csv", "Person_Name,Country\nJan NOVAK,NA\n");
    let out = w.run(&["in.csv"]);
    assert!(out.status.success());
    let text = w.output();
    assert!(text.contains("Jan NOVÁK"), "{text}");
    assert!(text.contains(",NA,"), "{text}");
}
#[test]
fn failed_run_keeps_previous_output() {
    let w = Workspace::new();
    w.write("output_corrected.csv", "old\n");
    w.write("in.csv", "Person_Name\nJan Novak\nJan Novak,extra\n");
    let out = w.run(&["in.csv"]);
    assert!(!out.status.success());
    assert_eq!(w.output(), "old\n");
    assert!(!w.0.join("output_corrected.csv.tmp").exists());
}
