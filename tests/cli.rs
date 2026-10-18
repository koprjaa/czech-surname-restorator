// Project: czech-surname-restorator
// File: tests/cli.rs
// Description: Offline CLI, CSV byte format, logging and failure regression tests.
// Author: Jan Alexandr Kopřiva
// License: MIT

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
    assert!(w.run(&["input.csv"]).status.success());
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
