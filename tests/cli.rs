use serde_json::Value;
use std::process::Command;
use tempfile::tempdir;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_context-braid"))
}

#[test]
fn cli_runs_the_vertical_slice() {
    let directory = tempdir().unwrap();
    let ledger = directory.path().join("team-context.jsonl");
    let snapshot = directory.path().join("snapshot.json");

    assert!(binary()
        .arg("init")
        .arg(&ledger)
        .status()
        .unwrap()
        .success());
    assert!(binary()
        .arg("append")
        .arg(&ledger)
        .args([
            "--topic",
            "release",
            "--stance",
            "support",
            "--claim",
            "local checks passed",
            "--source",
            "ci://local/1",
            "--author",
            "builder",
        ])
        .status()
        .unwrap()
        .success());
    let verification = binary().arg("verify").arg(&ledger).output().unwrap();
    assert!(verification.status.success());
    let result: Value = serde_json::from_slice(&verification.stdout).unwrap();
    assert_eq!(result["valid"], true);
    assert_eq!(result["events"], 1);

    assert!(binary()
        .arg("snapshot")
        .arg(&ledger)
        .args(["--output", snapshot.to_str().unwrap()])
        .status()
        .unwrap()
        .success());
    assert!(snapshot.exists());
}

#[test]
fn cli_returns_nonzero_for_a_missing_ledger() {
    let directory = tempdir().unwrap();
    let result = binary()
        .arg("verify")
        .arg(directory.path().join("missing.jsonl"))
        .output()
        .unwrap();

    assert!(!result.status.success());
    assert!(String::from_utf8(result.stderr)
        .unwrap()
        .contains("ledger does not exist"));
}
