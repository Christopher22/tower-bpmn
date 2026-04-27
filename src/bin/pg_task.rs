use std::process::{Command, ExitCode};

fn run_script(args: &[String]) -> ExitCode {
    let status = Command::new("bash")
        .arg("./scripts/reset_test_postgres.sh")
        .args(args)
        .status();

    match status {
        Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
        Err(error) => {
            eprintln!("Failed to execute reset script: {error}");
            ExitCode::from(1)
        }
    }
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("reset-postgres") => run_script(&[]),
        Some("test-postgres") => {
            let mut script_args = vec!["--run-tests".to_string()];
            script_args.extend(args);
            run_script(&script_args)
        }
        _ => {
            eprintln!("Usage:");
            eprintln!("  cargo pg-reset");
            eprintln!("  cargo pg-test-postgres [test-filter]");
            ExitCode::from(2)
        }
    }
}
