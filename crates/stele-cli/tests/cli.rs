use std::process::Command;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_stele"))
}

#[test]
fn help_lists_the_supported_commands() {
    let output = binary().arg("--help").output().expect("run stele --help");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for command in ["start", "serve", "dev", "db", "healthcheck"] {
        assert!(
            stdout.contains(command),
            "missing command {command}:\n{stdout}"
        );
    }
}

#[test]
fn healthcheck_reports_unreachable_servers() {
    let output = binary()
        .args(["healthcheck", "--url", "http://127.0.0.1:1"])
        .output()
        .expect("run healthcheck");
    assert!(!output.status.success());
}

#[test]
fn start_requires_a_readable_config() {
    let output = binary()
        .args(["start", "--config", "/nonexistent/stele.toml", "--once"])
        .output()
        .expect("run start");
    assert!(!output.status.success());
}
