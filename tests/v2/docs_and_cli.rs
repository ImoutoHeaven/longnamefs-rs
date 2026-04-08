use clap::CommandFactory;

fn cli_long_help() -> String {
    let mut command = crate::Cli::command();
    let mut out = Vec::new();
    command.write_long_help(&mut out).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn about_string_no_longer_marks_v2_as_wip() {
    let command = crate::Cli::command();
    let about = command
        .get_about()
        .expect("CLI about text should be present");
    let about = about.to_string();
    let help = cli_long_help();

    assert!(
        !about.contains("WIP"),
        "CLI about text should not describe v2 as WIP: {about}"
    );
    assert!(
        !about.contains("compatible with the C longnamefs backend layout"),
        "CLI about text should not claim C backend compatibility: {about}"
    );
    assert!(
        about.contains("long file name shim"),
        "CLI about text should stay minimal and factual: {about}"
    );
    assert!(
        !help.contains("xattr+index, WIP"),
        "CLI help should not describe v2 as WIP: {help}"
    );
    assert!(
        help.contains("incompatible with v1/C backends"),
        "CLI help should describe v2 as incompatible with v1/C backends: {help}"
    );
}

#[test]
fn index_sync_help_no_longer_claims_always_sync() {
    let help = cli_long_help();

    assert!(
        !help.to_ascii_lowercase().contains("always sync"),
        "CLI help should not claim index-sync=always performs durable sync: {help}"
    );
    assert!(
        help.contains("always flush pending index work"),
        "CLI help should describe index-sync=always as immediate index flushing: {help}"
    );
}
