use clap::CommandFactory;

fn cli_long_help() -> String {
    let mut command = crate::Cli::command();
    let mut out = Vec::new();
    command.write_long_help(&mut out).unwrap();
    String::from_utf8(out).unwrap()
}

fn readme_text() -> &'static str {
    include_str!("../../README.md")
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
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

#[test]
fn cli_help_describes_exclusive_writer_v2_backend_lock() {
    let help = normalize_whitespace(&cli_long_help());

    assert!(
        help.contains("exclusive-writer"),
        "CLI help should describe the v2 exclusive-writer contract: {help}"
    );
    assert!(
        help.contains(".ln2_fs_lock"),
        "CLI help should mention the v2 backend lock file: {help}"
    );
}

#[test]
fn cli_help_describes_stable_long_object_backend_names() {
    let help = normalize_whitespace(&cli_long_help());

    assert!(
        help.contains(".__ln2_obj_<id>"),
        "CLI help should describe stable long-object backend names: {help}"
    );
    assert!(
        help.contains("legacy hash-derived"),
        "CLI help should mention legacy hash-derived backend rejection: {help}"
    );
}

#[test]
fn cli_help_describes_long_rename_no_replace_and_hardlink_rejection() {
    let help = normalize_whitespace(&cli_long_help());

    assert!(
        help.contains("strict no-replace"),
        "CLI help should describe strict no-replace long rename semantics: {help}"
    );
    assert!(
        help.contains("EEXIST"),
        "CLI help should mention EEXIST for long rename conflicts: {help}"
    );
    assert!(
        help.contains("EPERM"),
        "CLI help should mention EPERM for long hardlink rejection: {help}"
    );
}

#[test]
fn cli_help_describes_v2_durability_as_transaction_driven_not_index_driven() {
    let help = normalize_whitespace(&cli_long_help());

    assert!(
        help.contains("recoverable acceleration structures"),
        "CLI help should describe index/journal as recoverable acceleration structures: {help}"
    );
    assert!(
        help.contains("transaction file protocol"),
        "CLI help should mention the transaction file protocol: {help}"
    );
    assert!(
        help.contains("parent-directory syncs"),
        "CLI help should mention required parent-directory syncs: {help}"
    );
    assert!(
        help.contains("not index flushing"),
        "CLI help should state that durability does not come from index flushing: {help}"
    );
}

#[test]
fn readme_describes_v2_strict_crash_acid_requirements() {
    let readme = normalize_whitespace(readme_text());

    for needle in [
        "exclusive-writer backend lock",
        ".ln2_fs_lock",
        ".__ln2_obj_<id>",
        "legacy hash-derived",
        "strict no-replace",
        "EEXIST",
        "EPERM",
        "recoverable acceleration structures",
        "transaction file protocol",
        "parent-directory syncs",
        "index flushing",
    ] {
        assert!(
            readme.contains(needle),
            "README should contain `{needle}`: {readme}"
        );
    }
}
