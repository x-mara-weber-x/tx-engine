pub mod tests {
    #[macro_export]
    macro_rules! test_resource_path {
        ($fname:expr) => {
            concat!(env!("CARGO_MANIFEST_DIR"), "/test-resources/", $fname)
        };
    }
}
