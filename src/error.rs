pub type PluginResult<T> = Result<T, String>;

pub fn err_to_string<E: std::fmt::Display>(error: E) -> String {
    error.to_string()
}
