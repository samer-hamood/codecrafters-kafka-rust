use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
pub struct Config {
    pub log: LogConfig,
}

#[derive(Deserialize)]
#[serde(default)]
pub struct LogConfig {
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

pub fn load_config() -> Config {
    let filename = "src/config.toml";
    let contents =
        fs::read_to_string(filename).unwrap_or_else(|_| panic!("Problem reading {filename} file"));
    let config: Config = toml::from_str(&contents).expect("Failed to parse TOML");
    config
}
