use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub max_packet_size: u32,
    pub receive_maximum: u16,
    pub allow_anonymous: bool,
    pub users: Vec<UserConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserConfig {
    pub username: String,
    pub password: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 1883,
            max_packet_size: 268_435_455, // Max allowed MQTT packet size (256 MB)
            receive_maximum: 1000,
            allow_anonymous: true,
            users: Vec::new(),
        }
    }
}
