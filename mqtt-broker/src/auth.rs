use crate::config::Config;

pub struct AuthManager {
    allow_anonymous: bool,
    users: std::collections::HashMap<String, String>,
}

impl AuthManager {
    pub fn new(config: &Config) -> Self {
        let mut users = std::collections::HashMap::new();
        for user in &config.users {
            users.insert(user.username.clone(), user.password.clone());
        }
        Self {
            allow_anonymous: config.allow_anonymous,
            users,
        }
    }

    pub fn authenticate(&self, username: Option<&str>, password: Option<&[u8]>) -> bool {
        match (username, password) {
            (Some(user), Some(pass)) => {
                if let Some(expected_pass) = self.users.get(user) {
                    if let Ok(pass_str) = std::str::from_utf8(pass) {
                        return expected_pass == pass_str;
                    }
                }
                false
            }
            (None, None) => self.allow_anonymous,
            _ => false,
        }
    }
}
