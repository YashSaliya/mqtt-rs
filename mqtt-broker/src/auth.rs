use std::collections::{HashMap, HashSet};
use crate::config::{Config, AuthMode};

/// The pluggable Authenticator trait.
pub trait Authenticator: Send + Sync {
    fn authenticate(&self, username: Option<&str>, password: Option<&[u8]>) -> bool;
}

/// Allows all connections unconditionally.
pub struct AnonymousAuthenticator;

impl Authenticator for AnonymousAuthenticator {
    fn authenticate(&self, _username: Option<&str>, _password: Option<&[u8]>) -> bool {
        true
    }
}

/// Standard Username/Password credentials authenticator.
pub struct BasicAuthenticator {
    users: HashMap<String, String>,
}

impl BasicAuthenticator {
    pub fn new(config: &Config) -> Self {
        let mut users = HashMap::new();
        for user in &config.users {
            users.insert(user.username.clone(), user.password.clone());
        }
        Self { users }
    }
}

impl Authenticator for BasicAuthenticator {
    fn authenticate(&self, username: Option<&str>, password: Option<&[u8]>) -> bool {
        match (username, password) {
            (Some(user), Some(pass)) => {
                if let Some(expected_pass) = self.users.get(user) {
                    if let Ok(pass_str) = std::str::from_utf8(pass) {
                        return expected_pass == pass_str;
                    }
                }
                false
            }
            _ => false,
        }
    }
}

/// Validates custom API keys / JWT tokens passed in the password field.
pub struct TokenAuthenticator {
    tokens: HashSet<String>,
}

impl TokenAuthenticator {
    pub fn new(config: &Config) -> Self {
        let mut tokens = HashSet::new();
        for token in &config.tokens {
            tokens.insert(token.clone());
        }
        Self { tokens }
    }
}

impl Authenticator for TokenAuthenticator {
    fn authenticate(&self, _username: Option<&str>, password: Option<&[u8]>) -> bool {
        if let Some(pass) = password {
            if let Ok(token_str) = std::str::from_utf8(pass) {
                return self.tokens.contains(token_str);
            }
        }
        false
    }
}

/// Dynamic factory to create authenticators based on the active config.
pub fn create_authenticator(config: &Config) -> Box<dyn Authenticator> {
    match config.auth_mode {
        AuthMode::Anonymous => Box::new(AnonymousAuthenticator),
        AuthMode::Basic => Box::new(BasicAuthenticator::new(config)),
        AuthMode::Token => Box::new(TokenAuthenticator::new(config)),
    }
}
