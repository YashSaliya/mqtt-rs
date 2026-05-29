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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UserConfig;

    #[test]
    fn test_anonymous_authenticator() {
        let auth = AnonymousAuthenticator;
        assert!(auth.authenticate(None, None));
        assert!(auth.authenticate(Some("yash"), Some(b"pass123")));
    }

    #[test]
    fn test_basic_authenticator() {
        let mut config = Config::default();
        config.users.push(UserConfig {
            username: "yash".to_string(),
            password: "securepassword".to_string(),
        });
        
        let auth = BasicAuthenticator::new(&config);

        // Correct credentials
        assert!(auth.authenticate(Some("yash"), Some(b"securepassword")));

        // Wrong password
        assert!(!auth.authenticate(Some("yash"), Some(b"wrongpassword")));

        // Non-existent user
        assert!(!auth.authenticate(Some("unknown"), Some(b"securepassword")));

        // Missing username
        assert!(!auth.authenticate(None, Some(b"securepassword")));

        // Missing password
        assert!(!auth.authenticate(Some("yash"), None));
    }

    #[test]
    fn test_token_authenticator() {
        let mut config = Config::default();
        config.tokens.push("jwt-token-12345".to_string());
        
        let auth = TokenAuthenticator::new(&config);

        // Correct token in password field
        assert!(auth.authenticate(None, Some(b"jwt-token-12345")));

        // Wrong token
        assert!(!auth.authenticate(None, Some(b"bad-token")));

        // Missing token
        assert!(!auth.authenticate(None, None));
    }

    #[test]
    fn test_factory_creation() {
        let mut config = Config::default();

        config.auth_mode = AuthMode::Anonymous;
        let auth = create_authenticator(&config);
        assert!(auth.authenticate(None, None)); // Anonymous succeeds

        config.auth_mode = AuthMode::Basic;
        let auth = create_authenticator(&config);
        assert!(!auth.authenticate(None, None)); // Basic fails without creds
    }
}
