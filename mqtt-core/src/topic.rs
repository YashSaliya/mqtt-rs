//! MQTT topic string validation — §4.7
//!
//! Topics in MQTT use `/` as a level separator.  Two wildcard characters are
//! defined for **subscriptions only** (never for PUBLISH topics):
//!
//! * `+`  — single-level wildcard: matches exactly one level
//! * `#`  — multi-level wildcard: matches the rest of the topic; **must** be
//!           the last character and preceded by `/` or be the only character
//!
//! Shared subscription filters use the prefix `$share/{ShareName}/{Filter}`
//! and follow the same rules for the actual filter portion.

use crate::error::Error;

/// Maximum topic or filter length allowed on the wire (UTF-8 bytes, §4.7.3).
pub const MAX_TOPIC_LEN: usize = 65_535;

// ── Public validators ─────────────────────────────────────────────────────────

/// Validate a **publish** topic string.
/// - Must not be empty
/// - Must not exceed `MAX_TOPIC_LEN`
/// - Must not contain `+` or `#` (wildcards are subscription-only)
/// - Must not contain a null byte (`\0`)
pub fn validate_publish_topic(topic: &str) -> Result<(), Error> {
    if topic.is_empty() {
        return Err(Error::InvalidTopic("topic must not be empty".into()));
    }
    if topic.len() > MAX_TOPIC_LEN {
        return Err(Error::InvalidTopic(format!(
            "topic exceeds maximum length of {} bytes",
            MAX_TOPIC_LEN
        )));
    }
    for ch in topic.chars() {
        match ch {
            '+' | '#' => {
                return Err(Error::InvalidTopic(format!(
                    "publish topic must not contain wildcard '{ch}'"
                )));
            }
            '\0' => {
                return Err(Error::InvalidTopic(
                    "topic must not contain null character".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Validate a **subscription** topic filter, including wildcard rules.
///
/// If the filter starts with `$share/`, the share-name is validated and the
/// rest is treated as a regular filter.
pub fn validate_subscription_filter(filter: &str) -> Result<(), Error> {
    if filter.is_empty() {
        return Err(Error::InvalidTopic("topic filter must not be empty".into()));
    }
    if filter.len() > MAX_TOPIC_LEN {
        return Err(Error::InvalidTopic(format!(
            "topic filter exceeds maximum length of {} bytes",
            MAX_TOPIC_LEN
        )));
    }

    // Handle shared subscription prefix ($share/{ShareName}/{filter})
    let actual_filter = if filter.starts_with("$share/") {
        validate_shared_filter(filter)?
    } else {
        filter
    };

    validate_filter_chars(actual_filter)
}

/// Returns true if `filter` matches `topic`.
///
/// Handles `+` (single-level) and `#` (multi-level) wildcards.
/// Does NOT handle `$share/` prefixes — strip those before calling.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    // System topics starting with `$` are NOT matched by `#` or `+` at the
    // first level (spec §4.7.2) unless the filter also starts with `$`.
    let topic_is_system = topic.starts_with('$');
    let filter_is_system = filter.starts_with('$');
    if topic_is_system && !filter_is_system {
        return false;
    }

    match_filter(filter, topic)
}

// ── Internal helpers ─────────────────────────────────────────────────────────

fn match_filter(filter: &str, topic: &str) -> bool {
    let mut f_iter = filter.split('/').peekable();
    let mut t_iter = topic.split('/').peekable();

    loop {
        match (f_iter.next(), t_iter.next()) {
            // Multi-level wildcard matches everything remaining
            (Some("#"), _) => return true,

            // Single-level wildcard matches exactly one level
            (Some("+"), Some(_)) => {
                if f_iter.peek().is_none() && t_iter.peek().is_none() {
                    return true;
                }
                // continue matching remaining levels
            }

            // Exact segment match
            (Some(f), Some(t)) if f == t => {
                if f_iter.peek().is_none() && t_iter.peek().is_none() {
                    return true;
                }
            }

            // Both exhausted at the same time → match
            (None, None) => return true,

            // Mismatch or unequal depth
            _ => return false,
        }
    }
}

fn validate_filter_chars(filter: &str) -> Result<(), Error> {
    let levels: Vec<&str> = filter.split('/').collect();

    for (i, level) in levels.iter().enumerate() {
        if level.contains('\0') {
            return Err(Error::InvalidTopic(
                "topic filter must not contain null character".into(),
            ));
        }
        if level.contains('#') {
            // `#` must be alone in its level and must be the last level
            if *level != "#" {
                return Err(Error::InvalidTopic(
                    "'#' must occupy an entire level by itself".into(),
                ));
            }
            if i != levels.len() - 1 {
                return Err(Error::InvalidTopic(
                    "'#' must be the last level in a topic filter".into(),
                ));
            }
        }
        if level.contains('+') && *level != "+" {
            return Err(Error::InvalidTopic(
                "'+' must occupy an entire level by itself".into(),
            ));
        }
    }
    Ok(())
}

/// Validate `$share/{ShareName}/{filter}` and return the bare filter portion.
fn validate_shared_filter(filter: &str) -> Result<&str, Error> {
    // strip "$share/"
    let rest = &filter["$share/".len()..];

    let slash = rest.find('/').ok_or_else(|| {
        Error::InvalidTopic(
            "shared subscription filter must be '$share/{name}/{filter}'".into(),
        )
    })?;

    let share_name = &rest[..slash];
    if share_name.is_empty() {
        return Err(Error::InvalidTopic(
            "share name in '$share/{name}/...' must not be empty".into(),
        ));
    }
    if share_name.contains('+') || share_name.contains('#') || share_name.contains('/') {
        return Err(Error::InvalidTopic(
            "share name must not contain '+', '#', or '/'".into(),
        ));
    }

    Ok(&rest[slash + 1..])
}

/// Strip the `$share/{name}/` prefix from a shared subscription filter,
/// returning `(share_name, bare_filter)`.  Returns `None` if not a shared sub.
pub fn parse_shared_subscription(filter: &str) -> Option<(&str, &str)> {
    if !filter.starts_with("$share/") {
        return None;
    }
    let rest = &filter["$share/".len()..];
    let slash = rest.find('/')?;
    Some((&rest[..slash], &rest[slash + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_publish_topics() {
        assert!(validate_publish_topic("sensors/temp").is_ok());
        assert!(validate_publish_topic("a").is_ok());
        assert!(validate_publish_topic("$SYS/broker/uptime").is_ok());
    }

    #[test]
    fn invalid_publish_topics() {
        assert!(validate_publish_topic("").is_err());
        assert!(validate_publish_topic("a/+/b").is_err());
        assert!(validate_publish_topic("a/#").is_err());
    }

    #[test]
    fn valid_subscription_filters() {
        assert!(validate_subscription_filter("#").is_ok());
        assert!(validate_subscription_filter("a/+/b").is_ok());
        assert!(validate_subscription_filter("a/b/c/#").is_ok());
        assert!(validate_subscription_filter("+").is_ok());
        assert!(validate_subscription_filter("$share/workers/jobs/#").is_ok());
    }

    #[test]
    fn invalid_subscription_filters() {
        assert!(validate_subscription_filter("a/#/b").is_err());
        assert!(validate_subscription_filter("a+b").is_err());
        assert!(validate_subscription_filter("$share//jobs/#").is_err());
        assert!(validate_subscription_filter("$share/w+k/jobs").is_err());
    }

    #[test]
    fn topic_matching() {
        assert!(topic_matches("#", "a/b/c"));
        assert!(topic_matches("a/+/c", "a/b/c"));
        assert!(topic_matches("a/b/c", "a/b/c"));
        assert!(topic_matches("a/#", "a/b/c/d"));
        assert!(!topic_matches("a/b", "a/b/c"));
        assert!(!topic_matches("+", "a/b"));
        // System topics not matched by plain wildcards
        assert!(!topic_matches("#", "$SYS/broker"));
        assert!(!topic_matches("+/broker", "$SYS/broker"));
        // But explicit $-prefixed filters match $-prefixed topics
        assert!(topic_matches("$SYS/#", "$SYS/broker/uptime"));
    }

    #[test]
    fn shared_sub_parsing() {
        let (name, filter) = parse_shared_subscription("$share/workers/jobs/#").unwrap();
        assert_eq!(name, "workers");
        assert_eq!(filter, "jobs/#");
    }
}
