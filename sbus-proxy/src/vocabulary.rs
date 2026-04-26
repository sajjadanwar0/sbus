// ───────────────────────────────────────────────────────────────────────────
// vocabulary.rs — shard-name matcher
//
// Given a list of shard names (the "vocabulary"), builds a single regex
// matcher that finds any occurrence with word-boundary semantics. We use a
// case-insensitive single-pattern regex with alternation rather than
// Aho-Corasick because (a) for vocabularies <10k items regex is fast enough,
// (b) zero extra dependencies beyond what we already pulled in.
//
// The matcher returns *unique* shard names found in a given text, not the
// count or positions. The DeliveryLog only cares which shards were touched.
// ───────────────────────────────────────────────────────────────────────────

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use regex::Regex;

use crate::config::ProxyConfig;

pub struct Vocabulary {
    shards:  Vec<String>,
    matcher: Regex,
}

impl Vocabulary {
    pub fn from_config(config: &ProxyConfig) -> Result<Self> {
        let shards = config.vocab();
        if shards.is_empty() {
            anyhow::bail!("vocabulary is empty");
        }

        // Build a case-insensitive alternation with word boundaries. We
        // regex-escape each shard name to prevent injection via malformed
        // config (e.g. if someone puts `.*` in the vocab list).
        let alternation = shards.iter()
            .map(|s| regex::escape(s))
            .collect::<Vec<_>>()
            .join("|");
        let pattern = format!(r"(?i)\b(?:{alternation})\b");
        let matcher = Regex::new(&pattern)
            .with_context(|| format!("failed to compile vocab regex: {pattern}"))?;

        Ok(Self { shards, matcher })
    }

    pub fn n_shards(&self) -> usize { self.shards.len() }

    /// Return the set of shard names that appear in `text`. Matches are
    /// case-insensitive; the returned names use the canonical casing from
    /// the vocabulary, not the form found in the text.
    pub fn scan(&self, text: &str) -> BTreeSet<String> {
        let mut hits = BTreeSet::new();
        for mat in self.matcher.find_iter(text) {
            // Find the canonical vocabulary entry matching this hit
            // (case-insensitive compare).
            let found = mat.as_str();
            if let Some(canonical) = self.shards.iter()
                .find(|s| s.eq_ignore_ascii_case(found))
            {
                hits.insert(canonical.clone());
            }
        }
        hits
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn vocab(items: &[&str]) -> Vocabulary {
        let cfg = ProxyConfig {
            listen_port:            9000,
            upstream_url:           "https://api.openai.com".into(),
            upstream_url_anthropic: "https://api.anthropic.com".into(),
            upstream_url_google:    "https://generativelanguage.googleapis.com".into(),
            sbus_url:               "http://localhost:7000".into(),
            vocab_raw:              items.join(","),
            agent_header:           "X-SBus-Agent-Id".into(),
            session_header:         "X-SBus-Session-Id".into(),
            debug_passthrough:      false,
        };
        Vocabulary::from_config(&cfg).unwrap()
    }

    #[test]
    fn finds_simple_match() {
        let v = vocab(&["models_state", "orm_query"]);
        let hits = v.scan("The models_state field is stale");
        assert_eq!(hits.len(), 1);
        assert!(hits.contains("models_state"));
    }

    #[test]
    fn word_boundaries_prevent_substring_match() {
        let v = vocab(&["query"]);
        // "queryset" should NOT match "query" because of word boundaries
        let hits = v.scan("We updated the queryset implementation");
        assert!(hits.is_empty(), "got hits: {hits:?}");
    }

    #[test]
    fn case_insensitive() {
        let v = vocab(&["db_schema"]);
        let hits = v.scan("The DB_SCHEMA was migrated");
        assert_eq!(hits.len(), 1);
        // Canonical form preserved
        assert!(hits.contains("db_schema"));
    }

    #[test]
    fn multiple_unique_hits() {
        let v = vocab(&["orm_query", "test_fixture", "review_notes"]);
        let text = "Update orm_query and orm_query and test_fixture, skip review_notes";
        let hits = v.scan(text);
        assert_eq!(hits.len(), 3);
        assert!(hits.contains("orm_query"));
        assert!(hits.contains("test_fixture"));
        assert!(hits.contains("review_notes"));
    }

    #[test]
    fn regex_special_chars_in_vocab_escaped() {
        // Shard names containing regex metacharacters must not break the
        // compiled pattern.
        let v = vocab(&["shard.v2", "orm-query"]);
        assert_eq!(v.scan("shard.v2 here").len(), 1);
        assert_eq!(v.scan("orm-query invoked").len(), 1);
        // And they should NOT match anything via regex interpretation.
        assert_eq!(v.scan("shardXv2 here").len(), 0);
    }

    #[test]
    fn empty_text_returns_empty() {
        let v = vocab(&["anything"]);
        assert!(v.scan("").is_empty());
    }
}