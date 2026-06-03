use aw_models::Event;

/// For each event in *base_events*, find the longest-overlapping event in
/// *subwatcher_events* and copy the named *keys* from that subwatcher event
/// into the base event's ``data`` dict.
///
/// Timestamps, durations, and event count of *base_events* are **unchanged**
/// — no phantom events are created. This makes duration/app/title aggregations
/// stay correct, unlike the ``concat`` workaround.
///
/// This is the backend primitive that lets every client (webui, native UIs,
/// exporters) categorize by subwatcher fields (browser ``url``/``$domain``;
/// editor ``project``/``file``/``language``) without bespoke per-watcher
/// client-side code.
///
/// # Arguments
///
/// * `base_events` - The canonical window/afk-filtered stream to enrich.
/// * `subwatcher_events` - Events from a subwatcher bucket (e.g. aw-watcher-vim,
///   aw-watcher-web). Should already be clipped via
///   `filter_period_intersect` before passing here.
/// * `keys` - Which keys to copy from the subwatcher event into the base event.
///   Keys already present in the base event are left untouched when
///   `conflict="base_wins"` (default).
/// * `conflict` - `"base_wins"` (default) — base event's existing keys are
///   never overwritten; subwatcher fields are purely additive.
///   `"sub_wins"` — subwatcher fields overwrite base fields.
///
/// Returns a new list of base events with subwatcher fields injected.
/// Events in *base_events* that have no overlapping subwatcher event are
/// returned with their original data unchanged.
///
/// # Example
///
/// ```ignore
/// let window_events = query_bucket(bid_window);
/// let editor_events = flood(query_bucket(bid_editor));
/// let editor_events = filter_period_intersect(editor_events, window_events);
/// let window_events = merge_subwatcher_fields(
///     window_events, editor_events,
///     &["project".to_string(), "file".to_string(), "language".to_string()],
///     "base_wins"
/// );
/// // Now categorize(window_events, ...) can match on "project"/"file"
/// ```
///
/// Note on N:1 overlap:
/// When multiple subwatcher events overlap a single base event, the one
/// with the **longest overlap duration** is used (attach-longest strategy).
pub fn merge_subwatcher_fields(
    base_events: Vec<Event>,
    subwatcher_events: Vec<Event>,
    keys: &[String],
    conflict: &str,
) -> Vec<Event> {
    assert!(
        conflict == "base_wins" || conflict == "sub_wins",
        "conflict must be 'base_wins' or 'sub_wins', got {conflict:?}"
    );

    if subwatcher_events.is_empty() || keys.is_empty() {
        return base_events;
    }

    // Sort subwatcher events by timestamp for linear scan
    let mut sub_sorted = subwatcher_events;
    sub_sorted.sort_by_key(|e| e.timestamp);

    base_events
        .into_iter()
        .map(|base| {
            let base_end = base.calculate_endtime();
            let mut best_sub: Option<&Event> = None;
            let mut best_overlap_ms: i64 = 0;

            for sub in &sub_sorted {
                let sub_end = sub.calculate_endtime();

                // Once sub starts after base ends, we can stop
                if sub.timestamp >= base_end {
                    break;
                }

                // Skip sub events that end before base starts
                if sub_end <= base.timestamp {
                    continue;
                }

                // Calculate overlap duration
                let overlap_start = sub.timestamp.max(base.timestamp);
                let overlap_end = sub_end.min(base_end);
                let overlap = overlap_end - overlap_start;
                let overlap_ms = overlap.num_milliseconds();

                if overlap_ms > best_overlap_ms {
                    best_overlap_ms = overlap_ms;
                    best_sub = Some(sub);
                }
            }

            let mut enriched = base.clone();
            if let Some(sub) = best_sub {
                for key in keys {
                    if let Some(value) = sub.data.get(key) {
                        if conflict == "base_wins" && enriched.data.contains_key(key) {
                            // base keeps its value
                            continue;
                        }
                        enriched.data.insert(key.clone(), value.clone());
                    }
                }
            }

            enriched
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{DateTime, Duration, Utc};
    use serde_json::json;

    use aw_models::Event;

    use super::merge_subwatcher_fields;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::from_str(s).unwrap()
    }

    #[test]
    fn test_basic_merge() {
        let base = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:00Z"),
            duration: Duration::seconds(10),
            data: json_map! {"app": json!("vim")},
        }];
        let sub = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:02Z"),
            duration: Duration::seconds(5),
            data: json_map! {"project": json!("gptme"), "file": json!("main.rs"), "language": json!("Rust")},
        }];
        let result = merge_subwatcher_fields(
            base,
            sub,
            &[
                "project".to_string(),
                "file".to_string(),
                "language".to_string(),
            ],
            "base_wins",
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data.get("app").unwrap(), &json!("vim"));
        assert_eq!(result[0].data.get("project").unwrap(), &json!("gptme"));
        assert_eq!(result[0].data.get("file").unwrap(), &json!("main.rs"));
        assert_eq!(result[0].data.get("language").unwrap(), &json!("Rust"));
        // Timestamps/durations unchanged
        assert_eq!(result[0].timestamp, ts("2000-01-01T00:00:00Z"));
        assert_eq!(result[0].duration, Duration::seconds(10));
    }

    #[test]
    fn test_no_overlap() {
        let base = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:00Z"),
            duration: Duration::seconds(5),
            data: json_map! {"app": json!("firefox")},
        }];
        let sub = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:10Z"),
            duration: Duration::seconds(5),
            data: json_map! {"project": json!("other")},
        }];
        let result = merge_subwatcher_fields(base, sub, &["project".to_string()], "base_wins");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data.get("app").unwrap(), &json!("firefox"));
        assert!(result[0].data.get("project").is_none());
    }

    #[test]
    fn test_base_wins_conflict() {
        let base = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:00Z"),
            duration: Duration::seconds(10),
            data: json_map! {"project": json!("base-project")},
        }];
        let sub = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:02Z"),
            duration: Duration::seconds(5),
            data: json_map! {"project": json!("sub-project")},
        }];
        let result = merge_subwatcher_fields(
            base,
            sub,
            &["project".to_string()],
            "base_wins",
        );
        assert_eq!(
            result[0].data.get("project").unwrap(),
            &json!("base-project")
        );
    }

    #[test]
    fn test_sub_wins_conflict() {
        let base = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:00Z"),
            duration: Duration::seconds(10),
            data: json_map! {"file": json!("old.rs")},
        }];
        let sub = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:02Z"),
            duration: Duration::seconds(5),
            data: json_map! {"file": json!("new.rs")},
        }];
        let result = merge_subwatcher_fields(
            base,
            sub,
            &["file".to_string()],
            "sub_wins",
        );
        assert_eq!(result[0].data.get("file").unwrap(), &json!("new.rs"));
    }

    #[test]
    fn test_attach_longest_overlap() {
        // sub_short overlaps for 1s, sub_long overlaps for 9s
        let base = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:00Z"),
            duration: Duration::seconds(10),
            data: json_map! {"app": json!("vim")},
        }];
        let sub_short = Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:00Z"),
            duration: Duration::seconds(1),
            data: json_map! {"project": json!("short")},
        };
        let sub_long = Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:01Z"),
            duration: Duration::seconds(9),
            data: json_map! {"project": json!("long")},
        };
        let result = merge_subwatcher_fields(
            base,
            vec![sub_short, sub_long],
            &["project".to_string()],
            "base_wins",
        );
        assert_eq!(
            result[0].data.get("project").unwrap(),
            &json!("long")
        );
    }

    #[test]
    fn test_empty_subwatcher_events() {
        let base = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:00Z"),
            duration: Duration::seconds(10),
            data: json_map! {"app": json!("vim")},
        }];
        let result = merge_subwatcher_fields(base, vec![], &["project".to_string()], "base_wins");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data.get("app").unwrap(), &json!("vim"));
    }

    #[test]
    fn test_empty_keys() {
        let base = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:00Z"),
            duration: Duration::seconds(10),
            data: json_map! {"app": json!("vim")},
        }];
        let sub = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:02Z"),
            duration: Duration::seconds(5),
            data: json_map! {"project": json!("gptme")},
        }];
        let result = merge_subwatcher_fields(base, sub, &[], "base_wins");
        assert_eq!(result.len(), 1);
        // Empty keys: no enrichment
        assert_eq!(result[0].data.get("app").unwrap(), &json!("vim"));
        assert!(result[0].data.get("project").is_none());
    }

    #[test]
    fn test_multiple_base_events() {
        let base = vec![
            Event {
                id: None,
                timestamp: ts("2000-01-01T00:00:00Z"),
                duration: Duration::seconds(5),
                data: json_map! {"app": json!("vim")},
            },
            Event {
                id: None,
                timestamp: ts("2000-01-01T00:00:05Z"),
                duration: Duration::seconds(5),
                data: json_map! {"app": json!("firefox")},
            },
        ];
        let sub = vec![Event {
            id: None,
            timestamp: ts("2000-01-01T00:00:02Z"),
            duration: Duration::seconds(8),
            data: json_map! {"project": json!("gptme")},
        }];
        let result = merge_subwatcher_fields(base, sub, &["project".to_string()], "base_wins");
        assert_eq!(result.len(), 2);
        // First base event overlaps with sub
        assert_eq!(result[0].data.get("project").unwrap(), &json!("gptme"));
        // Second base event also overlaps with sub
        assert_eq!(result[1].data.get("project").unwrap(), &json!("gptme"));
    }
}
