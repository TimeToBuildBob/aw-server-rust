use dirs::home_dir;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

/// Resolve the instance profile.
/// `--profile` wins, then `AW_PROFILE`, then `--testing` → `"testing"`, else `"default"`.
#[allow(dead_code)] // used by the aw-sync binary; the lib copy is unused
pub fn resolve_profile(
    cli_profile: Option<&str>,
    testing: bool,
    env_profile: Option<&str>,
) -> String {
    if let Some(p) = cli_profile {
        if !p.is_empty() {
            return p.to_string();
        }
    }
    if let Some(p) = env_profile {
        if !p.is_empty() {
            return p.to_string();
        }
    }
    if testing {
        "testing".to_string()
    } else {
        "default".to_string()
    }
}

/// aw-sync's own config dir: `{appname}/aw-sync`.
///
/// Uses the same profile appname as aw-server so a named profile (e.g.
/// `research`) does not share prod's sync config. `default` and `testing`
/// keep the bare `activitywatch` root, matching aw-server.
// TODO: add proper config support
#[cfg(not(target_os = "android"))]
#[allow(dead_code)]
pub fn get_config_dir() -> Result<PathBuf, Box<dyn Error>> {
    let dir = sync_config_dir(&aw_server::dirs::appname())?;
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Path construction only — does not create directories (so tests stay off-disk).
#[cfg(not(target_os = "android"))]
fn sync_config_dir(appname: &str) -> Result<PathBuf, Box<dyn Error>> {
    Ok(dirs::config_dir()
        .ok_or("Unable to read user config dir")?
        .join(appname)
        .join("aw-sync"))
}

#[cfg(not(target_os = "android"))]
pub fn get_server_config_path(testing: bool) -> Result<PathBuf, ()> {
    let dir = aw_server::dirs::get_config_dir()?;
    let profile = aw_server::config::get_profile();
    // If set_profile hasn't run yet, honour the legacy testing bool so
    // `--testing` still finds config-testing.toml.
    let filename = aw_server::config::config_filename(if profile == "default" && testing {
        "testing"
    } else {
        profile
    });
    Ok(dir.join(filename))
}

pub fn get_sync_dir() -> Result<PathBuf, Box<dyn Error>> {
    // if AW_SYNC_DIR is set, use that
    if let Ok(dir) = std::env::var("AW_SYNC_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let home_dir = home_dir().ok_or("Unable to read home_dir")?;
    Ok(home_dir.join("ActivityWatchSync"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_profile_cli_wins_over_env_and_testing() {
        assert_eq!(
            resolve_profile(Some("research"), true, Some("testing")),
            "research"
        );
    }

    #[test]
    fn resolve_profile_env_wins_over_testing() {
        assert_eq!(resolve_profile(None, true, Some("research")), "research");
    }

    #[test]
    fn resolve_profile_testing_alias_and_default() {
        assert_eq!(resolve_profile(None, true, None), "testing");
        assert_eq!(resolve_profile(None, false, None), "default");
        assert_eq!(resolve_profile(Some(""), false, Some("")), "default");
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn sync_config_dir_is_isolated_per_named_profile() {
        let default_dir = sync_config_dir(&aw_server::dirs::appname_for("default")).unwrap();
        let testing_dir = sync_config_dir(&aw_server::dirs::appname_for("testing")).unwrap();
        let research_dir = sync_config_dir(&aw_server::dirs::appname_for("research")).unwrap();

        // default and testing share the bare root (legacy suffixes elsewhere)
        assert_eq!(default_dir, testing_dir);
        assert!(
            default_dir.ends_with("activitywatch/aw-sync")
                || default_dir.ends_with("activitywatch\\aw-sync"),
            "default/testing should stay under activitywatch/aw-sync, got {default_dir:?}"
        );

        assert_ne!(
            research_dir, default_dir,
            "research must not share prod's aw-sync config dir"
        );
        assert!(
            research_dir.ends_with("activitywatch-research/aw-sync")
                || research_dir.ends_with("activitywatch-research\\aw-sync"),
            "research should live under activitywatch-research/aw-sync, got {research_dir:?}"
        );
    }

    #[test]
    fn server_config_filename_matches_aw_server_rule() {
        assert_eq!(aw_server::config::config_filename("default"), "config.toml");
        assert_eq!(
            aw_server::config::config_filename("testing"),
            "config-testing.toml"
        );
        assert_eq!(
            aw_server::config::config_filename("research"),
            "config-research.toml"
        );
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn server_config_path_honours_testing_bool_before_set_profile() {
        let testing = get_server_config_path(true).unwrap();
        let production = get_server_config_path(false).unwrap();
        assert!(
            testing.ends_with("config-testing.toml"),
            "testing should read config-testing.toml, got {testing:?}"
        );
        assert!(
            production.ends_with("config.toml"),
            "default should read config.toml, got {production:?}"
        );
    }
}
