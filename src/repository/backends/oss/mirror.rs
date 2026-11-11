//! A node-local, read-only copy of some repository objects.
//!
//! `AENV_LOCAL_MIRROR_DIR` names a directory laid out like the repository:
//! `<dir>/<object key>`, e.g. `<dir>/managed-layers/sha256:…` or
//! `<dir>/artifacts/<snapshot>/vm_state.bin`. It is filled outside the server
//! (for example with `s5cmd` before the server starts) for the templates a
//! node should start fast. An object found there is read in place; any other
//! object is read from object storage as before. The server never writes to
//! the mirror.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tracing::{info, warn};

pub(crate) const LOCAL_MIRROR_DIR_ENV: &str = "AENV_LOCAL_MIRROR_DIR";

pub(crate) struct LocalMirror {
    root: PathBuf,
    /// Keys whose source was already logged, so each object is logged once.
    logged: Mutex<HashSet<String>>,
}

impl LocalMirror {
    pub(crate) fn from_env() -> Option<Self> {
        let root = std::env::var_os(LOCAL_MIRROR_DIR_ENV).filter(|dir| !dir.is_empty())?;
        info!(root = %Path::new(&root).display(), "repository local mirror enabled");
        Some(Self::new(root.into()))
    }

    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            logged: Mutex::new(HashSet::new()),
        }
    }

    /// The mirror's copy of `key`, if it holds a regular file of `size` bytes
    /// (any size when `None`).
    pub(crate) fn locate(&self, key: &str, size: Option<u64>) -> Option<PathBuf> {
        let path = self.root.join(key);
        let actual = std::fs::metadata(&path)
            .ok()
            .filter(|metadata| metadata.is_file())
            .map(|metadata| metadata.len());
        let first = self.logged.lock().unwrap().insert(key.to_owned());
        match (actual, size) {
            (Some(actual), Some(expected)) if actual != expected => {
                if first {
                    warn!(
                        object = key,
                        path = %path.display(),
                        expected,
                        actual,
                        "repository object in the local mirror has the wrong size; reading it from object storage"
                    );
                }
                None
            }
            (Some(_), _) => {
                if first {
                    info!(object = key, path = %path.display(), "repository object read from the local mirror");
                }
                Some(path)
            }
            (None, _) => {
                if first {
                    info!(
                        object = key,
                        "repository object not in the local mirror; reading it from object storage"
                    );
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locates_only_present_files_of_the_expected_size() {
        let root = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(root.path().join("managed-layers")).expect("mkdir");
        std::fs::write(root.path().join("managed-layers/sha256:aa"), b"four").expect("write");
        let mirror = LocalMirror::new(root.path().to_path_buf());

        assert_eq!(
            mirror.locate("managed-layers/sha256:aa", Some(4)),
            Some(root.path().join("managed-layers/sha256:aa"))
        );
        assert_eq!(
            mirror.locate("managed-layers/sha256:aa", None),
            Some(root.path().join("managed-layers/sha256:aa"))
        );
        assert_eq!(mirror.locate("managed-layers/sha256:aa", Some(5)), None);
        assert_eq!(mirror.locate("managed-layers/sha256:bb", None), None);
        assert_eq!(mirror.locate("managed-layers", None), None);
    }
}
