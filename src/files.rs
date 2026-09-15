use std::{
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};

#[derive(Clone)]
pub struct FileArea {
    root: Arc<PathBuf>,
}

pub struct AvailableFile {
    pub name: String,
    pub size: u64,
}

impl FileArea {
    pub async fn open(path: PathBuf) -> Result<Self> {
        let root = tokio::task::spawn_blocking(move || {
            fs::create_dir_all(&path).with_context(|| {
                format!("failed to create file directory at {}", path.display())
            })?;
            path.canonicalize()
                .with_context(|| format!("failed to resolve file directory at {}", path.display()))
        })
        .await
        .context("file-directory setup task failed")??;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    pub async fn list(&self) -> Result<Vec<AvailableFile>> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let mut files = Vec::new();
            for entry in fs::read_dir(root.as_path())? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let Ok(name) = entry.file_name().into_string() else {
                    continue;
                };
                if !is_safe_file_name(&name) {
                    continue;
                }
                files.push(AvailableFile {
                    name,
                    size: entry.metadata()?.len(),
                });
            }
            files.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(files)
        })
        .await
        .context("file-list task failed")?
    }

    pub async fn resolve_download(&self, name: &str) -> Result<Option<PathBuf>> {
        if !is_safe_file_name(name) {
            bail!("file name must be a single, non-whitespace path component");
        }

        let root = self.root.clone();
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || {
            let candidate = root.join(name);
            let metadata = match fs::symlink_metadata(&candidate) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            if !metadata.file_type().is_file() {
                return Ok(None);
            }

            let resolved = candidate.canonicalize()?;
            if !resolved.starts_with(root.as_path()) {
                return Ok(None);
            }
            Ok(Some(resolved))
        })
        .await
        .context("file-resolution task failed")?
    }

    pub fn upload_paths(&self, name: &str) -> Result<(PathBuf, PathBuf)> {
        if !is_safe_file_name(name) {
            bail!("file name must be a single, non-whitespace path component");
        }
        let final_path = self.root.join(name);
        let part_path = self.root.join(format!(".{name}.part"));
        Ok((final_path, part_path))
    }
}

fn is_safe_file_name(name: &str) -> bool {
    if name.is_empty() || name.chars().any(char::is_whitespace) || name.contains(['/', '\\']) {
        return false;
    }

    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::FileArea;

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn directory_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "abbs-files-test-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[tokio::test]
    async fn lists_safe_files_and_rejects_traversal() {
        let path = directory_path();
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("bulletin.txt"), b"CQ CQ").unwrap();
        fs::create_dir(path.join("nested")).unwrap();
        fs::write(path.join("not downloadable.txt"), b"space").unwrap();

        let area = FileArea::open(path.clone()).await.unwrap();
        let files = area.list().await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "bulletin.txt");
        assert_eq!(files[0].size, 5);
        assert!(area.resolve_download("../bulletin.txt").await.is_err());
        assert!(area.resolve_download("nested/file.txt").await.is_err());
        assert!(
            area.resolve_download("bulletin.txt")
                .await
                .unwrap()
                .is_some()
        );

        fs::remove_dir_all(path).unwrap();
    }
}
