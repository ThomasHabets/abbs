use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

use crate::callsign::Callsign;

#[derive(Clone)]
pub struct MailStore {
    path: Arc<PathBuf>,
}

#[derive(Debug, PartialEq)]
pub struct MessageSummary {
    pub id: i64,
    pub sender: Callsign,
    pub recipient: Option<Callsign>,
    pub subject: String,
    pub created_at: String,
}

#[derive(Debug, PartialEq)]
pub struct Message {
    pub id: i64,
    pub sender: Callsign,
    pub recipient: Option<Callsign>,
    pub subject: String,
    pub body: String,
    pub created_at: String,
}

impl MailStore {
    pub async fn open(path: PathBuf) -> Result<Self> {
        let path = Arc::new(path);
        let initialization_path = path.clone();
        tokio::task::spawn_blocking(move || initialize(&initialization_path))
            .await
            .context("database initialization task failed")??;
        Ok(Self { path })
    }

    pub async fn save(
        &self,
        sender: Callsign,
        recipient: Option<Callsign>,
        subject: String,
        body: String,
    ) -> Result<i64> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path)?;
            let is_public = i64::from(recipient.is_none());
            connection.execute(
                "INSERT INTO messages \
                 (sender_callsign, recipient_callsign, is_public, subject, body) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    sender.as_str(),
                    recipient.as_ref().map(Callsign::as_str),
                    is_public,
                    subject,
                    body,
                ],
            )?;
            Ok(connection.last_insert_rowid())
        })
        .await
        .context("database save task failed")?
    }

    pub async fn list_visible(&self, viewer: Callsign) -> Result<Vec<MessageSummary>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path)?;
            let mut statement = connection.prepare(
                "SELECT id, sender_callsign, recipient_callsign, subject, created_at
                 FROM messages
                 WHERE is_public = 1 OR recipient_callsign = ?1
                 ORDER BY id DESC",
            )?;
            let messages = statement
                .query_map(params![viewer.as_str()], |row| {
                    Ok(MessageSummary {
                        id: row.get(0)?,
                        sender: Callsign::parse(&row.get::<_, String>(1)?).map_err(to_sql_error)?,
                        recipient: row
                            .get::<_, Option<String>>(2)?
                            .map(|value| Callsign::parse(&value).map_err(to_sql_error))
                            .transpose()?,
                        subject: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(messages)
        })
        .await
        .context("database list task failed")?
    }

    pub async fn list_sent(&self, sender: Callsign) -> Result<Vec<MessageSummary>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path)?;
            let mut statement = connection.prepare(
                "SELECT id, sender_callsign, recipient_callsign, subject, created_at
                 FROM messages
                 WHERE sender_callsign = ?1
                 ORDER BY id DESC",
            )?;
            let messages = statement
                .query_map(params![sender.as_str()], |row| {
                    Ok(MessageSummary {
                        id: row.get(0)?,
                        sender: Callsign::parse(&row.get::<_, String>(1)?).map_err(to_sql_error)?,
                        recipient: row
                            .get::<_, Option<String>>(2)?
                            .map(|value| Callsign::parse(&value).map_err(to_sql_error))
                            .transpose()?,
                        subject: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(messages)
        })
        .await
        .context("database sent-list task failed")?
    }

    pub async fn read_visible(&self, viewer: Callsign, id: i64) -> Result<Option<Message>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path)?;
            connection
                .query_row(
                    "SELECT id, sender_callsign, recipient_callsign, subject, body, created_at
                     FROM messages
                     WHERE id = ?1 AND (is_public = 1 OR recipient_callsign = ?2)",
                    params![id, viewer.as_str()],
                    |row| {
                        Ok(Message {
                            id: row.get(0)?,
                            sender: Callsign::parse(&row.get::<_, String>(1)?)
                                .map_err(to_sql_error)?,
                            recipient: row
                                .get::<_, Option<String>>(2)?
                                .map(|value| Callsign::parse(&value).map_err(to_sql_error))
                                .transpose()?,
                            subject: row.get(3)?,
                            body: row.get(4)?,
                            created_at: row.get(5)?,
                        })
                    },
                )
                .optional()
                .map_err(Into::into)
        })
        .await
        .context("database read task failed")?
    }

    pub async fn delete_authorized(&self, caller: Callsign, id: i64) -> Result<bool> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path)?;
            let deleted = connection.execute(
                "DELETE FROM messages
                 WHERE id = ?1
                   AND (sender_callsign = ?2 OR recipient_callsign = ?2)",
                params![id, caller.as_str()],
            )?;
            Ok(deleted != 0)
        })
        .await
        .context("database delete task failed")?
    }
}

fn initialize(path: &Path) -> Result<()> {
    let connection = open_connection(path)?;
    connection.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE IF NOT EXISTS messages (
             id INTEGER PRIMARY KEY,
             sender_callsign TEXT NOT NULL,
             recipient_callsign TEXT,
             is_public INTEGER NOT NULL CHECK (is_public IN (0, 1)),
             subject TEXT NOT NULL,
             body TEXT NOT NULL,
             created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
             CHECK (
                 (is_public = 1 AND recipient_callsign IS NULL) OR
                 (is_public = 0 AND recipient_callsign IS NOT NULL)
             )
         );
         CREATE INDEX IF NOT EXISTS messages_visible_by_recipient
             ON messages (recipient_callsign, id DESC);
         CREATE INDEX IF NOT EXISTS messages_by_sender
             ON messages (sender_callsign, id DESC);",
    )?;
    Ok(())
}

fn open_connection(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)
        .with_context(|| format!("failed to open SQLite database at {}", path.display()))?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(connection)
}

fn to_sql_error(error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::MailStore;
    use crate::callsign::Callsign;

    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

    fn database_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "abbs-store-test-{}-{}.sqlite3",
            std::process::id(),
            NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[tokio::test]
    async fn public_and_private_messages_have_correct_visibility() {
        let path = database_path();
        let store = MailStore::open(path.clone()).await.unwrap();
        let alice = Callsign::parse("M0ALICE").unwrap();
        let bob = Callsign::parse("M0BOB").unwrap();

        let private_id = store
            .save(
                alice.clone(),
                Some(bob.clone()),
                "Private".into(),
                "secret".into(),
            )
            .await
            .unwrap();
        let public_id = store
            .save(alice, None, "Public".into(), "hello everyone".into())
            .await
            .unwrap();

        let bob_messages = store.list_visible(bob.clone()).await.unwrap();
        assert_eq!(bob_messages.len(), 2);
        assert_eq!(bob_messages[0].id, public_id);
        assert_eq!(
            store
                .read_visible(bob.clone(), private_id)
                .await
                .unwrap()
                .unwrap()
                .body,
            "secret"
        );

        let eve = Callsign::parse("M0EVE").unwrap();
        assert_eq!(store.list_visible(eve.clone()).await.unwrap().len(), 1);
        assert!(
            store
                .read_visible(eve.clone(), private_id)
                .await
                .unwrap()
                .is_none()
        );

        let alice = Callsign::parse("M0ALICE").unwrap();
        let sent = store.list_sent(alice.clone()).await.unwrap();
        assert_eq!(
            sent.iter().map(|message| message.id).collect::<Vec<_>>(),
            vec![public_id, private_id]
        );

        assert!(!store.delete_authorized(eve, private_id).await.unwrap());
        assert!(
            store
                .delete_authorized(bob.clone(), private_id)
                .await
                .unwrap()
        );
        assert!(store.read_visible(bob, private_id).await.unwrap().is_none());
        assert!(
            store
                .delete_authorized(alice.clone(), public_id)
                .await
                .unwrap()
        );
        assert!(store.list_sent(alice).await.unwrap().is_empty());

        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
    }
}
