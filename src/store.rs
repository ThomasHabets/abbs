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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoginTransport {
    Tcp,
    Ax25,
    Mercury,
}

impl LoginTransport {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Ax25 => "AX.25",
            Self::Mercury => "MERCURY",
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct LoginRecord {
    pub callsign: Callsign,
    pub transport: LoginTransport,
    pub logged_in_at: String,
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
                 (sender_callsign, recipient_callsign, is_public, subject, body, \
                  sender_base_callsign, recipient_base_callsign) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    sender.as_str(),
                    recipient.as_ref().map(Callsign::as_str),
                    is_public,
                    subject,
                    body,
                    sender.base(),
                    recipient.as_ref().map(Callsign::base),
                ],
            )?;
            Ok(connection.last_insert_rowid())
        })
        .await
        .context("database save task failed")?
    }

    pub async fn record_login(&self, callsign: Callsign, transport: LoginTransport) -> Result<()> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path)?;
            connection.execute(
                "INSERT INTO logins (callsign, transport) VALUES (?1, ?2)",
                params![callsign.as_str(), transport.as_str()],
            )?;
            Ok(())
        })
        .await
        .context("login-record task failed")?
    }

    pub async fn recent_logins(&self, limit: usize) -> Result<Vec<LoginRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path)?;
            let mut statement = connection.prepare(
                "SELECT callsign, transport, logged_in_at
                 FROM logins
                 ORDER BY id DESC
                 LIMIT ?1",
            )?;
            let records = statement
                .query_map(params![limit], login_record_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(records)
        })
        .await
        .context("recent-login task failed")?
    }

    pub async fn list_visible(&self, viewer: Callsign) -> Result<Vec<MessageSummary>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path)?;
            let mut statement = connection.prepare(
                "SELECT id, sender_callsign, recipient_callsign, subject, created_at
                 FROM messages
                 WHERE is_public = 1 OR recipient_base_callsign = ?1
                 ORDER BY id DESC",
            )?;
            let messages = statement
                .query_map(params![viewer.base()], |row| {
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
                 WHERE sender_base_callsign = ?1
                 ORDER BY id DESC",
            )?;
            let messages = statement
                .query_map(params![sender.base()], |row| {
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
                     WHERE id = ?1 AND (is_public = 1 OR recipient_base_callsign = ?2)",
                    params![id, viewer.base()],
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
                   AND (sender_base_callsign = ?2 OR recipient_base_callsign = ?2)",
                params![id, caller.base()],
            )?;
            Ok(deleted != 0)
        })
        .await
        .context("database delete task failed")?
    }
}

fn initialize(path: &Path) -> Result<()> {
    let mut connection = open_connection(path)?;
    connection.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE IF NOT EXISTS messages (
             id INTEGER PRIMARY KEY,
             sender_callsign TEXT NOT NULL,
             sender_base_callsign TEXT NOT NULL,
             recipient_callsign TEXT,
             recipient_base_callsign TEXT,
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
             ON messages (sender_callsign, id DESC);
         CREATE TABLE IF NOT EXISTS logins (
             id INTEGER PRIMARY KEY,
             callsign TEXT NOT NULL,
             transport TEXT NOT NULL CHECK (transport IN ('TCP', 'AX.25', 'MERCURY')),
             logged_in_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
         );
         CREATE INDEX IF NOT EXISTS logins_by_recency
             ON logins (id DESC);",
    )?;
    add_base_callsign_columns(&connection)?;
    add_mercury_login_transport(&mut connection)?;
    Ok(())
}

fn add_mercury_login_transport(connection: &mut Connection) -> Result<()> {
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let schema: String = transaction.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'logins'",
        [],
        |row| row.get(0),
    )?;
    if !schema.contains("'MERCURY'") {
        transaction.execute_batch(
            "CREATE TABLE logins_with_mercury (
                 id INTEGER PRIMARY KEY,
                 callsign TEXT NOT NULL,
                 transport TEXT NOT NULL CHECK (transport IN ('TCP', 'AX.25', 'MERCURY')),
                 logged_in_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
             );
             INSERT INTO logins_with_mercury SELECT id, callsign, transport, logged_in_at FROM logins;
             DROP TABLE logins;
             ALTER TABLE logins_with_mercury RENAME TO logins;
             CREATE INDEX logins_by_recency ON logins (id DESC);",
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn add_base_callsign_columns(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(messages)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    if !columns
        .iter()
        .any(|column| column == "sender_base_callsign")
    {
        connection.execute(
            "ALTER TABLE messages ADD COLUMN sender_base_callsign TEXT",
            [],
        )?;
    }
    if !columns
        .iter()
        .any(|column| column == "recipient_base_callsign")
    {
        connection.execute(
            "ALTER TABLE messages ADD COLUMN recipient_base_callsign TEXT",
            [],
        )?;
    }

    connection.execute(
        "UPDATE messages
         SET sender_base_callsign = CASE
             WHEN instr(sender_callsign, '-') > 0
                 THEN substr(sender_callsign, 1, instr(sender_callsign, '-') - 1)
             ELSE sender_callsign
         END
         WHERE sender_base_callsign IS NULL",
        [],
    )?;
    connection.execute(
        "UPDATE messages
         SET recipient_base_callsign = CASE
             WHEN recipient_callsign IS NULL THEN NULL
             WHEN instr(recipient_callsign, '-') > 0
                 THEN substr(recipient_callsign, 1, instr(recipient_callsign, '-') - 1)
             ELSE recipient_callsign
         END
         WHERE recipient_base_callsign IS NULL",
        [],
    )?;
    connection.execute_batch(
        "CREATE INDEX IF NOT EXISTS messages_visible_by_recipient_base
             ON messages (recipient_base_callsign, id DESC);
         CREATE INDEX IF NOT EXISTS messages_by_sender_base
             ON messages (sender_base_callsign, id DESC);",
    )?;
    Ok(())
}

fn open_connection(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)
        .with_context(|| format!("failed to open SQLite database at {}", path.display()))?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(connection)
}

fn login_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LoginRecord> {
    let transport = match row.get::<_, String>(1)?.as_str() {
        "TCP" => LoginTransport::Tcp,
        "AX.25" => LoginTransport::Ax25,
        "MERCURY" => LoginTransport::Mercury,
        other => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid login transport {other:?}"),
                )),
            ));
        }
    };
    Ok(LoginRecord {
        callsign: Callsign::parse(&row.get::<_, String>(0)?).map_err(to_sql_error)?,
        transport,
        logged_in_at: row.get(2)?,
    })
}

fn to_sql_error(error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        error.into_boxed_dyn_error(),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{LoginTransport, MailStore, MessageSummary};
    use crate::callsign::Callsign;
    use rusqlite::{Connection, params};

    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn mercury_login_migration_preserves_existing_history_and_is_repeatable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("legacy.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE logins (
                 id INTEGER PRIMARY KEY,
                 callsign TEXT NOT NULL,
                 transport TEXT NOT NULL CHECK (transport IN ('TCP', 'AX.25')),
                 logged_in_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
             );
             CREATE INDEX logins_by_recency ON logins (id DESC);
             INSERT INTO logins VALUES (7, 'M0ALICE', 'TCP', '2020-01-02T03:04:05.000Z');
             INSERT INTO logins VALUES (42, 'M0BOB', 'AX.25', '2021-01-02T03:04:05.000Z');",
            )
            .unwrap();
        drop(connection);

        let store = MailStore::open(path.clone()).await.unwrap();
        store
            .record_login(Callsign::parse("M0HF").unwrap(), LoginTransport::Mercury)
            .await
            .unwrap();
        let reopened = MailStore::open(path.clone()).await.unwrap();
        let logins = reopened.recent_logins(10).await.unwrap();
        assert_eq!(logins.len(), 3);
        assert_eq!(logins[0].transport, LoginTransport::Mercury);
        assert_eq!(logins[1].callsign.as_str(), "M0BOB");
        assert_eq!(logins[1].transport, LoginTransport::Ax25);
        assert_eq!(logins[1].logged_in_at, "2021-01-02T03:04:05.000Z");
        assert_eq!(logins[2].transport, LoginTransport::Tcp);
        assert_eq!(logins[2].logged_in_at, "2020-01-02T03:04:05.000Z");
        let connection = Connection::open(&path).unwrap();
        let ids: Vec<i64> = connection
            .prepare("SELECT id FROM logins ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(ids, [7, 42, 43]);
        let indexes: i64 = connection.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 'logins_by_recency'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(indexes, 1);
    }

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
        let alice = Callsign::parse("M0ALICE-1").unwrap();
        let bob = Callsign::parse("M0BOB-2").unwrap();

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

        let bob_base = Callsign::parse("M0BOB").unwrap();
        let bob_messages = store.list_visible(bob_base.clone()).await.unwrap();
        assert_eq!(bob_messages.len(), 2);
        assert_eq!(bob_messages[0].id, public_id);
        assert_eq!(
            store
                .read_visible(bob_base.clone(), private_id)
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

        let alice_base = Callsign::parse("M0ALICE").unwrap();
        let sent = store.list_sent(alice_base.clone()).await.unwrap();
        assert_eq!(
            sent.iter().map(|message| message.id).collect::<Vec<_>>(),
            vec![public_id, private_id]
        );

        assert!(!store.delete_authorized(eve, private_id).await.unwrap());
        assert!(
            store
                .delete_authorized(bob_base.clone(), private_id)
                .await
                .unwrap()
        );
        assert!(
            store
                .read_visible(bob_base, private_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .delete_authorized(alice_base.clone(), public_id)
                .await
                .unwrap()
        );
        assert_eq!(
            store.list_sent(alice_base).await.unwrap(),
            [] as [MessageSummary; 0]
        );

        store
            .record_login(Callsign::parse("M0ALICE").unwrap(), LoginTransport::Tcp)
            .await
            .unwrap();
        store
            .record_login(Callsign::parse("M0BOB").unwrap(), LoginTransport::Ax25)
            .await
            .unwrap();
        let logins = store.recent_logins(10).await.unwrap();
        assert_eq!(logins.len(), 2);
        assert_eq!(logins[0].callsign.as_str(), "M0BOB");
        assert_eq!(logins[0].transport, LoginTransport::Ax25);
        assert_eq!(logins[1].transport, LoginTransport::Tcp);

        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[tokio::test]
    async fn legacy_messages_are_backfilled_with_base_callsigns() {
        let path = database_path();
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE messages (
                    id INTEGER PRIMARY KEY,
                    sender_callsign TEXT NOT NULL,
                    recipient_callsign TEXT,
                    is_public INTEGER NOT NULL,
                    subject TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages
                 (sender_callsign, recipient_callsign, is_public, subject, body, created_at)
                 VALUES (?1, ?2, 0, 'Legacy', 'body', '2026-09-14T00:00:00Z')",
                params!["M0ALICE-1", "M0BOB-7"],
            )
            .unwrap();
        drop(connection);

        let store = MailStore::open(path.clone()).await.unwrap();
        let bob = Callsign::parse("M0BOB").unwrap();
        let alice = Callsign::parse("M0ALICE-3").unwrap();

        assert_eq!(store.list_visible(bob.clone()).await.unwrap().len(), 1);
        assert_eq!(store.list_sent(alice).await.unwrap().len(), 1);
        assert!(store.delete_authorized(bob, 1).await.unwrap());

        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
    }
}
