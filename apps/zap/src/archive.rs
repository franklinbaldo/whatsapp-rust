//! Banco do arquivo (`archive.db`), separado do banco de sessão da biblioteca.
//!
//! Três camadas:
//! - `events`: todo evento recebido, serializado em JSON, só inclusão;
//! - `history_chunks`: cada bloco de histórico exatamente como o celular enviou
//!   (zlib + protobuf), a cópia sem perdas;
//! - `messages` / `chats` (+ `messages_fts`): projeções para consulta.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Binary, Integer, Nullable, Text};
use serde_json::Value;

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at INTEGER NOT NULL,
    kind        TEXT    NOT NULL,
    json        TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS events_kind ON events (kind, received_at);

CREATE TABLE IF NOT EXISTS history_chunks (
    message_id  TEXT PRIMARY KEY,
    sync_type   TEXT,
    captured_at INTEGER NOT NULL,
    compressed  BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    chat        TEXT    NOT NULL,
    id          TEXT    NOT NULL,
    sender      TEXT,
    from_me     INTEGER NOT NULL,
    ts          INTEGER,
    push_name   TEXT,
    kind        TEXT,
    text        TEXT,
    origin      TEXT    NOT NULL,
    archived_at INTEGER NOT NULL,
    json        TEXT    NOT NULL,
    proto       BLOB,
    PRIMARY KEY (chat, id)
);
CREATE INDEX IF NOT EXISTS messages_chat_ts ON messages (chat, ts);
CREATE INDEX IF NOT EXISTS messages_ts ON messages (ts);

CREATE TABLE IF NOT EXISTS chats (
    jid        TEXT PRIMARY KEY,
    name       TEXT,
    updated_at INTEGER NOT NULL
);
"#;

/// Busca textual. Separada do schema principal: se o SQLite não tiver FTS5,
/// o arquivo continua funcionando e a busca cai para LIKE.
const FTS_SCHEMA: &str = r#"
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
    USING fts5(text, content='messages', content_rowid='rowid');
CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages
    WHEN new.text IS NOT NULL
BEGIN
    INSERT INTO messages_fts (rowid, text) VALUES (new.rowid, new.text);
END;
"#;

/// Uma mensagem pronta para gravar.
pub struct MessageRow {
    pub chat: String,
    pub id: String,
    pub sender: Option<String>,
    pub from_me: bool,
    pub ts: Option<i64>,
    pub push_name: Option<String>,
    pub kind: Option<String>,
    pub text: Option<String>,
    /// `live` (tempo real) ou `history` (histórico do pareamento).
    pub origin: &'static str,
    pub json: String,
    pub proto: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct Archive {
    conn: Arc<Mutex<SqliteConnection>>,
}

impl Archive {
    pub fn open(path: &str) -> Result<Self> {
        let mut conn =
            SqliteConnection::establish(path).with_context(|| format!("abrindo {path}"))?;
        conn.batch_execute("PRAGMA busy_timeout = 10000;")?;
        conn.batch_execute(SCHEMA)
            .with_context(|| format!("criando schema em {path}"))?;
        if let Err(e) = conn.batch_execute(FTS_SCHEMA) {
            log::warn!("FTS5 indisponível, busca vai usar LIKE: {e}");
        }
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Executa `f` com a conexão numa thread de bloqueio (I/O fora do runtime).
    pub async fn with_conn<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> Result<T> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn
                .lock()
                .map_err(|_| anyhow!("lock do banco envenenado"))?;
            f(&mut guard)
        })
        .await
        .map_err(|e| anyhow!("tarefa do banco falhou: {e}"))?
    }

    pub fn conn(&self) -> Result<std::sync::MutexGuard<'_, SqliteConnection>> {
        self.conn
            .lock()
            .map_err(|_| anyhow!("lock do banco envenenado"))
    }

    pub async fn insert_event(&self, kind: String, json: String) -> Result<()> {
        let now = wacore::time::now_millis();
        self.with_conn(move |c| {
            diesel::sql_query("INSERT INTO events (received_at, kind, json) VALUES (?, ?, ?)")
                .bind::<BigInt, _>(now)
                .bind::<Text, _>(kind)
                .bind::<Text, _>(json)
                .execute(c)?;
            Ok(())
        })
        .await
    }

    pub async fn insert_history_chunk(
        &self,
        message_id: String,
        sync_type: Option<String>,
        compressed: Vec<u8>,
    ) -> Result<()> {
        let now = wacore::time::now_millis();
        self.with_conn(move |c| {
            diesel::sql_query(
                "INSERT OR IGNORE INTO history_chunks (message_id, sync_type, captured_at, compressed) \
                 VALUES (?, ?, ?, ?)",
            )
            .bind::<Text, _>(message_id)
            .bind::<Nullable<Text>, _>(sync_type)
            .bind::<BigInt, _>(now)
            .bind::<Binary, _>(compressed)
            .execute(c)?;
            Ok(())
        })
        .await
    }

    /// Grava um lote numa transação só: ou entra tudo, ou nada (o SDK só
    /// confirma ao servidor depois que isto retorna `Ok`).
    pub async fn insert_messages(&self, rows: Vec<MessageRow>) -> Result<usize> {
        let now = wacore::time::now_millis();
        self.with_conn(move |c| {
            c.transaction::<_, anyhow::Error, _>(|c| {
                let mut inserted = 0;
                for r in rows {
                    inserted += diesel::sql_query(
                        "INSERT OR IGNORE INTO messages \
                         (chat, id, sender, from_me, ts, push_name, kind, text, origin, archived_at, json, proto) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    )
                    .bind::<Text, _>(r.chat)
                    .bind::<Text, _>(r.id)
                    .bind::<Nullable<Text>, _>(r.sender)
                    .bind::<Integer, _>(i32::from(r.from_me))
                    .bind::<Nullable<BigInt>, _>(r.ts)
                    .bind::<Nullable<Text>, _>(r.push_name)
                    .bind::<Nullable<Text>, _>(r.kind)
                    .bind::<Nullable<Text>, _>(r.text)
                    .bind::<Text, _>(r.origin)
                    .bind::<BigInt, _>(now)
                    .bind::<Text, _>(r.json)
                    .bind::<Nullable<Binary>, _>(r.proto)
                    .execute(c)?;
                }
                Ok(inserted)
            })
        })
        .await
    }

    pub async fn upsert_chats(&self, chats: Vec<(String, Option<String>)>) -> Result<()> {
        let now = wacore::time::now_millis();
        self.with_conn(move |c| {
            c.transaction::<_, anyhow::Error, _>(|c| {
                for (jid, name) in chats {
                    diesel::sql_query(
                        "INSERT INTO chats (jid, name, updated_at) VALUES (?, ?, ?) \
                         ON CONFLICT (jid) DO UPDATE SET \
                         name = COALESCE(excluded.name, chats.name), updated_at = excluded.updated_at",
                    )
                    .bind::<Text, _>(jid)
                    .bind::<Nullable<Text>, _>(name)
                    .bind::<BigInt, _>(now)
                    .execute(c)?;
                }
                Ok(())
            })
        })
        .await
    }
}

/// Primeira chave cujo valor não é nulo, aceitando nomes camelCase e snake_case.
pub fn field<'a>(v: &'a Value, names: &[&str]) -> Option<&'a Value> {
    names
        .iter()
        .filter_map(|n| v.get(*n))
        .find(|x| !x.is_null())
}

pub fn field_str(v: &Value, names: &[&str]) -> Option<String> {
    field(v, names).and_then(|x| match x {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    })
}

pub fn field_i64(v: &Value, names: &[&str]) -> Option<i64> {
    field(v, names).and_then(|x| match x {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    })
}

/// Tipo da mensagem (`conversation`, `imageMessage`...) e texto legível,
/// lidos do JSON do proto `Message`.
pub fn kind_and_text(message: &Value) -> (Option<String>, Option<String>) {
    let Some(obj) = message.as_object() else {
        return (None, None);
    };
    let kind = obj
        .iter()
        .find(|(k, v)| {
            !v.is_null() && !matches!(k.as_str(), "messageContextInfo" | "message_context_info")
        })
        .map(|(k, _)| k.clone());
    let text = field_str(message, &["conversation"])
        .or_else(|| {
            field(message, &["extendedTextMessage", "extended_text_message"])
                .and_then(|m| field_str(m, &["text"]))
        })
        .or_else(|| {
            kind.as_deref()
                .and_then(|k| obj.get(k))
                .and_then(|m| field_str(m, &["caption", "text", "title", "fileName", "file_name"]))
        });
    (kind, text)
}
