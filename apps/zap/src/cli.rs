//! Consultas sobre o arquivo. Abrem o mesmo `archive.db` que o serviço está
//! gravando; o modo WAL permite ler enquanto ele escreve.

use anyhow::Result;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};

use crate::archive::Archive;

#[derive(QueryableByName)]
struct ChatRow {
    #[diesel(sql_type = Text)]
    chat: String,
    #[diesel(sql_type = Nullable<Text>)]
    name: Option<String>,
    #[diesel(sql_type = BigInt)]
    total: i64,
    #[diesel(sql_type = Nullable<Text>)]
    last: Option<String>,
}

#[derive(QueryableByName)]
struct MsgRow {
    #[diesel(sql_type = Text)]
    chat: String,
    #[diesel(sql_type = Nullable<Text>)]
    quando: Option<String>,
    #[diesel(sql_type = Integer)]
    from_me: i32,
    #[diesel(sql_type = Nullable<Text>)]
    who: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    kind: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    text: Option<String>,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = Text)]
    label: String,
    #[diesel(sql_type = BigInt)]
    total: i64,
}

#[derive(QueryableByName)]
struct EventRow {
    #[diesel(sql_type = BigInt)]
    seq: i64,
    #[diesel(sql_type = Text)]
    quando: String,
    #[diesel(sql_type = Text)]
    kind: String,
    #[diesel(sql_type = Text)]
    json: String,
}

const MSG_COLUMNS: &str = "m.chat, datetime(m.ts, 'unixepoch', '-4 hours') AS quando, m.from_me, \
     COALESCE(m.push_name, m.sender) AS who, m.kind, m.text";

fn print_messages(rows: &[MsgRow], show_chat: bool) {
    for r in rows {
        let who = if r.from_me != 0 {
            "eu".to_string()
        } else {
            r.who.clone().unwrap_or_else(|| "?".to_string())
        };
        let body = match (&r.text, &r.kind) {
            (Some(t), _) => t.replace('\n', " ⏎ "),
            (None, Some(k)) => format!("[{k}]"),
            (None, None) => "[vazio]".to_string(),
        };
        let quando = r.quando.as_deref().unwrap_or("????-??-?? ??:??:??");
        if show_chat {
            println!("{quando}  {}  {who}: {body}", r.chat);
        } else {
            println!("{quando}  {who}: {body}");
        }
    }
}

pub fn chats(archive: &Archive, limit: i64) -> Result<()> {
    let mut c = archive.conn()?;
    let rows: Vec<ChatRow> = diesel::sql_query(
        "SELECT m.chat AS chat, \
                COALESCE(c.name, MAX(CASE WHEN m.from_me = 0 THEN m.push_name END)) AS name, \
                COUNT(*) AS total, \
                datetime(MAX(m.ts), 'unixepoch', '-4 hours') AS last \
         FROM messages m LEFT JOIN chats c ON c.jid = m.chat \
         GROUP BY m.chat ORDER BY MAX(m.ts) DESC LIMIT ?",
    )
    .bind::<BigInt, _>(limit)
    .load(&mut *c)?;
    for r in rows {
        println!(
            "{}  {:>6}  {}  {}",
            r.last.as_deref().unwrap_or("-"),
            r.total,
            r.chat,
            r.name.as_deref().unwrap_or("")
        );
    }
    Ok(())
}

/// Mensagens de uma conversa, identificada pelo JID ou por parte do nome.
pub fn chat(archive: &Archive, who: &str, limit: i64) -> Result<()> {
    let mut c = archive.conn()?;
    let pattern = format!("%{who}%");
    let rows: Vec<MsgRow> = diesel::sql_query(format!(
        "SELECT * FROM (SELECT {MSG_COLUMNS} FROM messages m \
         WHERE m.chat = ?1 OR m.chat IN ( \
             SELECT jid FROM chats WHERE name LIKE ?2 \
             UNION SELECT chat FROM messages WHERE push_name LIKE ?2 AND from_me = 0 AND chat NOT LIKE '%@g.us' \
             UNION SELECT chat FROM messages WHERE chat LIKE ?2) \
         ORDER BY m.ts DESC LIMIT ?3) ORDER BY quando ASC"
    ))
    .bind::<Text, _>(who)
    .bind::<Text, _>(pattern)
    .bind::<BigInt, _>(limit)
    .load(&mut *c)?;
    print_messages(&rows, true);
    Ok(())
}

pub fn recent(archive: &Archive, limit: i64) -> Result<()> {
    let mut c = archive.conn()?;
    let rows: Vec<MsgRow> = diesel::sql_query(format!(
        "SELECT * FROM (SELECT {MSG_COLUMNS} FROM messages m ORDER BY m.ts DESC LIMIT ?) \
         ORDER BY quando ASC"
    ))
    .bind::<BigInt, _>(limit)
    .load(&mut *c)?;
    print_messages(&rows, true);
    Ok(())
}

pub fn search(archive: &Archive, query: &str, limit: i64) -> Result<()> {
    let mut c = archive.conn()?;
    let fts: Result<Vec<MsgRow>, _> = diesel::sql_query(format!(
        "SELECT {MSG_COLUMNS} FROM messages_fts f JOIN messages m ON m.rowid = f.rowid \
         WHERE messages_fts MATCH ? ORDER BY m.ts DESC LIMIT ?"
    ))
    .bind::<Text, _>(query)
    .bind::<BigInt, _>(limit)
    .load(&mut *c);
    let rows = match fts {
        Ok(rows) => rows,
        Err(_) => diesel::sql_query(format!(
            "SELECT {MSG_COLUMNS} FROM messages m WHERE m.text LIKE ? ORDER BY m.ts DESC LIMIT ?"
        ))
        .bind::<Text, _>(format!("%{query}%"))
        .bind::<BigInt, _>(limit)
        .load(&mut *c)?,
    };
    print_messages(&rows, true);
    Ok(())
}

pub fn stats(archive: &Archive) -> Result<()> {
    let mut c = archive.conn()?;
    let totals: Vec<CountRow> = diesel::sql_query(
        "SELECT 'mensagens (' || origin || ')' AS label, COUNT(*) AS total FROM messages GROUP BY origin \
         UNION ALL SELECT 'conversas', COUNT(DISTINCT chat) FROM messages \
         UNION ALL SELECT 'blocos de histórico', COUNT(*) FROM history_chunks \
         UNION ALL SELECT 'eventos', COUNT(*) FROM events",
    )
    .load(&mut *c)?;
    for r in totals {
        println!("{:>8}  {}", r.total, r.label);
    }
    println!("-- eventos por tipo:");
    let kinds: Vec<CountRow> = diesel::sql_query(
        "SELECT kind AS label, COUNT(*) AS total FROM events GROUP BY kind ORDER BY total DESC",
    )
    .load(&mut *c)?;
    for r in kinds {
        println!("{:>8}  {}", r.total, r.label);
    }
    Ok(())
}

pub fn events(archive: &Archive, kind: Option<&str>, limit: i64) -> Result<()> {
    let mut c = archive.conn()?;
    let rows: Vec<EventRow> = diesel::sql_query(
        "SELECT seq, datetime(received_at / 1000, 'unixepoch', '-4 hours') AS quando, kind, json \
         FROM events WHERE (?1 IS NULL OR kind = ?1) ORDER BY seq DESC LIMIT ?2",
    )
    .bind::<Nullable<Text>, _>(kind)
    .bind::<BigInt, _>(limit)
    .load(&mut *c)?;
    for r in rows.iter().rev() {
        println!("#{} {} {} {}", r.seq, r.quando, r.kind, r.json);
    }
    Ok(())
}
