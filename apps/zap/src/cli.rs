//! Consultas sobre o arquivo. Abrem o mesmo `archive.db` que o serviço está
//! gravando; o modo WAL permite ler enquanto ele escreve.

use std::time::Duration;

use anyhow::{Result, anyhow};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};

use crate::archive::Archive;

#[derive(QueryableByName)]
struct ContactRow {
    #[diesel(sql_type = Text)]
    chat: String,
    #[diesel(sql_type = Nullable<Text>)]
    name: Option<String>,
}

#[derive(QueryableByName)]
struct LastId {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

#[derive(QueryableByName)]
struct OutboxStatus {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Nullable<Text>)]
    sent_to: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    error: Option<String>,
}

/// Só dígitos, aceitando `+`, espaços, traços e parênteses na digitação.
fn as_phone(who: &str) -> Option<String> {
    let digits: String = who.chars().filter(char::is_ascii_digit).collect();
    (!digits.is_empty()
        && who
            .chars()
            .all(|ch| ch.is_ascii_digit() || "+-() ".contains(ch)))
    .then_some(digits)
}

/// Escolhe uma única conversa entre as candidatas, ou explica a ambiguidade.
fn pick_one(who: &str, mut rows: Vec<ContactRow>) -> Result<ContactRow> {
    rows.dedup_by(|a, b| a.chat == b.chat);
    match rows.len() {
        0 => Err(anyhow!(
            "nenhuma conversa encontrada para '{who}'; passe o número (ex.: 5569...) ou o JID"
        )),
        1 => Ok(rows.remove(0)),
        _ => {
            let list = rows
                .iter()
                .map(|r| format!("  {}  {}", r.chat, r.name.as_deref().unwrap_or("")))
                .collect::<Vec<_>>()
                .join("\n");
            Err(anyhow!("'{who}' é ambíguo, escolha pelo JID:\n{list}"))
        }
    }
}

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

/// Codifica `s` para a query string de um link (RFC 3986, byte a byte).
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Número de telefone (só dígitos) e nome do contato. Aceita o número
/// (com ou sem `+`, espaços e traços) ou parte do nome de uma conversa
/// individual já arquivada.
fn resolve_contact(c: &mut SqliteConnection, who: &str) -> Result<(String, Option<String>)> {
    if let Some(digits) = as_phone(who) {
        return Ok((digits, None));
    }
    let rows: Vec<ContactRow> = diesel::sql_query(
        "SELECT m.chat AS chat, \
                COALESCE(c.name, MAX(CASE WHEN m.from_me = 0 THEN m.push_name END)) AS name \
         FROM messages m LEFT JOIN chats c ON c.jid = m.chat \
         WHERE m.chat LIKE '%@s.whatsapp.net' \
           AND (c.name LIKE ?1 OR (m.from_me = 0 AND m.push_name LIKE ?1)) \
         GROUP BY m.chat ORDER BY MAX(m.ts) DESC LIMIT 10",
    )
    .bind::<Text, _>(format!("%{who}%"))
    .load(c)?;
    let one = pick_one(who, rows)?;
    let number = one.chat.split('@').next().unwrap_or_default().to_string();
    Ok((number, one.name))
}

/// Destinatário para a fila de envio e um rótulo legível. Aceita `me`,
/// número, JID completo ou parte do nome de qualquer conversa (grupos inclusive).
fn resolve_target(c: &mut SqliteConnection, who: &str) -> Result<(String, String)> {
    if who == "me" {
        return Ok(("me".to_string(), "você mesmo".to_string()));
    }
    if who.contains('@') {
        return Ok((who.to_string(), who.to_string()));
    }
    if let Some(digits) = as_phone(who) {
        return Ok((digits.clone(), format!("+{digits}")));
    }
    let rows: Vec<ContactRow> = diesel::sql_query(
        "SELECT jid AS chat, name FROM chats WHERE name LIKE ?1 \
         UNION \
         SELECT chat, MAX(push_name) AS name FROM messages \
         WHERE from_me = 0 AND push_name LIKE ?1 AND chat NOT LIKE '%@g.us' GROUP BY chat \
         ORDER BY chat LIMIT 10",
    )
    .bind::<Text, _>(format!("%{who}%"))
    .load(c)?;
    let one = pick_one(who, rows)?;
    let label = match &one.name {
        Some(n) => format!("{n} ({})", one.chat),
        None => one.chat.clone(),
    };
    Ok((one.chat, label))
}

/// Põe a mensagem na fila do serviço e espera a confirmação do envio.
pub fn send(archive: &Archive, to: &str, text: &str, wait_secs: u64) -> Result<()> {
    let mut c = archive.conn()?;
    let (recipient, label) = resolve_target(&mut c, to)?;
    diesel::sql_query("INSERT INTO outbox (created_at, recipient, text) VALUES (?, ?, ?)")
        .bind::<BigInt, _>(wacore::time::now_millis())
        .bind::<Text, _>(recipient)
        .bind::<Text, _>(text)
        .execute(&mut *c)?;
    let id = diesel::sql_query("SELECT last_insert_rowid() AS id")
        .get_result::<LastId>(&mut *c)?
        .id;
    drop(c);
    println!("#{id} para {label}: na fila");

    for _ in 0..wait_secs * 2 {
        std::thread::sleep(Duration::from_millis(500));
        let st = diesel::sql_query("SELECT status, sent_to, error FROM outbox WHERE id = ?")
            .bind::<BigInt, _>(id)
            .get_result::<OutboxStatus>(&mut *archive.conn()?)?;
        match st.status.as_str() {
            "sent" => {
                println!(
                    "#{id} enviada para {}",
                    st.sent_to.as_deref().unwrap_or("?")
                );
                return Ok(());
            }
            "failed" => {
                return Err(anyhow!(
                    "#{id} não enviada: {}",
                    st.error.as_deref().unwrap_or("?")
                ));
            }
            _ => {}
        }
    }
    println!("#{id} ainda na fila após {wait_secs}s; o serviço envia quando reconectar.");
    Ok(())
}

/// Imprime um link `wa.me` que abre a conversa no app oficial com o texto
/// pronto. O zap não envia nada: quem envia é o dono, tocando no link que
/// recebe por um canal próprio (o agente, por exemplo).
pub fn link(archive: &Archive, to: &str, text: &str) -> Result<()> {
    let mut c = archive.conn()?;
    let (number, name) = resolve_contact(&mut c, to)?;
    let label = match &name {
        Some(n) => format!("{n} (+{number})"),
        None => format!("+{number}"),
    };
    println!("para: {label}");
    println!("https://wa.me/{number}?text={}", percent_encode(text));
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
