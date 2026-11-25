//! `zap run`: mantém a sessão conectada e arquiva tudo.
//!
//! - Mensagens: gancho de durabilidade. O SDK só confirma a mensagem ao
//!   servidor depois que ela foi gravada; se o processo cair antes, o servidor
//!   reentrega. A gravação é idempotente (`INSERT OR IGNORE` por chat + id).
//! - Histórico: cada bloco é gravado cru antes do recibo `hist_sync`, e depois
//!   projetado em `messages`/`chats` quando o evento `HistorySync` chega.
//! - Todo o resto: cada evento vira uma linha JSON em `events`.
//! - Envio: `zap send` grava na tabela `outbox`; este serviço, dono da
//!   sessão, envia e arquiva a mensagem enviada (`origin = 'sent'`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use log::{error, info, warn};
use serde_json::Value;
use whatsapp_rust::pair_code::PairCodeOptions;
use whatsapp_rust::prelude::*;
use whatsapp_rust::{HistorySyncCaptureHook, InboundDurabilityHook};

use crate::archive::{
    Archive, MessageRow, OutboxItem, SendOutcome, field, field_i64, field_str, kind_and_text,
};

pub struct RunOptions {
    pub session_db: String,
    pub archive_db: String,
    pub phone: Option<String>,
}

#[derive(Clone)]
struct Hooks {
    archive: Archive,
}

fn live_row(m: &InboundMessage) -> Result<MessageRow> {
    let message_json = serde_json::to_value(&*m.message)?;
    let (kind, text) = kind_and_text(&message_json);
    let push_name = m.info.push_name.to_string();
    Ok(MessageRow {
        chat: m.info.source.chat.to_string(),
        id: m.info.id.to_string(),
        sender: Some(m.info.source.sender.to_string()),
        from_me: m.info.source.is_from_me,
        ts: Some(m.info.timestamp.as_secs()),
        push_name: (!push_name.is_empty()).then_some(push_name),
        kind,
        text,
        origin: "live",
        json: serde_json::to_string(m)?,
        proto: Some(waproto::codec::message_to_vec(&m.message)),
    })
}

#[async_trait::async_trait]
impl InboundDurabilityHook for Hooks {
    async fn on_messages(&self, _client: Arc<Client>, batch: &[InboundMessage]) -> Result<()> {
        let rows = batch.iter().map(live_row).collect::<Result<Vec<_>>>()?;
        let n = self.archive.insert_messages(rows).await?;
        if n > 0 {
            info!("arquivadas {n} mensagem(ns)");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl HistorySyncCaptureHook for Hooks {
    async fn on_history_sync(
        &self,
        _client: Arc<Client>,
        message_id: &str,
        sync_type: Option<wa::message::HistorySyncType>,
        compressed: &[u8],
    ) -> Result<()> {
        self.archive
            .insert_history_chunk(
                message_id.to_string(),
                sync_type.map(|t| format!("{t:?}")),
                compressed.to_vec(),
            )
            .await?;
        info!(
            "bloco de histórico {message_id} capturado ({} bytes)",
            compressed.len()
        );
        Ok(())
    }
}

/// Lê um bloco de histórico decodificado e extrai conversas e mensagens.
fn history_rows(hs: &Value) -> (Vec<(String, Option<String>)>, Vec<MessageRow>) {
    let mut chats = Vec::new();
    let mut rows = Vec::new();
    let conversations = field(hs, &["conversations"])
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    for conv in conversations {
        let Some(conv_jid) = field_str(conv, &["id"]) else {
            continue;
        };
        let name = field_str(conv, &["name", "displayName", "display_name"]);
        chats.push((conv_jid.clone(), name));
        let messages = field(conv, &["messages"])
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for item in messages {
            let Some(wmi) = field(item, &["message"]) else {
                continue;
            };
            let key = field(wmi, &["key"]);
            let Some(id) = key.and_then(|k| field_str(k, &["id"])) else {
                continue;
            };
            let chat = key
                .and_then(|k| field_str(k, &["remoteJid", "remote_jid"]))
                .unwrap_or_else(|| conv_jid.clone());
            let from_me = key
                .and_then(|k| field(k, &["fromMe", "from_me"]))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let sender = key
                .and_then(|k| field_str(k, &["participant"]))
                .or_else(|| field_str(wmi, &["participant"]))
                .or_else(|| (!from_me).then(|| chat.clone()));
            let (kind, text) = field(wmi, &["message"])
                .map(kind_and_text)
                .unwrap_or((None, None));
            rows.push(MessageRow {
                chat,
                id,
                sender,
                from_me,
                ts: field_i64(wmi, &["messageTimestamp", "message_timestamp"]),
                push_name: field_str(wmi, &["pushName", "push_name"]),
                kind,
                text,
                origin: "history",
                json: wmi.to_string(),
                proto: None,
            });
        }
    }
    (chats, rows)
}

async fn project_history(
    archive: &Archive,
    lazy: &wacore::types::events::LazyHistorySync,
) -> Result<()> {
    let lazy = lazy.clone();
    let (chats, rows) = tokio::task::spawn_blocking(move || -> Result<_> {
        let bytes = lazy.decompress()?;
        let hs = waproto::codec::history_sync_decode(&bytes)
            .map_err(|e| anyhow!("decodificando HistorySync: {e:?}"))?;
        let value = serde_json::to_value(&hs)?;
        Ok(history_rows(&value))
    })
    .await
    .map_err(|e| anyhow!("tarefa de histórico falhou: {e}"))??;
    let (n_chats, n_rows) = (chats.len(), rows.len());
    archive.upsert_chats(chats).await?;
    let inserted = archive.insert_messages(rows).await?;
    info!("histórico: {n_chats} conversas, {n_rows} mensagens lidas, {inserted} novas");
    Ok(())
}

const MAX_SEND_ATTEMPTS: i32 = 5;

/// Variantes de um número brasileiro de celular, com e sem o nono dígito:
/// contas antigas seguem registradas sem ele.
fn phone_candidates(digits: &str) -> Vec<String> {
    let mut out = vec![digits.to_string()];
    if digits.starts_with("55") {
        if digits.len() == 13 && digits.as_bytes()[4] == b'9' {
            out.push(format!("{}{}", &digits[..4], &digits[5..]));
        } else if digits.len() == 12 {
            out.push(format!("{}9{}", &digits[..4], &digits[4..]));
        }
    }
    out
}

/// Resolve o destinatário da fila. `Err((motivo, desistir))`.
async fn resolve_recipient(client: &Client, recipient: &str) -> Result<Jid, (String, bool)> {
    if recipient == "me" {
        return client
            .pn()
            .map(|j| j.to_non_ad())
            .ok_or_else(|| ("sessão sem número próprio (não pareada?)".to_string(), true));
    }
    if recipient.contains('@') {
        return recipient
            .parse()
            .map_err(|e| (format!("JID inválido '{recipient}': {e}"), true));
    }
    if recipient.is_empty() || !recipient.chars().all(|c| c.is_ascii_digit()) {
        return Err((format!("destinatário inválido '{recipient}'"), true));
    }
    let jids: Vec<Jid> = phone_candidates(recipient)
        .into_iter()
        .map(Jid::pn)
        .collect();
    let results = client
        .contacts()
        .is_on_whatsapp(&jids)
        .await
        .map_err(|e| (format!("consultando o número: {e}"), false))?;
    results
        .into_iter()
        .find(|r| r.is_registered)
        .map(|r| r.jid.to_non_ad())
        .ok_or_else(|| (format!("+{recipient} não tem WhatsApp"), true))
}

async fn send_one(client: &Arc<Client>, item: &OutboxItem) -> SendOutcome {
    let last_try = item.attempts + 1 >= MAX_SEND_ATTEMPTS;
    let jid = match resolve_recipient(client, &item.recipient).await {
        Ok(jid) => jid,
        Err((error, give_up)) => {
            warn!("envio #{}: {error}", item.id);
            return SendOutcome::Failed {
                error,
                give_up: give_up || last_try,
            };
        }
    };
    match client
        .send_message(jid, wa::Message::text(item.text.as_str()))
        .await
    {
        Ok(sent) => {
            let to = sent.to.to_string();
            let message_id = sent.message_id.to_string();
            info!("enviada {message_id} para {to} (fila #{})", item.id);
            let row = MessageRow {
                chat: to.clone(),
                id: message_id.clone(),
                sender: client.pn().map(|j| j.to_non_ad().to_string()),
                from_me: true,
                ts: Some(wacore::time::now_millis() / 1000),
                push_name: None,
                kind: Some("conversation".to_string()),
                text: Some(item.text.clone()),
                origin: "sent",
                json: serde_json::json!({
                    "outbox_id": item.id,
                    "to": to,
                    "message_id": message_id,
                })
                .to_string(),
                proto: Some(waproto::codec::message_to_vec(&sent.message)),
            };
            SendOutcome::Sent {
                to,
                message_id,
                row: Box::new(row),
            }
        }
        Err(e) => {
            warn!(
                "envio #{} falhou (tentativa {}): {e}",
                item.id,
                item.attempts + 1
            );
            SendOutcome::Failed {
                error: e.to_string(),
                give_up: last_try,
            }
        }
    }
}

/// Esvazia a fila `outbox` enquanto a sessão estiver ativa.
async fn outbox_loop(archive: Archive, client: Arc<Client>) {
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if !client.is_logged_in() {
            continue;
        }
        let items = match archive.pending_outbox().await {
            Ok(items) => items,
            Err(e) => {
                error!("lendo fila de envio: {e:#}");
                continue;
            }
        };
        for item in items {
            let outcome = send_one(&client, &item).await;
            if let Err(e) = archive.finish_outbox(item.id, outcome).await {
                error!("registrando envio #{}: {e:#}", item.id);
            }
        }
    }
}

async fn record_event(archive: &Archive, event: &Event) -> Result<()> {
    match event {
        // Já gravadas pelo gancho de durabilidade. Newsletters e mensagens
        // recuperadas por PDO passam por fora do gancho: gravar aqui.
        Event::Messages(batch) => {
            if !batch.hook_committed {
                let rows = batch.iter().map(live_row).collect::<Result<Vec<_>>>()?;
                archive.insert_messages(rows).await?;
            }
            return Ok(());
        }
        Event::HistorySync(lazy) => {
            project_history(archive, lazy).await?;
            let meta = serde_json::json!({
                "sync_type": lazy.sync_type(),
                "chunk_order": lazy.chunk_order(),
                "progress": lazy.progress(),
            });
            return archive
                .insert_event("HistorySync".to_string(), meta.to_string())
                .await;
        }
        _ => {}
    }
    let value = serde_json::to_value(event)?;
    let kind = match &value {
        Value::Object(map) if map.len() == 1 => map.keys().next().cloned(),
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
    .unwrap_or_else(|| "Unknown".to_string());
    archive.insert_event(kind, value.to_string()).await
}

pub async fn run(opts: RunOptions) -> Result<()> {
    let archive = Archive::open(&opts.archive_db)?;
    info!("arquivo aberto em {}", opts.archive_db);
    let store = SqliteStore::open(&opts.session_db)
        .await
        .map_err(|e| anyhow!("abrindo sessão {}: {e}", opts.session_db))?;

    let hooks = Hooks {
        archive: archive.clone(),
    };
    let events_archive = archive.clone();
    let mut builder = Bot::builder()
        .with_backend(store)
        .with_inbound_durability_hook(hooks.clone())
        .with_history_sync_capture_hook(hooks)
        .on_event(move |event, _client| {
            let archive = events_archive.clone();
            async move {
                if let Err(e) = record_event(&archive, &event).await {
                    error!("falha ao arquivar evento: {e:#}");
                }
            }
        })
        .on_qr_code(|code, timeout| async move {
            info!("QR code (válido por {}s):\n{code}", timeout.as_secs());
        })
        .on_pair_code(|code, timeout| async move {
            info!(
                "CÓDIGO DE PAREAMENTO (válido por {}s): >>> {code} <<<",
                timeout.as_secs()
            );
        })
        .on_connected(|_client| async {
            info!("conectado");
        })
        .on_logged_out(|_info| async {
            warn!("sessão desconectada pelo celular (logged out)");
        });

    if let Some(phone) = opts.phone {
        builder = builder.with_pair_code(PairCodeOptions {
            phone_number: phone,
            ..Default::default()
        });
    }

    let bot = builder
        .build()
        .await
        .map_err(|e| anyhow!("criando cliente: {e}"))?;
    let mut handle = bot.spawn();
    tokio::spawn(outbox_loop(archive, handle.client()));
    tokio::select! {
        _ = &mut handle => {}
        _ = shutdown_signal() => {
            info!("sinal de parada recebido, encerrando...");
            let report = handle.shutdown().await;
            info!("encerramento: {report:?}");
        }
    }
    Ok(())
}
