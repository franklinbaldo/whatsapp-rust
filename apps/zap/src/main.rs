//! zap: arquivo completo de uma conta WhatsApp.
//!
//!   zap run [--phone 5569...]     serviço: conecta e grava tudo
//!   zap recent [-n 50]            últimas mensagens de todas as conversas
//!   zap chats [-n 30]             conversas, da mais recente para a mais antiga
//!   zap chat <jid|nome> [-n 50]   mensagens de uma conversa
//!   zap search <texto> [-n 50]    busca textual
//!   zap send <me|número|jid|nome> <texto>  envia (pelo serviço em execução)
//!   zap link <número|nome> <texto> link wa.me com a mensagem pronta (não envia)
//!   zap stats                     contagens do arquivo
//!   zap events [--kind K] [-n 20] eventos brutos (JSON)
//!
//! Os horários da CLI saem em UTC-4 (Rondônia).
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod archive;
mod cli;
mod service;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "zap",
    version,
    about = "Arquivo completo de uma conta WhatsApp"
)]
struct Args {
    /// Banco do arquivo (mensagens, eventos, histórico).
    #[arg(
        long,
        global = true,
        env = "ZAP_ARCHIVE_DB",
        default_value = "archive.db"
    )]
    archive_db: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Conecta e arquiva todos os eventos da conta.
    Run {
        /// Banco de sessão da biblioteca (chaves, aparelho pareado).
        #[arg(long, env = "ZAP_SESSION_DB", default_value = "whatsapp.db")]
        session_db: String,
        /// Número para parear por código (só na primeira vez).
        #[arg(long, short)]
        phone: Option<String>,
    },
    /// Últimas mensagens de todas as conversas.
    Recent {
        #[arg(short = 'n', long, default_value_t = 50)]
        limit: i64,
    },
    /// Lista as conversas.
    Chats {
        #[arg(short = 'n', long, default_value_t = 30)]
        limit: i64,
    },
    /// Mensagens de uma conversa (JID exato ou parte do nome/número).
    Chat {
        who: String,
        #[arg(short = 'n', long, default_value_t = 50)]
        limit: i64,
    },
    /// Busca textual nas mensagens.
    Search {
        query: String,
        #[arg(short = 'n', long, default_value_t = 50)]
        limit: i64,
    },
    /// Envia uma mensagem de texto (pelo serviço em execução).
    Send {
        /// `me`, número (5569...), JID ou parte do nome de uma conversa.
        to: String,
        /// Texto da mensagem.
        #[arg(required = true, trailing_var_arg = true)]
        text: Vec<String>,
        /// Segundos aguardando a confirmação do envio.
        #[arg(long, default_value_t = 30)]
        wait: u64,
    },
    /// Link wa.me com a mensagem pronta; o envio é manual, pelo app oficial.
    Link {
        /// Número (5569...) ou parte do nome de um contato.
        to: String,
        /// Texto da mensagem.
        #[arg(required = true, trailing_var_arg = true)]
        text: Vec<String>,
    },
    /// Contagens do arquivo.
    Stats,
    /// Eventos brutos, em JSON.
    Events {
        #[arg(long)]
        kind: Option<String>,
        #[arg(short = 'n', long, default_value_t = 20)]
        limit: i64,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmd::Run { session_db, phone } => {
            env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
                .format(|buf, record| {
                    use std::io::Write;
                    writeln!(
                        buf,
                        "{} [{:<5}] [{}] - {}",
                        wacore::time::now_utc().display_hms(),
                        record.level(),
                        record.target(),
                        record.args()
                    )
                })
                .init();
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            rt.block_on(service::run(service::RunOptions {
                session_db,
                archive_db: args.archive_db,
                phone,
            }))
        }
        cmd => {
            let archive = archive::Archive::open(&args.archive_db)?;
            match cmd {
                Cmd::Recent { limit } => cli::recent(&archive, limit),
                Cmd::Chats { limit } => cli::chats(&archive, limit),
                Cmd::Chat { who, limit } => cli::chat(&archive, &who, limit),
                Cmd::Search { query, limit } => cli::search(&archive, &query, limit),
                Cmd::Send { to, text, wait } => cli::send(&archive, &to, &text.join(" "), wait),
                Cmd::Link { to, text } => cli::link(&archive, &to, &text.join(" ")),
                Cmd::Stats => cli::stats(&archive),
                Cmd::Events { kind, limit } => cli::events(&archive, kind.as_deref(), limit),
                Cmd::Run { .. } => unreachable!(),
            }
        }
    }
}
