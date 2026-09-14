use anyhow::Result;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    callsign::Callsign,
    store::{LoginTransport, MailStore, Message},
    terminal::Terminal,
};

const MAX_SUBJECT_CHARS: usize = 80;
const MAX_BODY_CHARS: usize = 4_000;
const RECENT_LOGIN_LIMIT: usize = 10;

pub async fn run_session<S>(
    mut terminal: Terminal<S>,
    identity: Callsign,
    bbs_callsign: Callsign,
    store: MailStore,
    show_bbs_welcome: bool,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if show_bbs_welcome {
        terminal
            .write_line(&format!("Welcome to {bbs_callsign} amateur radio BBS."))
            .await?;
    }
    terminal
        .write_line(&format!("Welcome, {identity}."))
        .await?;
    terminal.write_line("Type HELP for commands.").await?;

    loop {
        terminal.write("> ").await?;
        let Some(line) = terminal.read_line().await? else {
            terminal.shutdown().await?;
            return Ok(());
        };

        let mut fields = line.split_whitespace();
        let Some(command) = fields.next() else {
            continue;
        };
        let command = command.to_ascii_uppercase();

        match command.as_str() {
            "HELP" if fields.next().is_none() => write_help(&mut terminal).await?,
            "LIST" if fields.next().is_none() => {
                list_messages(&mut terminal, &store, identity.clone()).await?;
            }
            "SENT" if fields.next().is_none() => {
                list_sent_messages(&mut terminal, &store, identity.clone()).await?;
            }
            "LOGINS" if fields.next().is_none() => {
                list_recent_logins(&mut terminal, &store).await?;
            }
            "READ" => {
                let Some(id) = fields.next() else {
                    terminal.write_line("Usage: READ <id>").await?;
                    continue;
                };
                if fields.next().is_some() {
                    terminal.write_line("Usage: READ <id>").await?;
                    continue;
                }
                match id.parse::<i64>() {
                    Ok(id) if id > 0 => {
                        read_message(&mut terminal, &store, identity.clone(), id).await?;
                    }
                    _ => {
                        terminal
                            .write_line("Message ID must be a positive number.")
                            .await?;
                    }
                }
            }
            "DELETE" => {
                let Some(id) = fields.next() else {
                    terminal.write_line("Usage: DELETE <id>").await?;
                    continue;
                };
                if fields.next().is_some() {
                    terminal.write_line("Usage: DELETE <id>").await?;
                    continue;
                }
                match id.parse::<i64>() {
                    Ok(id) if id > 0 => {
                        delete_message(&mut terminal, &store, identity.clone(), id).await?;
                    }
                    _ => {
                        terminal
                            .write_line("Message ID must be a positive number.")
                            .await?;
                    }
                }
            }
            "SEND" => {
                let Some(recipient) = fields.next() else {
                    terminal.write_line("Usage: SEND <callsign|ALL>").await?;
                    continue;
                };
                if fields.next().is_some() {
                    terminal.write_line("Usage: SEND <callsign|ALL>").await?;
                    continue;
                }
                compose_message(&mut terminal, &store, identity.clone(), recipient).await?;
            }
            "QUIT" if fields.next().is_none() => {
                terminal.write_line("Goodbye.").await?;
                terminal.shutdown().await?;
                return Ok(());
            }
            _ => {
                terminal
                    .write_line("Unknown command. Type HELP for commands.")
                    .await?;
            }
        }
    }
}

async fn write_help<S>(terminal: &mut Terminal<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    terminal.write_line("Commands:").await?;
    terminal
        .write_line("  LIST                 List public and your private messages")
        .await?;
    terminal
        .write_line("  SENT                 List messages you sent")
        .await?;
    terminal
        .write_line("  LOGINS               List the 10 most recent logins")
        .await?;
    terminal
        .write_line("  READ <id>            Read a visible message")
        .await?;
    terminal
        .write_line("  DELETE <id>          Delete mail you sent or received")
        .await?;
    terminal
        .write_line("  SEND <callsign|ALL>  Send private mail or post publicly")
        .await?;
    terminal
        .write_line("  HELP                 Show this help")
        .await?;
    terminal
        .write_line("  QUIT                 Disconnect")
        .await?;
    Ok(())
}

async fn list_recent_logins<S>(terminal: &mut Terminal<S>, store: &MailStore) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let logins = store.recent_logins(RECENT_LOGIN_LIMIT).await?;
    if logins.is_empty() {
        terminal.write_line("No logins recorded.").await?;
        return Ok(());
    }

    terminal.write_line("Recent logins:").await?;
    for login in logins {
        let transport = match login.transport {
            LoginTransport::Tcp => "TCP",
            LoginTransport::Ax25 => "AX.25",
        };
        terminal
            .write_line(&format!(
                "{} via {transport} at {}",
                login.callsign, login.logged_in_at
            ))
            .await?;
    }
    Ok(())
}

async fn list_sent_messages<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    sender: Callsign,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let messages = store.list_sent(sender).await?;
    if messages.is_empty() {
        terminal.write_line("No sent messages.").await?;
        return Ok(());
    }

    for message in messages {
        let kind = if message.recipient.is_some() {
            "MAIL"
        } else {
            "PUBLIC"
        };
        let recipient = message
            .recipient
            .as_ref()
            .map_or_else(|| "ALL".to_owned(), ToString::to_string);
        terminal
            .write_line(&format!(
                "#{} [{kind}] TO {recipient} {} - {}",
                message.id, message.created_at, message.subject
            ))
            .await?;
    }
    Ok(())
}

async fn list_messages<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    viewer: Callsign,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let messages = store.list_visible(viewer).await?;
    if messages.is_empty() {
        terminal.write_line("No messages.").await?;
        return Ok(());
    }

    for message in messages {
        let kind = if message.recipient.is_some() {
            "MAIL"
        } else {
            "PUBLIC"
        };
        terminal
            .write_line(&format!(
                "#{} [{kind}] FROM {} {} - {}",
                message.id, message.sender, message.created_at, message.subject
            ))
            .await?;
    }
    Ok(())
}

async fn read_message<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    viewer: Callsign,
    id: i64,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(message) = store.read_visible(viewer, id).await? else {
        terminal.write_line("Message not found.").await?;
        return Ok(());
    };
    write_message(terminal, message).await
}

async fn delete_message<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    caller: Callsign,
    id: i64,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if store.delete_authorized(caller, id).await? {
        terminal.write_line("Message deleted.").await?;
    } else {
        terminal
            .write_line("Message not found or cannot be deleted.")
            .await?;
    }
    Ok(())
}

async fn write_message<S>(terminal: &mut Terminal<S>, message: Message) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let recipient = message
        .recipient
        .as_ref()
        .map_or_else(|| "ALL".to_owned(), ToString::to_string);
    terminal
        .write_line(&format!("Message #{}", message.id))
        .await?;
    terminal
        .write_line(&format!("From: {}", message.sender))
        .await?;
    terminal.write_line(&format!("To: {recipient}")).await?;
    terminal
        .write_line(&format!("Date: {}", message.created_at))
        .await?;
    terminal
        .write_line(&format!("Subject: {}", message.subject))
        .await?;
    terminal.write_line("").await?;
    for line in message.body.split('\n') {
        terminal.write_line(line).await?;
    }
    Ok(())
}

async fn compose_message<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    sender: Callsign,
    recipient_input: &str,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let recipient = if recipient_input.eq_ignore_ascii_case("ALL") {
        None
    } else {
        match Callsign::parse(recipient_input) {
            Ok(callsign) => Some(callsign),
            Err(error) => {
                terminal
                    .write_line(&format!("Invalid recipient: {error}"))
                    .await?;
                return Ok(());
            }
        }
    };

    terminal.write("Subject: ").await?;
    let Some(subject_line) = terminal.read_line().await? else {
        return Ok(());
    };
    let subject = subject_line.trim();
    if subject.is_empty() {
        terminal.write_line("Subject cannot be empty.").await?;
        return Ok(());
    }
    if subject.chars().count() > MAX_SUBJECT_CHARS {
        terminal
            .write_line(&format!(
                "Subject cannot exceed {MAX_SUBJECT_CHARS} characters."
            ))
            .await?;
        return Ok(());
    }

    terminal
        .write_line("Enter message text. End with a line containing only a period.")
        .await?;
    let mut lines = Vec::new();
    let mut character_count = 0;
    loop {
        terminal.write("> ").await?;
        let Some(line) = terminal.read_line().await? else {
            return Ok(());
        };
        if line == "." {
            break;
        }

        character_count += line.chars().count();
        if !lines.is_empty() {
            character_count += 1;
        }
        if character_count > MAX_BODY_CHARS {
            terminal
                .write_line(&format!(
                    "Message cannot exceed {MAX_BODY_CHARS} characters."
                ))
                .await?;
            return Ok(());
        }
        lines.push(line);
    }

    let body = lines.join("\n");
    if body.trim().is_empty() {
        terminal.write_line("Message body cannot be empty.").await?;
        return Ok(());
    }

    let id = store
        .save(sender, recipient, subject.to_owned(), body)
        .await?;
    terminal
        .write_line(&format!("Message #{id} saved."))
        .await?;
    Ok(())
}
