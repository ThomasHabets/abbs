# ABBS

ABBS is a small amateur-radio bulletin board system. It accepts AX.25
connections through an AGWPE-compatible endpoint such as Direwolf, as well as
plain TCP terminal connections. Messages are stored in SQLite.

<https://github.com/ThomasHabets/abbs/>

## Requirements

- A current Rust toolchain.
- SQLite development/runtime libraries available to `rusqlite`.
- For AX.25 access, an AGWPE-compatible service. Direwolf can provide one with
  an `AGWPORT` configured.

## Run

Start the BBS with its station callsign:

```sh
cargo run -- --callsign M0BBS
```

By default this creates `abbs.sqlite3`, listens for TCP clients on port 8000,
and connects to an AGWPE endpoint at `127.0.0.1:8010` using radio port 1.  TCP
stays available when the AGWPE endpoint is offline; the radio listener retries
every five seconds.

Files offered for download are placed in the `files` directory by default.

```text
Usage: abbs [OPTIONS] --callsign <CALLSIGN>

Options:
--callsign <CALLSIGN>      BBS callsign (required)
--db <PATH>                SQLite database path [default: abbs.sqlite3]
--files-dir <PATH>         Directory containing downloadable files [default: files]
--tcp-listen <ADDRESS>     TCP bind address [default: 0.0.0.0:8000]
--agw-addr <ADDRESS>       AGWPE/Direwolf endpoint [default: 127.0.0.1:8010]
--agw-port <NUMBER>        AGWPE radio port [default: 1]
```

For a local TCP session:

```sh
nc 127.0.0.1 8000
```

The BBS prompts for a callsign on TCP. AX.25 sessions receive their identity
from the remote callsign reported by AGWPE. Callsigns are normalized to
uppercase and must be ASCII alphanumeric/hyphen values no longer than ten
characters.

## Commands

After connecting, type `HELP` to display the command list.

| Command | Description |
| --- | --- |
| `LIST` | List public posts and private mail addressed to you. |
| `SENT` | List messages sent by your callsign. |
| `LOGINS` | List the 10 most recent callsign logins, their transport, and time. |
| `FILES` | List files available for download. |
| `DOWNLOAD <file>` | Send one listed file using ZMODEM. |
| `READ <id>` | Read a public message or private mail addressed to you. |
| `SEND <callsign>` | Compose private mail. |
| `SEND ALL` | Compose a public post. |
| `DELETE <id>` | Permanently delete a private message you sent or received, or a public message you sent. |
| `QUIT` | Disconnect. |

`SEND` prompts for a subject and then accepts a multiline body. End the body
with a line containing only `.`. Subjects are limited to 80 characters and
message bodies to 4,000 characters.

The terminal protocol accepts CR, CRLF, and LF input line endings and writes
CRLF responses.

Mailbox access ignores an SSID suffix: mail sent to `M0ABC-7` is visible to
`M0ABC` and all of its SSIDs. Message listings retain the full callsigns used
when a message was sent.

`DOWNLOAD` invokes the external `sz` utility in binary ZMODEM mode. The client
must start `rz` when it detects the ZMODEM header. Only immediate regular files
with a simple, non-whitespace filename are listed or downloadable; paths,
directories, and symlinks are excluded to prevent directory traversal.
After a successful transfer the BBS deliberately sends no completion text or
prompt, so that it cannot be mistaken for the final ZMODEM frame by the
client's `rz`. Send the next command normally once the client reports that the
transfer has finished.

## Identity and privacy

TCP callsign entry is an identity label, not authentication: a TCP user can
claim any valid callsign. Private-message visibility and deletion permissions
therefore rely on that stated callsign. Do not expose the TCP listener to
untrusted users if stronger identity guarantees are required.

`LOGINS` records and displays the callsign, login time, and whether the user
connected over TCP or AX.25. It does not retain or display TCP IP addresses.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
