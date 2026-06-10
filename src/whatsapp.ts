import {
  makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  DisconnectReason,
  type WAMessage,
  proto,
  isJidGroup,
  jidNormalizedUser,
} from "@whiskeysockets/baileys";
import P from "pino";
import path from "node:path";
import open from "open";

import {
  initializeDatabase,
  storeMessage,
  storeChat,
  storeContact,
  linkLidToPn,
  getUnmappedLids,
  type Message as DbMessage,
} from "./database.ts";

const AUTH_DIR = path.join(import.meta.dirname, "..", "auth_info");

export type WhatsAppSocket = ReturnType<typeof makeWASocket>;

function parseMessageForDb(msg: WAMessage): DbMessage | null {
  if (!msg.message || !msg.key || !msg.key.remoteJid) {
    return null;
  }

  let content: string | null = null;
  const messageType = Object.keys(msg.message)[0];

  let isMedia = false;
  if (msg.message.conversation) {
    content = msg.message.conversation;
  } else if (msg.message.extendedTextMessage?.text) {
    content = msg.message.extendedTextMessage.text;
  } else if (msg.message.imageMessage) {
    content = `[Image] ${msg.message.imageMessage.caption ?? ""}`.trim();
    isMedia = true;
  } else if (msg.message.videoMessage) {
    content = `[Video] ${msg.message.videoMessage.caption ?? ""}`.trim();
    isMedia = true;
  } else if (msg.message.documentMessage) {
    content = `[Document] ${
      msg.message.documentMessage.caption ||
      msg.message.documentMessage.fileName ||
      ""
    }`.trim();
    isMedia = true;
  } else if (msg.message.audioMessage) {
    content = `[Audio]`;
    isMedia = true;
  } else if (msg.message.stickerMessage) {
    content = `[Sticker]`;
    isMedia = true;
  } else if (msg.message.locationMessage?.address) {
    content = `[Location] ${msg.message.locationMessage.address}`;
  } else if (msg.message.contactMessage?.displayName) {
    content = `[Contact] ${msg.message.contactMessage.displayName}`;
  } else if (msg.message.pollCreationMessage?.name) {
    content = `[Poll] ${msg.message.pollCreationMessage.name}`;
  }

  if (!content) {
    return null;
  }

  // Use WhatsApp's original message timestamp (seconds since epoch)
  let timestampSeconds: number;

  if (msg.messageTimestamp != null) {
    // Handles number, bigint, and Long-like objects
    timestampSeconds = Number(msg.messageTimestamp);
  } else {
    // Fallback only if WA didn't give us a timestamp at all
    timestampSeconds = Date.now() / 1000;
  }

  const timestamp = new Date(timestampSeconds * 1000);

  // For group messages, newer WhatsApp protocol puts the sender on the top-level
  // WebMessageInfo.participant rather than key.participant. Fall back accordingly.
  let senderJid: string | null | undefined =
    msg.key.participant || (msg as any).participant;
  if (!msg.key.fromMe && !senderJid && !isJidGroup(msg.key.remoteJid)) {
    senderJid = msg.key.remoteJid;
  }
  if (msg.key.fromMe && !isJidGroup(msg.key.remoteJid)) {
    senderJid = null;
  }

  // pushName is the sender's display name as broadcast by their phone — useful for
  // group participants we have no contact entry for. Persist it as a contact hint.
  const pushName = (msg as any).pushName as string | null | undefined;
  if (senderJid && pushName) {
    try {
      // Lazy import to avoid circular ref at top of file
      import("./database.ts").then(({ storeContact }) => {
        storeContact({
          jid: senderJid as string,
          notify: pushName,
        });
      }).catch(() => {});
    } catch {}
  }

  let rawEncoded: Uint8Array | null = null;
  if (isMedia) {
    try {
      rawEncoded = proto.WebMessageInfo.encode(msg).finish();
    } catch (err) {
      // If encoding fails, we silently drop raw storage; content is still saved.
      rawEncoded = null;
    }
  }

  return {
    id: msg.key.id!,
    chat_jid: msg.key.remoteJid,
    sender: senderJid ? jidNormalizedUser(senderJid) : null,
    content: content,
    timestamp: timestamp,
    is_from_me: msg.key.fromMe ?? false,
    raw_message: rawEncoded,
  };
}

export async function startWhatsAppConnection(
  logger: P.Logger
): Promise<WhatsAppSocket> {
  initializeDatabase();

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version, isLatest } = await fetchLatestBaileysVersion();
  logger.info(`Using WA v${version.join(".")}, isLatest: ${isLatest}`);

  const sock = makeWASocket({
    version,
    logger,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    generateHighQualityLinkPreview: true,
  });

  sock.ev.process(async (events) => {
    if (events["connection.update"]) {
      const update = events["connection.update"];
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        logger.info(
          { qrCodeData: qr },
          "QR Code Received. Copy the qrCodeData string and use a QR code generator (e.g., online website) to display and scan it with your WhatsApp app."
        );
        // for now we roughly open the QR code in a browser
        await open(`https://quickchart.io/qr?text=${encodeURIComponent(qr)}`);
      }

      if (connection === "close") {
        const statusCode = (lastDisconnect?.error as any)?.output?.statusCode;
        logger.warn(
          `Connection closed. Reason: ${
            DisconnectReason[statusCode as number] || "Unknown"
          }`,
          lastDisconnect?.error
        );
        if (statusCode !== DisconnectReason.loggedOut) {
          logger.info("Reconnecting...");
          startWhatsAppConnection(logger);
        } else {
          logger.error(
            "Connection closed: Logged Out. Please delete auth_info and restart."
          );
          process.exit(1);
        }
      } else if (connection === "open") {
        logger.info(`Connection opened. WA user: ${sock.user?.name}`);
        // Kick off LID->PN backfill once the socket is live. We don't await it so
        // the connection-update handler stays snappy.
        backfillLidMappings(sock, logger).catch((err) => {
          logger.warn({ err }, "LID backfill failed");
        });
      }
    }

    if (events["creds.update"]) {
      await saveCreds();
      logger.info("Credentials saved.");
    }

    if (events["messaging-history.set"]) {
      const { chats, contacts, messages, isLatest, progress, syncType } =
        events["messaging-history.set"];
      if (contacts.length > 0) {
        logger.info(`Storing ${contacts.length} contacts from history sync.`);
        contacts.forEach((c) => {
          storeContact({
            jid: c.id,
            name: c.name ?? null,
            notify: c.notify ?? null,
            phoneNumber: (c as any).phoneNumber ?? null,
            lid: (c as any).lid ?? null,
          });
          // If the contact came in keyed by PN but also carries a LID, persist the
          // LID separately so future @lid lookups resolve back to the same row.
          const lid = (c as any).lid as string | undefined;
          const pn = (c as any).phoneNumber as string | undefined;
          if (lid && pn && c.id !== lid) {
            linkLidToPn(lid, pn);
          }
        });
        logger.info(`Stored ${contacts.length} contacts from history sync.`);
      }

      logger.info(`Storing ${chats.length} chats from history sync.`);
      chats.forEach((chat) =>
        storeChat({
          jid: chat.id,
          name: chat.name,
          last_message_time: chat.conversationTimestamp
            ? new Date(Number(chat.conversationTimestamp) * 1000)
            : undefined,
        })
      );

      let storedCount = 0;
      messages.forEach((msg) => {
        const parsed = parseMessageForDb(msg);
        if (parsed) {
          storeMessage(parsed);
          storedCount++;
        }
      });
      logger.info(`Stored ${storedCount} messages from history sync.`);
    }

    if (events["messages.upsert"]) {
      const { messages, type } = events["messages.upsert"];
      logger.info(
        { type, count: messages.length },
        "Received messages.upsert event"
      );

      if (type === "notify" || type === "append") {
        for (const msg of messages) {
          const parsed = parseMessageForDb(msg);
          if (parsed) {
            logger.info(
              {
                msgId: parsed.id,
                chatId: parsed.chat_jid,
                fromMe: parsed.is_from_me,
                sender: parsed.sender,
              },
              `Storing message: ${parsed.content.substring(0, 50)}...`
            );
            storeMessage(parsed);
          } else {
            logger.warn(
              { msgId: msg.key?.id, chatId: msg.key?.remoteJid },
              "Skipped storing message (parsing failed or unsupported type)"
            );
          }
        }
      }
    }

    if (events["chats.update"]) {
      logger.info(
        { count: events["chats.update"].length },
        "Received chats.update event"
      );
      for (const chatUpdate of events["chats.update"]) {
        storeChat({
          jid: chatUpdate.id!,
          name: chatUpdate.name,
          last_message_time: chatUpdate.conversationTimestamp
            ? new Date(Number(chatUpdate.conversationTimestamp) * 1000)
            : undefined,
        });
      }
    }
  });

  return sock;
}


/**
 * Walks all @lid JIDs referenced by the DB that we haven't yet mapped to a phone
 * number, asks Baileys' signalRepository.lidMapping for the PN counterpart, and
 * persists the link so chat/contact lookups resolve them.
 */
export async function backfillLidMappings(
  sock: WhatsAppSocket,
  logger: P.Logger,
): Promise<{ resolved: number; unresolved: number }> {
  const lidMapping = (sock as any)?.signalRepository?.lidMapping;
  if (!lidMapping || typeof lidMapping.getPNForLID !== "function") {
    logger.warn("LID mapping API not available on socket; skipping backfill");
    return { resolved: 0, unresolved: 0 };
  }

  const lids = getUnmappedLids();
  if (lids.length === 0) {
    logger.info("LID backfill: nothing to resolve");
    return { resolved: 0, unresolved: 0 };
  }
  logger.info(`LID backfill: resolving ${lids.length} unmapped LID JIDs`);

  let resolved = 0;
  let unresolved = 0;
  for (const lid of lids) {
    try {
      const pn = await lidMapping.getPNForLID(lid);
      if (pn) {
        const normalized = jidNormalizedUser(pn);
        linkLidToPn(lid, normalized);
        resolved++;
      } else {
        unresolved++;
      }
    } catch (err) {
      unresolved++;
    }
  }
  logger.info(
    `LID backfill complete: ${resolved} resolved, ${unresolved} unresolved`,
  );
  return { resolved, unresolved };
}

export async function sendWhatsAppMessage(
  logger: P.Logger,
  sock: WhatsAppSocket | null,
  recipientJid: string,
  text: string
): Promise<proto.WebMessageInfo | void> {
  if (!sock || !sock.user) {
    logger.error(
      "Cannot send message: WhatsApp socket not connected or initialized."
    );
    return;
  }
  if (!recipientJid) {
    logger.error("Cannot send message: Recipient JID is missing.");
    return;
  }
  if (!text) {
    logger.error("Cannot send message: Message text is empty.");
    return;
  }

  try {
    logger.info(
      `Sending message to ${recipientJid}: ${text.substring(0, 50)}...`
    );
    const normalizedJid = jidNormalizedUser(recipientJid);
    const result = await sock.sendMessage(normalizedJid, { text: text });
    logger.info({ msgId: result?.key.id }, "Message sent successfully");
    return result;
  } catch (error) {
    logger.error({ err: error, recipientJid }, "Failed to send message");
    return;
  }
}
