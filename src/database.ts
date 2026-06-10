import { DatabaseSync } from "node:sqlite";
import path from "node:path";
import fs from "node:fs";

const DATA_DIR = path.join(import.meta.dirname, "..", "data");
const DB_PATH = path.join(DATA_DIR, "whatsapp.db");

export interface Chat {
  jid: string;
  name?: string | null;
  last_message_time?: Date | null;
  last_message?: string | null;
  last_sender?: string | null;
  last_is_from_me?: boolean | null;
}

export type Message = {
  id: string;
  chat_jid: string;
  sender?: string | null;
  content: string;
  timestamp: Date;
  is_from_me: boolean;
  chat_name?: string | null;
  raw_message?: Uint8Array | null;
};

let dbInstance: DatabaseSync | null = null;

function getDb(): DatabaseSync {
  if (!dbInstance) {
    if (!fs.existsSync(DATA_DIR)) {
      fs.mkdirSync(DATA_DIR, { recursive: true });
    }
    dbInstance = new DatabaseSync(DB_PATH);
  }
  return dbInstance;
}

export function initializeDatabase(): DatabaseSync {
  const db = getDb();

  db.exec("PRAGMA journal_mode = WAL");

  db.exec(`
        CREATE TABLE IF NOT EXISTS chats (
            jid TEXT PRIMARY KEY,
            name TEXT,
            last_message_time TEXT -- Store dates as ISO strings
        );
    `);

  db.exec(`
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT,
            chat_jid TEXT,
            sender TEXT,      -- JID of the sender (can be group participant or contact)
            content TEXT,
            timestamp TEXT, -- Store dates as ISO strings
            is_from_me INTEGER, -- Store booleans as 0 or 1
            PRIMARY KEY (id, chat_jid),
            FOREIGN KEY (chat_jid) REFERENCES chats(jid) ON DELETE CASCADE
        );
    `);

    db.exec(`
      CREATE TABLE IF NOT EXISTS contacts (
        jid TEXT PRIMARY KEY,
        name TEXT,
        notify TEXT,
        phone_number TEXT
      );
    `);

  // Migration: add raw_message column if missing (stores proto-encoded WAMessage for media).
  try {
    const cols = db.prepare(`PRAGMA table_info(messages)`).all() as Array<{ name: string }>;
    if (!cols.some((c) => c.name === "raw_message")) {
      db.exec(`ALTER TABLE messages ADD COLUMN raw_message BLOB`);
    }
  } catch (e) {
    console.error("Failed to migrate messages.raw_message:", e);
  }

  // Migration: add `lid` column to contacts (LID format JID for the same contact).
  try {
    const cols = db.prepare(`PRAGMA table_info(contacts)`).all() as Array<{ name: string }>;
    if (!cols.some((c) => c.name === "lid")) {
      db.exec(`ALTER TABLE contacts ADD COLUMN lid TEXT`);
    }
  } catch (e) {
    console.error("Failed to migrate contacts.lid:", e);
  }

  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp);`,
  );
  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_messages_chat_jid ON messages (chat_jid);`,
  );
  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages (sender);`,
  );
  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_chats_last_message_time ON chats (last_message_time);`,
  );
  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_contacts_lid ON contacts (lid);`,
  );

  return db;
}

export function storeChat(chat: Partial<Chat> & { jid: string }): void {
  const db = getDb();
  try {
    const stmt = db.prepare(`
            INSERT INTO chats (jid, name, last_message_time)
            VALUES (@jid, @name, @last_message_time)
            ON CONFLICT(jid) DO UPDATE SET
                name = COALESCE(excluded.name, name),
                last_message_time = COALESCE(excluded.last_message_time, last_message_time)
        `);
    stmt.run({
      jid: chat.jid,
      name: chat.name ?? null,
      last_message_time:
        chat.last_message_time instanceof Date
          ? chat.last_message_time.toISOString()
          : chat.last_message_time === null
            ? null
            : String(chat.last_message_time),
    });
  } catch (error) {
    console.error("Error storing chat:", error);
  }
}

export function storeMessage(message: Message): void {
  const db = getDb();
  try {
    storeChat({ jid: message.chat_jid, last_message_time: message.timestamp });

    const stmt = db.prepare(`
            INSERT OR REPLACE INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, raw_message)
            VALUES (@id, @chat_jid, @sender, @content, @timestamp, @is_from_me, @raw_message)
        `);

    stmt.run({
      id: message.id,
      chat_jid: message.chat_jid,
      sender: message.sender ?? null,
      content: message.content,
      timestamp: message.timestamp.toISOString(),
      is_from_me: message.is_from_me ? 1 : 0,
      raw_message: message.raw_message ?? null,
    });

    const updateChatTimeStmt = db.prepare(`
            UPDATE chats
            SET last_message_time = MAX(COALESCE(last_message_time, '1970-01-01T00:00:00.000Z'), @timestamp)
            WHERE jid = @jid
        `);
    updateChatTimeStmt.run({
      timestamp: message.timestamp.toISOString(),
      jid: message.chat_jid,
    });
  } catch (error) {
    console.error("Error storing message:", error);
  }
}

function parseDateSafe(dateString: string | null | undefined): Date | null {
  if (!dateString) return null;
  try {
    const date = new Date(dateString);
    return isNaN(date.getTime()) ? null : date;
  } catch (e) {
    return null;
  }
}

function rowToMessage(row: any): Message {
  return {
    id: row.id,
    chat_jid: row.chat_jid,
    sender: row.sender,
    content: row.content,
    timestamp: parseDateSafe(row.timestamp)!,
    is_from_me: Boolean(row.is_from_me),
    chat_name: row.chat_name,
  };
}

function rowToChat(row: any): Chat {
  return {
    jid: row.jid,
    name: row.name,
    last_message_time: parseDateSafe(row.last_message_time),
    last_message: row.last_message,
    last_sender: row.last_sender,
    last_is_from_me:
      row.last_is_from_me !== null ? Boolean(row.last_is_from_me) : null,
  };
}

export function getMessages(
  chatJid: string,
  limit: number = 20,
  page: number = 0,
): Message[] {
  const db = getDb();
  try {
    const offset = page * limit;
    const stmt = db.prepare(`
            SELECT m.*, c.name as chat_name
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.chat_jid = ? -- Positional parameter 1
            ORDER BY m.timestamp DESC
            LIMIT ?             -- Positional parameter 2
            OFFSET ?            -- Positional parameter 3
        `);
    const rows = stmt.all(chatJid, limit, offset) as any[];
    return rows.map(rowToMessage);
  } catch (error) {
    console.error("Error getting messages:", error);
    return [];
  }
}

export function getChats(
  limit: number = 20,
  page: number = 0,
  sortBy: "last_active" | "name" = "last_active",
  query?: string | null,
  includeLastMessage: boolean = true,
): Chat[] {
  const db = getDb();
  try {
    const offset = page * limit;
    let sql = `
            SELECT
                c.jid,
                COALESCE(c.name, ct.name, ct.notify, ct.phone_number) as name,
                c.last_message_time
                ${
                  includeLastMessage
                    ? `,
                (SELECT m.content FROM messages m WHERE m.chat_jid = c.jid ORDER BY m.timestamp DESC LIMIT 1) as last_message,
                (SELECT m.sender FROM messages m WHERE m.chat_jid = c.jid ORDER BY m.timestamp DESC LIMIT 1) as last_sender,
                (SELECT m.is_from_me FROM messages m WHERE m.chat_jid = c.jid ORDER BY m.timestamp DESC LIMIT 1) as last_is_from_me
                `
                    : ""
                }
            FROM chats c
            LEFT JOIN contacts ct ON ct.jid = c.jid OR ct.lid = c.jid
        `;

    const params: (string | number)[] = [];

    if (query) {
      sql += ` WHERE (LOWER(COALESCE(c.name, ct.name, ct.notify, ct.phone_number)) LIKE LOWER(?) OR c.jid LIKE ?)`;
      params.push(`%${query}%`, `%${query}%`);
    }

    const orderByClause =
      sortBy === "last_active"
        ? "c.last_message_time DESC NULLS LAST"
        : "COALESCE(c.name, ct.name, ct.notify, ct.phone_number) ASC";
    sql += ` ORDER BY ${orderByClause}, c.jid ASC`;

    sql += ` LIMIT ? OFFSET ?`;
    params.push(limit, offset);

    const stmt = db.prepare(sql);
    const rows = stmt.all(...params) as any[];
    return rows.map(rowToChat);
  } catch (error) {
    console.error("Error getting chats:", error);
    return [];
  }
}

export function getChat(
  jid: string,
  includeLastMessage: boolean = true,
): Chat | null {
  const db = getDb();
  try {
    let sql = `
            SELECT
                c.jid,
                COALESCE(c.name, ct.name, ct.notify, ct.phone_number) as name,
                c.last_message_time
                ${
                  includeLastMessage
                    ? `,
                (SELECT m.content FROM messages m WHERE m.chat_jid = c.jid ORDER BY m.timestamp DESC LIMIT 1) as last_message,
                (SELECT m.sender FROM messages m WHERE m.chat_jid = c.jid ORDER BY m.timestamp DESC LIMIT 1) as last_sender,
                (SELECT m.is_from_me FROM messages m WHERE m.chat_jid = c.jid ORDER BY m.timestamp DESC LIMIT 1) as last_is_from_me
                `
                    : ""
                }
            FROM chats c
            LEFT JOIN contacts ct ON ct.jid = c.jid OR ct.lid = c.jid
            WHERE c.jid = ? -- Positional parameter 1
        `;

    const stmt = db.prepare(sql);
    const row = stmt.get(jid) as any | undefined;
    return row ? rowToChat(row) : null;
  } catch (error) {
    console.error("Error getting chat:", error);
    return null;
  }
}

export function getMessagesAround(
  messageId: string,
  before: number = 5,
  after: number = 5,
): { before: Message[]; target: Message | null; after: Message[] } {
  const db = getDb();
  const result: {
    before: Message[];
    target: Message | null;
    after: Message[];
  } = { before: [], target: null, after: [] };

  try {
    const targetStmt = db.prepare(`
             SELECT m.*, c.name as chat_name
             FROM messages m
             JOIN chats c ON m.chat_jid = c.jid
             WHERE m.id = ? -- Positional parameter 1
        `);
    const targetRow = targetStmt.get(messageId) as any | undefined;

    if (!targetRow) {
      return result;
    }
    result.target = rowToMessage(targetRow);
    const targetTimestamp = result.target.timestamp.toISOString();
    const chatJid = result.target.chat_jid;

    const beforeStmt = db.prepare(`
            SELECT m.*, c.name as chat_name
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.chat_jid = ? AND m.timestamp < ? -- Positional params 1, 2
            ORDER BY m.timestamp DESC
            LIMIT ?                                  -- Positional param 3
        `);
    const beforeRows = beforeStmt.all(
      chatJid,
      targetTimestamp,
      before,
    ) as any[];
    result.before = beforeRows.map(rowToMessage).reverse();

    const afterStmt = db.prepare(`
            SELECT m.*, c.name as chat_name
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.chat_jid = ? AND m.timestamp > ? -- Positional params 1, 2
            ORDER BY m.timestamp ASC
            LIMIT ?                                  -- Positional param 3
        `);
    const afterRows = afterStmt.all(chatJid, targetTimestamp, after) as any[];
    result.after = afterRows.map(rowToMessage);

    return result;
  } catch (error) {
    console.error("Error getting messages around:", error);
    return result;
  }
}

export function searchDbForContacts(
  query: string,
  limit: number = 20
): { jid: string; name: string | null }[] {
  const db = getDb();
  try {
    const pattern = `%${query}%`;

    const stmt = db.prepare(`
      SELECT
        jid,
        COALESCE(name, notify, phone_number, jid) AS display_name
      FROM contacts
      WHERE
        LOWER(COALESCE(name, notify, phone_number, jid)) LIKE LOWER(?)
        OR LOWER(COALESCE(lid, '')) LIKE LOWER(?)
      LIMIT ?
    `);

    const rows = stmt.all(pattern, pattern, limit) as {
      jid: string;
      display_name: string | null;
    }[];

    return rows.map((r) => ({
      jid: r.jid,
      name: r.display_name,
    }));
  } catch (error) {
    console.error("Error searching contacts:", error);
    return [];
  }
}

export function searchMessages(
  searchQuery: string,
  chatJid?: string | null,
  limit: number = 10,
  page: number = 0,
): Message[] {
  const db = getDb();
  try {
    const offset = page * limit;
    const searchPattern = `%${searchQuery}%`;
    let sql = `
            SELECT m.*, COALESCE(c.name, ct.name, ct.notify, ct.phone_number) as chat_name
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            LEFT JOIN contacts ct ON ct.jid = c.jid OR ct.lid = c.jid
            WHERE LOWER(m.content) LIKE LOWER(?) -- Param 1: searchPattern
        `;
    const params: (string | number | null)[] = [searchPattern];

    if (chatJid) {
      sql += ` AND m.chat_jid = ?`;
      params.push(chatJid);
    }

    sql += ` ORDER BY m.timestamp DESC`;
    sql += ` LIMIT ?`;
    params.push(limit);
    sql += ` OFFSET ?`;
    params.push(offset);

    const stmt = db.prepare(sql);
    const rows = stmt.all(...params) as any[];
    return rows.map(rowToMessage);
  } catch (error) {
    console.error("Error searching messages:", error);
    return [];
  }
}

export function getRawMessageById(
  messageId: string,
): { raw: Uint8Array; chat_jid: string } | null {
  const db = getDb();
  try {
    const stmt = db.prepare(
      `SELECT raw_message, chat_jid FROM messages WHERE id = ? LIMIT 1`,
    );
    const row = stmt.get(messageId) as
      | { raw_message: Uint8Array | null; chat_jid: string }
      | undefined;
    if (!row || !row.raw_message) return null;
    return { raw: row.raw_message, chat_jid: row.chat_jid };
  } catch (error) {
    console.error("Error getting raw message:", error);
    return null;
  }
}

export function closeDatabase(): void {
  if (dbInstance) {
    try {
      dbInstance.close();
      dbInstance = null;
      console.log("Database connection closed.");
    } catch (error) {
      console.error("Error closing database:", error);
    }
  }
}

export function storeContact(contact: {
  jid: string;
  name?: string | null;
  notify?: string | null;
  phoneNumber?: string | null;
  lid?: string | null;
}): void {
  const db = getDb();
  try {
    const stmt = db.prepare(`
      INSERT INTO contacts (jid, name, notify, phone_number, lid)
      VALUES (@jid, @name, @notify, @phone_number, @lid)
      ON CONFLICT(jid) DO UPDATE SET
        name = COALESCE(excluded.name, name),
        notify = COALESCE(excluded.notify, notify),
        phone_number = COALESCE(excluded.phone_number, phone_number),
        lid = COALESCE(excluded.lid, lid)
    `);

    stmt.run({
      jid: contact.jid,
      name: contact.name ?? null,
      notify: contact.notify ?? null,
      phone_number: contact.phoneNumber ?? null,
      lid: contact.lid ?? null,
    });
  } catch (error) {
    console.error("Error storing contact:", error);
  }
}

/**
 * Record a LID → PN mapping by attaching the LID to an existing PN-keyed contact,
 * or creating a placeholder PN-keyed row if needed. Used by the LID backfill on
 * startup so chat/message queries can resolve @lid JIDs to known contacts.
 */
export function linkLidToPn(lid: string, pnJid: string): void {
  const db = getDb();
  try {
    db.prepare(`
      INSERT INTO contacts (jid, lid, phone_number)
      VALUES (@jid, @lid, @jid)
      ON CONFLICT(jid) DO UPDATE SET
        lid = COALESCE(excluded.lid, lid),
        phone_number = COALESCE(phone_number, excluded.phone_number)
    `).run({ jid: pnJid, lid });
  } catch (error) {
    console.error("Error linking LID->PN:", error);
  }
}

/**
 * Returns all distinct LID JIDs referenced by the DB (in chats.jid or messages.sender)
 * that don't yet have a PN counterpart attached in contacts.
 */
export function getUnmappedLids(): string[] {
  const db = getDb();
  try {
    const rows = db.prepare(`
      SELECT DISTINCT lid_jid FROM (
        SELECT jid AS lid_jid FROM chats WHERE jid LIKE '%@lid'
        UNION
        SELECT sender AS lid_jid FROM messages WHERE sender LIKE '%@lid'
      )
      WHERE lid_jid NOT IN (SELECT lid FROM contacts WHERE lid IS NOT NULL)
    `).all() as { lid_jid: string }[];
    return rows.map(r => r.lid_jid);
  } catch (error) {
    console.error("Error getting unmapped LIDs:", error);
    return [];
  }
}

/**
 * Returns a human-friendly display string for a sender JID. Tries contacts
 * table by jid, then by lid, then falls back to the local part of the JID.
 */
export function resolveSenderDisplay(senderJid: string): string {
  const db = getDb();
  try {
    const row = db.prepare(`
      SELECT COALESCE(name, notify, phone_number) AS display
      FROM contacts
      WHERE jid = ? OR lid = ?
      LIMIT 1
    `).get(senderJid, senderJid) as { display: string | null } | undefined;
    if (row?.display) return row.display;
  } catch (error) {
    console.error("resolveSenderDisplay error:", error);
  }
  return senderJid.split("@")[0] ?? senderJid;
}

