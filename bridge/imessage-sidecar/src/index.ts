import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { Spectrum, attachment } from "spectrum-ts";
import { imessage } from "spectrum-ts/providers/imessage";
import type { Space, Message } from "spectrum-ts";

interface InboundMessage {
  type: "message";
  sender: string;
  chat_id: string;
  content: string;
  message_id: string;
  is_group: boolean;
  was_mentioned: boolean;
  media: { fileName: string; mimetype: string }[];
}

function extractMedia(content: Message["content"]): { fileName: string; mimetype: string }[] {
  if (content.type === "attachment") {
    return [{ fileName: content.name, mimetype: content.mimeType }];
  }
  // TODO: content.read() bytes — for voice and other binary types
  return [];
}

function extractText(content: Message["content"]): string {
  if (content.type === "text") {
    return content.text;
  }
  if (content.type === "attachment") {
    return `[attachment: ${content.name}]`;
  }
  if (content.type === "voice") {
    return `[Voice Message]`;
  }
  return `[${content.type}]`;
}

// ── Exported for testing ──────────────────────────────────────────────

/**
 * Resolve a space by ID: cache first, then SDK, then null.
 *
 * Extracted so tests can verify the three branches without needing
 * a real Photon connection.  The `im` parameter is typed as `any` to
 * avoid the complex generic PlatformInstance signature.
 */
export function resolveSpace(
  im: any,
  spaceCache: Map<string, Space>,
  spaceId: string
): Promise<Space | null> {
  const cached = spaceCache.get(spaceId);
  if (cached) return Promise.resolve(cached);

  return im.space(spaceId).then(
    (resolved: Space) => {
      spaceCache.set(resolved.id, resolved);
      return resolved;
    },
    () => null
  );
}

// ── Entry point ────────────────────────────────────────────────────────

async function main() {
  const PROJECT_ID = process.env.PROJECT_ID || "";
  const PROJECT_SECRET = process.env.PROJECT_SECRET || "";
  const PORT = parseInt(process.env.PORT || "8789", 10);

  if (!PROJECT_ID || !PROJECT_SECRET) {
    console.error(
      "Missing PROJECT_ID or PROJECT_SECRET. Set them in environment variables."
    );
    process.exit(1);
  }

  const app = await Spectrum({
    projectId: PROJECT_ID,
    projectSecret: PROJECT_SECRET,
    providers: [imessage.config()],
  });

  console.log("Spectrum SDK initialized");

  const im = imessage(app);

  // Cache resolved spaces by ID for outbound sends
  const spaceCache = new Map<string, Space>();

  // #2: Track the active inbound consumer via a generation counter.
  // When a new /inbound connection arrives, increment the generation. The old
  // loop sees the mismatch and exits immediately — before consuming another
  // message from app.messages — avoiding the race where `res.end()` hasn't
  // closed the socket yet.
  let inboundGeneration = 0;

  const server = createServer(
    async (req: IncomingMessage, res: ServerResponse) => {
      if (req.method === "GET" && req.url === "/inbound") {
        // #2: single-consumer guard — generation counter ensures the old
        // loop exits immediately (before consuming another message) when a
        // new connection arrives.
        const myGeneration = ++inboundGeneration;

        res.writeHead(200, {
          "Content-Type": "application/x-ndjson",
          "Transfer-Encoding": "chunked",
          "X-Accel-Buffering": "no",
          "Cache-Control": "no-cache",
        });

        try {
          for await (const [space, message] of app.messages) {
            if (myGeneration !== inboundGeneration) {
              // Kicked by a newer connection — exit before consuming.
              break;
            }
            if (res.destroyed) break;

            // Cache space for outbound sends
            spaceCache.set(space.id, space);

            const spaceType = (space as any).type as string | undefined;

            const msg: InboundMessage = {
              type: "message",
              sender: message.sender?.id || "",
              chat_id: space.id,
              content: extractText(message.content),
              message_id: message.id || "",
              is_group: spaceType === "group",
              was_mentioned: (message as any).wasMentioned === true,
              media: extractMedia(message.content),
            };

            res.write(JSON.stringify(msg) + "\n");
          }
        } catch (err) {
          if (!res.destroyed) {
            console.error("Inbound stream error:", err);
          }
        }
        if (!res.destroyed) {
          res.end();
        }
        return;
      }

      if (req.method === "POST" && req.url === "/send") {
        let body = "";
        req.on("data", (chunk) => (body += chunk));
        req.on("end", async () => {
          try {
            const { space: spaceId, text } = JSON.parse(body);
            const target = await resolveSpace(im, spaceCache, spaceId);
            if (!target) {
              res.writeHead(404, { "Content-Type": "application/json" });
              res.end(JSON.stringify({ error: `Space not found: ${spaceId}` }));
              return;
            }
            await target.send(text);
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ ok: true }));
          } catch (err: any) {
            console.error("Send error:", err);
            res.writeHead(500, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ error: err.message }));
          }
        });
        return;
      }

      if (req.method === "POST" && req.url === "/send-attachment") {
        let body = "";
        req.on("data", (chunk) => (body += chunk));
        req.on("end", async () => {
          try {
            const { space: spaceId, filePath, mimetype, fileName } =
              JSON.parse(body);
            const target = await resolveSpace(im, spaceCache, spaceId);
            if (!target) {
              res.writeHead(404, { "Content-Type": "application/json" });
              res.end(JSON.stringify({ error: `Space not found: ${spaceId}` }));
              return;
            }

            const att = attachment(filePath, { mimeType: mimetype });
            await target.send(att);
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ ok: true }));
          } catch (err: any) {
            console.error("Send attachment error:", err);
            res.writeHead(500, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ error: err.message }));
          }
        });
        return;
      }

      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Not found" }));
    }
  );

  server.listen(PORT, "127.0.0.1", () => {
    console.log(`iMessage sidecar listening on http://127.0.0.1:${PORT}`);
  });

  const shutdown = async () => {
    console.log("Shutting down...");
    server.close();
    await app.stop();
    process.exit(0);
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

// Only run main() when executed directly, not when imported for testing.
const isMain =
  process.argv[1] &&
  (process.argv[1].endsWith("/dist/index.js") ||
    process.argv[1].endsWith("/src/index.ts") ||
    process.argv[1].endsWith("/src/index.js"));

if (isMain) {
  main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  });
}