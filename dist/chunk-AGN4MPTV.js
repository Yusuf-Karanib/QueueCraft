// src/dashboard.ts
import {
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand
} from "@aws-sdk/client-sqs";
import {
  createServer
} from "http";
var DEFAULT_HOST = "127.0.0.1";
var DEFAULT_PORT = 4173;
var DEFAULT_CACHE_TTL_MS = 5 * 60 * 1e3;
var MAX_REQUEST_BYTES = 4096;
var html = [
  "<!doctype html>",
  '<html lang="en">',
  "<head>",
  '<meta charset="utf-8">',
  '<meta name="viewport" content="width=device-width,initial-scale=1">',
  "<title>QueueCraft Dashboard</title>",
  "<style>",
  ":root{font-family:Inter,system-ui,sans-serif;color:#10231a;background:#f4f8f5}",
  "*{box-sizing:border-box}body{margin:0}main{max-width:1080px;margin:auto;padding:40px 24px}",
  "header{display:flex;justify-content:space-between;align-items:center;margin-bottom:28px}",
  "h1{font-size:42px;margin:5px 0}.eyebrow{color:#19733d;font-weight:800;letter-spacing:.12em;font-size:12px}",
  ".badge{background:#e0f7e8;color:#126b34;padding:8px 12px;border-radius:999px;font-weight:700}",
  ".cards{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}.card,.panel{background:white;border:1px solid #d8e4dc;border-radius:16px;padding:22px}",
  ".number{font-size:40px;font-weight:800;margin:8px 0}.muted{color:#607168}.panel{margin-top:18px}",
  ".row{display:flex;justify-content:space-between;gap:20px;align-items:center;padding:16px 0;border-top:1px solid #e5ece7}",
  ".row:first-of-type{border-top:0}code{word-break:break-word}button{background:#164f2e;color:white;border:0;border-radius:8px;padding:10px 14px;font-weight:700;cursor:pointer}",
  "button:disabled{opacity:.5;cursor:not-allowed}.error{color:#a22424}.empty{padding:28px 0;color:#607168}",
  "@media(max-width:700px){.cards{grid-template-columns:1fr}header{align-items:flex-start;gap:12px}h1{font-size:34px}.row{align-items:flex-start;flex-direction:column}}",
  "</style>",
  "</head>",
  "<body><main>",
  '<header><div><div class="eyebrow">LOCAL AWS OPERATIONS</div><h1 id="title">QueueCraft</h1><div class="muted">Queue health and guarded dead-letter replay</div></div><div class="badge">Local only</div></header>',
  '<section class="cards">',
  '<article class="card"><div class="muted">Ready</div><div class="number" id="visible">\u2014</div><div>Messages waiting</div></article>',
  '<article class="card"><div class="muted">In flight</div><div class="number" id="inflight">\u2014</div><div>Currently processed</div></article>',
  '<article class="card"><div class="muted">Dead letter</div><div class="number" id="dlq">\u2014</div><div>Need attention</div></article>',
  "</section>",
  '<section class="panel"><div style="display:flex;justify-content:space-between;align-items:center"><div><div class="eyebrow">DLQ</div><h2>Failed jobs</h2></div><button id="refresh">Refresh</button></div><div id="error" class="error"></div><div id="messages"></div></section>',
  "</main>",
  "<script>",
  "const byId=(id)=>document.getElementById(id);",
  "async function request(path,options){const response=await fetch(path,options);const data=await response.json();if(!response.ok)throw new Error(data.error||'Request failed');return data}",
  "function escapeHtml(value){const node=document.createElement('div');node.textContent=String(value);return node.innerHTML}",
  "async function replay(id,button){if(!confirm('Replay this failed job to the main queue?'))return;button.disabled=true;try{await request('/api/dlq/replay',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({messageId:id,confirm:'REPLAY'})});await load()}catch(error){byId('error').textContent=error.message}finally{button.disabled=false}}",
  `function render(messages){const root=byId('messages');if(!messages.length){root.innerHTML='<div class="empty">No failed jobs.</div>';return}root.innerHTML='';for(const item of messages){const row=document.createElement('div');row.className='row';row.innerHTML='<div><strong>'+escapeHtml(item.id)+'</strong><div class="muted">Receives: '+escapeHtml(item.receiveCount)+' \xB7 Sent: '+escapeHtml(item.sentAt||'unknown')+'</div><code>'+escapeHtml(item.bodyPreview)+'</code></div>';const button=document.createElement('button');button.textContent='Replay';button.onclick=()=>replay(item.id,button);row.appendChild(button);root.appendChild(row)}}`,
  "async function load(){byId('error').textContent='';try{const [overview,dlq]=await Promise.all([request('/api/overview'),request('/api/dlq')]);byId('title').textContent=overview.title;byId('visible').textContent=overview.main.visible;byId('inflight').textContent=overview.main.inFlight;byId('dlq').textContent=overview.dlq.visible;render(dlq.messages)}catch(error){byId('error').textContent=error.message}}",
  "byId('refresh').onclick=load;load();setInterval(load,15000);",
  "</script></body></html>"
].join("\n");
async function createQueueCraftDashboard(options) {
  if (!options.queueUrl || !options.dlqUrl) {
    throw new Error("QueueCraft dashboard requires queueUrl and dlqUrl.");
  }
  if (options.queueUrl.endsWith(".fifo")) {
    throw new Error("QueueCraft dashboard replay supports standard queues only.");
  }
  const host = options.host ?? DEFAULT_HOST;
  if (host !== DEFAULT_HOST && host !== "::1" && host !== "localhost") {
    throw new Error("QueueCraft dashboard must bind to a loopback host.");
  }
  const port = options.port ?? DEFAULT_PORT;
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new RangeError("Dashboard port must be an integer between 0 and 65535.");
  }
  const cacheTtlMs = options.replayCacheTtlMs ?? DEFAULT_CACHE_TTL_MS;
  if (!Number.isInteger(cacheTtlMs) || cacheTtlMs < 1) {
    throw new RangeError("replayCacheTtlMs must be a positive integer.");
  }
  const cache = /* @__PURE__ */ new Map();
  const server = createServer(async (request, response) => {
    try {
      await handleRequest(options, cache, cacheTtlMs, request, response);
    } catch (error) {
      try {
        options.onError?.(error);
      } catch {
      }
      sendJson(response, 500, { error: "Dashboard request failed." });
    }
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
  const address = server.address();
  return {
    url: "http://" + host + ":" + address.port,
    close: () => new Promise((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    })
  };
}
async function handleRequest(options, cache, cacheTtlMs, request, response) {
  const url = new URL(request.url ?? "/", "http://localhost");
  if (request.method === "GET" && url.pathname === "/") {
    sendHtml(response, html);
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/overview") {
    const [main, dlq] = await Promise.all([
      getQueueCounts(options.sqsClient, options.queueUrl),
      getQueueCounts(options.sqsClient, options.dlqUrl)
    ]);
    sendJson(response, 200, { title: options.title ?? "QueueCraft", main, dlq });
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/dlq") {
    removeExpired(cache);
    const result = await options.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: options.dlqUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 0,
        VisibilityTimeout: 0,
        MessageAttributeNames: ["All"],
        MessageSystemAttributeNames: [
          "ApproximateReceiveCount",
          "SentTimestamp"
        ]
      })
    );
    const messages = (result.Messages ?? []).flatMap((message) => {
      if (!message.MessageId || !message.ReceiptHandle) return [];
      cache.set(message.MessageId, {
        receiptHandle: message.ReceiptHandle,
        body: message.Body ?? "",
        attributes: message.MessageAttributes,
        expiresAt: Date.now() + cacheTtlMs,
        published: false
      });
      return [{
        id: message.MessageId,
        receiveCount: Number(message.Attributes?.ApproximateReceiveCount ?? "1"),
        sentAt: formatTimestamp(message.Attributes?.SentTimestamp),
        bodyPreview: safeBodyPreview(message.Body)
      }];
    });
    sendJson(response, 200, { messages });
    return;
  }
  if (request.method === "POST" && url.pathname === "/api/dlq/replay") {
    const body = await readJson(request);
    if (body.confirm !== "REPLAY" || typeof body.messageId !== "string") {
      sendJson(response, 400, { error: "Replay requires messageId and confirm=REPLAY." });
      return;
    }
    removeExpired(cache);
    const cached = cache.get(body.messageId);
    if (!cached) {
      sendJson(response, 409, { error: "Refresh the DLQ before replaying this job." });
      return;
    }
    if (!cached.published) {
      await options.sqsClient.send(
        new SendMessageCommand({
          QueueUrl: options.queueUrl,
          MessageBody: cached.body,
          MessageAttributes: cached.attributes
        })
      );
      cached.published = true;
    }
    await options.sqsClient.send(
      new DeleteMessageCommand({
        QueueUrl: options.dlqUrl,
        ReceiptHandle: cached.receiptHandle
      })
    );
    cache.delete(body.messageId);
    sendJson(response, 200, { replayed: true });
    return;
  }
  sendJson(response, 404, { error: "Not found." });
}
async function getQueueCounts(client, queueUrl) {
  const result = await client.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed"
      ]
    })
  );
  return {
    visible: Number(result.Attributes?.ApproximateNumberOfMessages ?? "0"),
    inFlight: Number(result.Attributes?.ApproximateNumberOfMessagesNotVisible ?? "0"),
    delayed: Number(result.Attributes?.ApproximateNumberOfMessagesDelayed ?? "0")
  };
}
function removeExpired(cache) {
  const now = Date.now();
  for (const [id, message] of cache) {
    if (message.expiresAt <= now) cache.delete(id);
  }
}
function safeBodyPreview(body) {
  if (!body) return "(empty body)";
  try {
    const parsed = JSON.parse(body);
    return truncate(JSON.stringify(redact(parsed)));
  } catch {
    return "(non-JSON body hidden)";
  }
}
function redact(value, key = "") {
  if (/phone|email|message|text|token|secret|authorization/i.test(key)) {
    return "[redacted]";
  }
  if (Array.isArray(value)) return value.map((item) => redact(item));
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([childKey, childValue]) => [
        childKey,
        redact(childValue, childKey)
      ])
    );
  }
  return value;
}
function truncate(value) {
  return value.length > 500 ? value.slice(0, 497) + "..." : value;
}
function formatTimestamp(value) {
  if (!value) return void 0;
  const milliseconds = Number(value);
  return Number.isFinite(milliseconds) ? new Date(milliseconds).toISOString() : void 0;
}
async function readJson(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > MAX_REQUEST_BYTES) throw new Error("Request body is too large.");
    chunks.push(buffer);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new Error("Request body must be valid JSON.");
  }
}
function sendHtml(response, body) {
  response.writeHead(200, {
    "content-type": "text/html; charset=utf-8",
    "content-security-policy": "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'",
    "x-content-type-options": "nosniff",
    "cache-control": "no-store"
  });
  response.end(body);
}
function sendJson(response, status, value) {
  if (response.headersSent) return;
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "x-content-type-options": "nosniff"
  });
  response.end(JSON.stringify(value));
}

export {
  createQueueCraftDashboard
};
