import assert from 'node:assert/strict';
import { request as httpRequest } from 'node:http';
import { createQueueCraftDashboard } from '@yusufkaranib/queuecraft';

const mode = '--candidate';

function chunkedPost(url, headers, chunks) {
  return new Promise((resolve, reject) => {
    const timers = [];
    const clean = () => timers.forEach(clearTimeout);
    const request = httpRequest(url, { method: 'POST', agent: false, headers: { ...headers, 'transfer-encoding': 'chunked' } }, response => {
      response.resume();
      response.once('error', reject);
      response.once('end', () => {
        clean();
        if (!request.writableEnded && !request.destroyed) request.end();
        resolve(response.statusCode);
      });
    });
    request.setTimeout(2_500, () => request.destroy(new Error('Chunked request timed out.')));
    request.once('error', error => { clean(); reject(error); });
    chunks.forEach((chunk, index) => {
      timers.push(setTimeout(() => {
        if (request.destroyed || request.writableEnded) return;
        if (index === chunks.length - 1) request.end(chunk);
        else request.write(chunk);
      }, index * 45));
    });
  });
}

const queueCalls = [];
let serverErrors = 0;
let failPublish = false;
const dashboard = await createQueueCraftDashboard({
  port: 0,
  queueUrl: 'https://queue.example.test/main',
  dlqUrl: 'https://queue.example.test/failed',
  onError: () => { serverErrors++; },
  sqsClient: { async send(command) {
    const name = command.constructor.name;
    queueCalls.push(name);
    if (name === 'ReceiveMessageCommand') {
      return { Messages: [{ MessageId: 'fixture', ReceiptHandle: 'local-fixture', Body: '{"fake":true}' }] };
    }
    if (name === 'SendMessageCommand' && failPublish) throw new Error('Simulated provider failure.');
    return {};
  } },
});

try {
  const origin = new URL(dashboard.url);
  assert.equal(origin.hostname, '127.0.0.1');
  const request = (path, options) => fetch(new URL(path, origin), {
    redirect: 'error', signal: AbortSignal.timeout(3_000), ...options,
  });
  const home = await request('/');
  assert.equal(home.status, 200);
  const token = (await home.text()).match(/const WRITE_TOKEN="([A-Za-z0-9_-]+)"/)?.[1];
  assert.ok(token);
  const headers = { origin: origin.origin, 'content-type': 'application/json', 'x-queuecraft-write-token': token };
  const endpoint = '/api/dlq/replay';
  const post = async (body, extraHeaders = {}) => {
    const response = await request(endpoint, { method: 'POST', body, headers: { ...headers, ...extraHeaders } });
    return { status: response.status, body: await response.text() };
  };
  const observe = async action => {
    const queueStart = queueCalls.length;
    const errorStart = serverErrors;
    let status;
    try { status = await action(); }
    catch (error) {
      const code = error.cause?.code ?? error.code;
      if (!['UND_ERR_SOCKET', 'ECONNRESET', 'EPIPE'].includes(code)) throw error;
      status = code;
    }
    return { status, queueCalls: queueCalls.length - queueStart, serverErrors: serverErrors - errorStart };
  };
  const uncached = JSON.stringify({ messageId: 'missing', confirm: 'REPLAY' });
  const atLimit = uncached.padEnd(4_096);
  const overLimit = atLimit + ' ';
  const multibyte = JSON.stringify({ messageId: 'é'.repeat(2_060), confirm: 'REPLAY' });
  assert.equal(Buffer.byteLength(atLimit), 4_096);
  assert.equal(Buffer.byteLength(overLimit), 4_097);
  assert.ok(multibyte.length < 4_096 && Buffer.byteLength(multibyte) > 4_096);
  const oversize = {
    contentLength: await observe(async () => (await post(overLimit)).status),
    chunked: await observe(() => chunkedPost(new URL(endpoint, origin), headers, [atLimit, ' ', ' '])),
    multibyte: await observe(async () => (await post(multibyte)).status),
    streamedMultibyte: await observe(() => chunkedPost(new URL(endpoint, origin), headers, [multibyte])),
  };
  const atLimitCheck = await observe(async () => (await post(atLimit)).status);
  assert.deepEqual(atLimitCheck, { status: 409, queueCalls: 0, serverErrors: 0 });
  const badJson = await observe(async () => (await post('{')).status);
  assert.deepEqual(badJson, { status: 400, queueCalls: 0, serverErrors: 0 });
  const wrongToken = await observe(async () => (await post(overLimit, { 'x-queuecraft-write-token': 'invalid' })).status);
  assert.deepEqual(wrongToken, { status: 403, queueCalls: 0, serverErrors: 0 });

  const listing = await request('/api/dlq');
  assert.equal(listing.status, 200);
  await listing.text();
  const valid = JSON.stringify({ messageId: 'fixture', confirm: 'REPLAY' });
  failPublish = true;
  const failure = await post(valid);
  assert.equal(failure.status, 500);
  assert.deepEqual(JSON.parse(failure.body), { error: 'Dashboard request failed.' });
  failPublish = false;
  const replay = await post(valid);
  assert.equal(replay.status, 200);
  assert.deepEqual(JSON.parse(replay.body), { replayed: true });
  assert.deepEqual(queueCalls, ['ReceiveMessageCommand', 'SendMessageCommand', 'SendMessageCommand', 'DeleteMessageCommand']);

  for (const result of Object.values(oversize)) {
    assert.equal(result.queueCalls, 0);
    if (mode === '--candidate') assert.deepEqual(result, { status: 413, queueCalls: 0, serverErrors: 0 });
  }
  if (mode === '--baseline') assert.notEqual(oversize.contentLength.status, 413, 'The recorded baseline already fixes this issue.');
  console.log(JSON.stringify({ mode, oversize, atLimit: atLimitCheck.status, malformed: badJson.status,
    accessGuard: wrongToken.status, replay: replay.status, providerFailure: failure.status, fakeQueueOnly: true }));
} finally {
  await dashboard.close();
}
