const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const {
  app,
  server,
  io,
  rooms,
  isAllowedImageDataURL,
  MAX_IMAGE_DATA_URL_LENGTH,
} = require('../server');

function listen(appOrServer) {
  return new Promise((resolve) => {
    const listener = appOrServer.listen(0, '127.0.0.1', () => resolve(listener));
  });
}

function getJSON(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        try {
          resolve({ statusCode: res.statusCode, body: JSON.parse(body) });
        } catch (error) {
          reject(error);
        }
      });
    }).on('error', reject);
  });
}

function getText(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, body }));
    }).on('error', reject);
  });
}

test.after(() => {
  io.close();
  if (server.listening) server.close();
});

test('health endpoint reports a live empty process', async () => {
  const listener = await listen(app);
  try {
    const { port } = listener.address();
    const response = await getJSON(`http://127.0.0.1:${port}/health`);
    assert.equal(response.statusCode, 200);
    assert.equal(response.body.ok, true);
    assert.equal(response.body.app, 'johnbox-games');
    assert.equal(response.body.rooms, Object.keys(rooms).length);
  } finally {
    listener.close();
  }
});

test('main pages are served with socket base path placeholders replaced', async () => {
  const listener = await listen(app);
  try {
    const { port } = listener.address();
    for (const route of ['/', '/display']) {
      const response = await getText(`http://127.0.0.1:${port}${route}`);
      assert.equal(response.statusCode, 200);
      assert.match(response.body, /socket\.io/);
      assert.doesNotMatch(response.body, /__BASE_PATH__/);
    }
  } finally {
    listener.close();
  }
});

test('inline browser scripts parse', () => {
  for (const fileName of ['player.html', 'display.html']) {
    const filePath = path.join(__dirname, '..', 'public', fileName);
    const html = fs.readFileSync(filePath, 'utf8');
    const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)]
      .map((match) => match[1])
      .filter((script) => !script.includes('socket.io.js'));

    assert.ok(scripts.length > 0, `${fileName} should have inline scripts`);
    scripts.forEach((script, index) => {
      assert.doesNotThrow(() => new vm.Script(script, { filename: `${fileName}:script${index}` }));
    });
  }
});

test('image data URL guard accepts only supported bounded images', () => {
  assert.equal(isAllowedImageDataURL('data:image/jpeg;base64,abcd'), true);
  assert.equal(isAllowedImageDataURL('data:image/png;base64,abcd'), true);
  assert.equal(isAllowedImageDataURL('data:text/html;base64,abcd'), false);
  assert.equal(isAllowedImageDataURL('data:image/svg+xml;base64,abcd'), false);
  assert.equal(isAllowedImageDataURL('x'.repeat(MAX_IMAGE_DATA_URL_LENGTH + 1)), false);
});
