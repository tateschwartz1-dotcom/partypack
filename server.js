const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require('socket.io');
const path = require('path');
const os = require('os');
const fs = require('fs');
const QRCode = require('qrcode');

function loadLocalEnv() {
  const envPath = path.join(__dirname, '.env');
  if (!fs.existsSync(envPath)) return;

  const lines = fs.readFileSync(envPath, 'utf8').split(/\r?\n/);
  lines.forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) return;

    const eqIndex = trimmed.indexOf('=');
    if (eqIndex === -1) return;

    const key = trimmed.slice(0, eqIndex).trim();
    let value = trimmed.slice(eqIndex + 1).trim();

    if (!key || process.env[key]) return;
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    process.env[key] = value;
  });
}

loadLocalEnv();

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e6 });
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const ANTHROPIC_TRIVIA_MODEL = process.env.ANTHROPIC_TRIVIA_MODEL || 'claude-haiku-4-5-20251001';
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
const OPENAI_TRIVIA_MODEL = process.env.OPENAI_TRIVIA_MODEL || 'gpt-5-mini';
const TRIVIA_API_KEY = process.env.TRIVIA_API_KEY || '';

app.use(express.static('public'));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'player.html'));
});

app.get('/host', (req, res) => {
  res.redirect('/');
});

function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return 'localhost';
}

const PLAYER_COLORS = [
  '#f87171', '#fb923c', '#facc15', '#4ade80',
  '#38bdf8', '#818cf8', '#e879f9', '#f472b6',
  '#a3e635', '#34d399',
];

const HT_QUESTIONS = [
  // ── Generic ──────────────────────────────────────────────────────
  { text: "🙏 Who is most likely to accidentally start a cult?", needsPlayer: false },
  { text: "📱 Who would go viral for the dumbest reason?", needsPlayer: false },
  { text: "🚫 Who would get banned from a public place first?", needsPlayer: false },
  { text: "🤫 Who do you trust the least with a secret?", needsPlayer: false },
  { text: "💔 Who would be the worst person to date?", needsPlayer: false },
  { text: "🧠 Who thinks they're smarter than they actually are?", needsPlayer: false },
  { text: "😈 Who would survive the longest in a cheating scandal?", needsPlayer: false },
  { text: "🤥 Who would lie in a relationship and get away with it?", needsPlayer: false },
  { text: "🎭 Who would be the easiest to manipulate?", needsPlayer: false },
  { text: "💰 Who would become unbearable if they got rich?", needsPlayer: false },
  { text: "👑 Who would abuse power the most?", needsPlayer: false },
  { text: "🙈 Who is hiding something right now?", needsPlayer: false },
  { text: "🕵️ Who has the most questionable past?", needsPlayer: false },
  { text: "💀 Who would fold under pressure first?", needsPlayer: false },
  { text: "✈️ Who would ruin a group vacation?", needsPlayer: false },
  { text: "⚖️ Who would you NOT want defending you in court?", needsPlayer: false },
  { text: "📺 Who would win in a reality TV show?", needsPlayer: false },
  { text: "🎬 Who would get cast as the villain in a movie?", needsPlayer: false },
  { text: "🌟 Who would let fame change them the fastest?", needsPlayer: false },
  { text: "🎭 Who acts the most different around different people?", needsPlayer: false },
  { text: "😅 Who tries the hardest to impress others?", needsPlayer: false },
  { text: "👀 Who cares the most about what people think?", needsPlayer: false },
  { text: "📉 Who will peak the earliest in life?", needsPlayer: false },
  { text: "🌲 Who will disappear off the grid one day?", needsPlayer: false },
  { text: "🤦 Who has the worst judgment?", needsPlayer: false },
  { text: "🎲 Who makes the most questionable decisions?", needsPlayer: false },
  { text: "🚩 Who is the biggest red flag?", needsPlayer: false },
  { text: "😤 Who would go too far on a dare?", needsPlayer: false },
  { text: "🔪 Who would stab you in the back with a smile?", needsPlayer: false },
  { text: "🏠 Who would be the worst roommate?", needsPlayer: false },
  { text: "🚗 Who would flirt their way out of a speeding ticket?", needsPlayer: false },
  { text: "📸 Who is most likely to overshare on social media?", needsPlayer: false },
  { text: "🎙️ Who would end up on a true crime podcast?", needsPlayer: false },
  { text: "💸 Who would blow their savings in a week?", needsPlayer: false },
  { text: "😬 Who is faking their confidence the most?", needsPlayer: false },
  { text: "🐟 Who would get catfished and fall for it?", needsPlayer: false },
  { text: "🏃 Who would betray the group first in a survival scenario?", needsPlayer: false },
  { text: "😭 Who would cry first if stranded in the wilderness?", needsPlayer: false },
  { text: "🏝️ Who would last the longest on a deserted island?", needsPlayer: false },
  { text: "💍 Who would start the most drama at their own wedding?", needsPlayer: false },
  // ── Player-specific ───────────────────────────────────────────────
  { text: "🚌 Who would throw {player} under the bus first?", needsPlayer: true },
  { text: "🤷 Who understands {player} the least?", needsPlayer: true },
  { text: "🧐 Who is secretly judging {player} the most?", needsPlayer: true },
  { text: "📣 Who would expose {player}'s secrets first?", needsPlayer: true },
  { text: "🥊 Who would side with {player} in a fight, no matter what?", needsPlayer: true },
  { text: "😤 Who is most jealous of {player}?", needsPlayer: true },
  { text: "😬 Who would get {player} in trouble without even trying?", needsPlayer: true },
  { text: "📞 Who would {player} call first in a crisis?", needsPlayer: true },
  { text: "💵 Who would betray {player} for $100?", needsPlayer: true },
  { text: "💬 Who has the most unfiltered opinions about {player}?", needsPlayer: true },
  { text: "💒 Who would NOT invite {player} to their wedding?", needsPlayer: true },
  { text: "🫣 Who secretly admires {player} more than they show?", needsPlayer: true },
  { text: "👂 Who would talk the most behind {player}'s back?", needsPlayer: true },
  { text: "🥩 Who has the most unresolved beef with {player}?", needsPlayer: true },
  { text: "💌 Who would try to date {player}'s ex?", needsPlayer: true },
  { text: "👋 Who would benefit most from {player} disappearing for a week?", needsPlayer: true },
  { text: "😊 Who would last the longest pretending to like {player}?", needsPlayer: true },
  { text: "😈 Who has been the worst influence on {player}?", needsPlayer: true },
  { text: "🔒 Who would {player} trust the least in this room?", needsPlayer: true },
  { text: "🤐 Who knows {player}'s deepest secret?", needsPlayer: true },
];

const MAFIA_NIGHT_POLLS = [
  { question: 'Best late-night snack?', options: ['Pizza', 'Fries', 'Cereal', 'Tacos'] },
  { question: 'Pick a vacation vibe', options: ['Beach', 'Cabin', 'Big city', 'Mountains'] },
  { question: 'Best movie night pick', options: ['Action', 'Comedy', 'Horror', 'Mystery'] },
  { question: 'Choose a superpower', options: ['Flight', 'Invisibility', 'Time travel', 'Mind reading'] },
  { question: 'Best fast food fries?', options: ['McDonalds', 'Wendys', 'Chick-fil-A', 'Five Guys'] },
  { question: 'Which pet has the strongest aura?', options: ['Cat', 'Dog', 'Parrot', 'Turtle'] },
  { question: 'Pick a dream concert spot', options: ['Front row', 'Lawn seats', 'VIP box', 'Balcony'] },
  { question: 'Best rainy-day move', options: ['Nap', 'Gaming', 'Movie marathon', 'Bake something'] },
];

function selectHTQuestions(players, count) {
  const hasThirdPlayer = players.length > 2;
  const pool = hasThirdPlayer ? [...HT_QUESTIONS] : HT_QUESTIONS.filter(q => !q.needsPlayer);
  const shuffled = shuffle(pool);
  const selected = [];
  for (let i = 0; i < count; i++) {
    selected.push(shuffled[i % shuffled.length]);
  }
  return selected;
}

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function getMafiaRoleCounts(playerCount, jokerEnabled) {
  let killerCount = 1;
  if (playerCount >= 13) killerCount = 4;
  else if (playerCount >= 9) killerCount = 3;
  else if (playerCount >= 6) killerCount = 2;

  return {
    killers: killerCount,
    doctor: 1,
    joker: jokerEnabled ? 1 : 0,
    villagers: playerCount - killerCount - 1 - (jokerEnabled ? 1 : 0),
  };
}

function pickRandom(arr) {
  if (!arr.length) return null;
  return arr[Math.floor(Math.random() * arr.length)];
}

function getMafiaPollGap(counts) {
  const sortedCounts = [...counts].map((entry) => entry.count).sort((a, b) => b - a);
  const top = sortedCounts[0] || 0;
  const second = sortedCounts[1] || 0;
  return top - second;
}

function getMafiaMostPolarizedPoll(room) {
  const history = room.mafia.pollHistory || [];
  if (!history.length) return null;

  return [...history]
    .sort((a, b) => {
      const gapDiff = getMafiaPollGap(a.counts) - getMafiaPollGap(b.counts);
      if (gapDiff !== 0) return gapDiff;
      const totalVotesDiff = b.totalVotes - a.totalVotes;
      if (totalVotesDiff !== 0) return totalVotesDiff;
      return a.nightNumber - b.nightNumber;
    })[0];
}

function htmlDecode(text = '') {
  return String(text)
    .replace(/&quot;/g, '"')
    .replace(/&#039;|&apos;/g, "'")
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function httpsGetJSON(url, options = {}) {
  return new Promise((resolve, reject) => {
    const request = https.get(url, options, (res) => {
      let data = '';

      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        httpsGetJSON(res.headers.location).then(resolve).catch(reject);
        return;
      }

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        if (res.statusCode !== 200) {
          reject(new Error(`Request failed with status ${res.statusCode}: ${data.slice(0, 300)}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (error) {
          reject(error);
        }
      });
    });

    request.on('error', reject);
  });
}

function httpsPostJSON(url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const request = https.request(url, {
      method: 'POST',
      headers: {
        ...headers,
      },
    }, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`POST failed with status ${res.statusCode}: ${data.slice(0, 500)}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (error) {
          reject(error);
        }
      });
    });

    request.on('error', reject);
    request.write(JSON.stringify(body));
    request.end();
  });
}

function buildETScores(correctCount) {
  if (correctCount <= 0) return [];
  if (correctCount === 1) return [67];

  const top = 67;
  const floor = 12;
  const step = (top - floor) / (correctCount - 1);
  return Array.from({ length: correctCount }, (_, index) => (
    Math.round(top - (step * index))
  ));
}

function getETPlayerStatsTemplate() {
  return {
    correctAnswers: 0,
    firstPlaceFinishes: 0,
    fastestCorrectMs: null,
    bestTopicSection: { topic: '', score: 0 },
  };
}

async function fetchETQuestionsForTopic(topic) {
  const anthropicQuestions = await fetchETAnthropicQuestions(topic);
  if (anthropicQuestions.length >= 3) {
    return anthropicQuestions.slice(0, 3);
  }

  const openAIQuestions = await fetchETOpenAIQuestions(topic);
  if (openAIQuestions.length >= 3) {
    return openAIQuestions.slice(0, 3);
  }

  const response = await fetchETSemanticQuestions(topic);
  if (response.length >= 3) {
    return response.slice(0, 3);
  }

  return fetchETFallbackQuestions(topic);
}

async function fetchETAnthropicQuestions(topic) {
  if (!ANTHROPIC_API_KEY) return [];

  const body = {
    model: ANTHROPIC_TRIVIA_MODEL,
    max_tokens: 1200,
    system: [
      'Generate exactly 3 multiple-choice trivia questions for the given topic.',
      'Return valid JSON only.',
      'The JSON must have the shape {"questions":[{"question":"...","correctAnswer":"...","answers":["...","...","...","..."]}]}',
      'Each question must be factual, clear, and suitable for a party trivia game.',
      'Each answers array must have exactly 4 distinct answers and include the correct answer.',
      'Avoid trick questions, subjective questions, and duplicates.',
      'If the topic is narrow, reinterpret it into the closest fair trivia topic instead of refusing.',
    ].join(' '),
    messages: [
      {
        role: 'user',
        content: `Topic: ${topic}`,
      },
    ],
  };

  const response = await httpsPostJSON('https://api.anthropic.com/v1/messages', body, {
    'x-api-key': ANTHROPIC_API_KEY,
    'anthropic-version': '2023-06-01',
    'content-type': 'application/json',
  });

  if (response.error) {
    throw new Error(response.error.message || 'Anthropic request failed');
  }

  const parsed = extractAnthropicTriviaPayload(response);
  if (!parsed || !Array.isArray(parsed.questions)) {
    return [];
  }

  const normalized = parsed.questions
    .map((item, index) => normalizeGeneratedTriviaQuestion(item, `anthropic-${Date.now()}-${index}`))
    .filter(Boolean);

  return normalized.slice(0, 3);
}

function extractAnthropicTriviaPayload(response) {
  if (!response || !Array.isArray(response.content)) return null;

  const textParts = response.content
    .filter((item) => item && item.type === 'text' && typeof item.text === 'string')
    .map((item) => item.text);

  for (const text of textParts) {
    try {
      return JSON.parse(text);
    } catch (error) {
      const match = text.match(/\{[\s\S]*\}/);
      if (match) {
        try {
          return JSON.parse(match[0]);
        } catch (innerError) {
          // Keep trying other content blocks.
        }
      }
    }
  }

  return null;
}

async function fetchETOpenAIQuestions(topic) {
  if (!OPENAI_API_KEY) return [];

  const schema = {
    name: 'everything_trivia_questions',
    strict: true,
    schema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        questions: {
          type: 'array',
          minItems: 3,
          maxItems: 3,
          items: {
            type: 'object',
            additionalProperties: false,
            properties: {
              question: { type: 'string' },
              correctAnswer: { type: 'string' },
              answers: {
                type: 'array',
                minItems: 4,
                maxItems: 4,
                items: { type: 'string' },
              },
            },
            required: ['question', 'correctAnswer', 'answers'],
          },
        },
      },
      required: ['questions'],
    },
  };

  const body = {
    model: OPENAI_TRIVIA_MODEL,
    input: [
      {
        role: 'system',
        content: [
          {
            type: 'input_text',
            text: [
              'Generate exactly 3 multiple-choice trivia questions for the given topic.',
              'The questions must be factual, broadly answerable, and suitable for a party trivia game.',
              'Avoid trick questions, subjective questions, and ambiguous wording.',
              'Each question must have exactly 4 distinct answer choices, with exactly 1 correct answer.',
              'Include the correct answer inside the answers array.',
              'Use concise wording and keep difficulty mixed but fair.',
              'If the topic is too narrow, reinterpret it into the closest fair trivia topic instead of refusing.',
            ].join(' '),
          },
        ],
      },
      {
        role: 'user',
        content: [
          {
            type: 'input_text',
            text: `Topic: ${topic}`,
          },
        ],
      },
    ],
    text: {
      format: {
        type: 'json_schema',
        ...schema,
      },
    },
    reasoning: {
      effort: 'minimal',
    },
  };

  const response = await httpsPostJSON('https://api.openai.com/v1/responses', body, {
    Authorization: `Bearer ${OPENAI_API_KEY}`,
    'Content-Type': 'application/json',
  });

  if (response.error) {
    throw new Error(response.error.message || 'OpenAI request failed');
  }

  const parsed = extractOpenAITriviaPayload(response);
  if (!parsed || !Array.isArray(parsed.questions)) {
    return [];
  }

  const normalized = parsed.questions
    .map((item, index) => normalizeGeneratedTriviaQuestion(item, `openai-${Date.now()}-${index}`))
    .filter(Boolean);

  return normalized.slice(0, 3);
}

function summarizeProviderError(error) {
  const message = String(error && error.message ? error.message : error || '');

  if (message.includes('401') || message.toLowerCase().includes('invalid_api_key')) {
    return 'The AI API key was rejected.';
  }
  if (message.includes('429') || message.toLowerCase().includes('rate limit')) {
    return 'The AI request hit a rate limit.';
  }
  if (message.includes('insufficient_quota')) {
    return 'The AI account has no available quota.';
  }
  if (message.includes('ENOTFOUND') || message.includes('ECONNRESET') || message.includes('ETIMEDOUT')) {
    return 'Could not reach the AI provider over the network.';
  }

  return 'The trivia generator returned an unexpected provider error.';
}

function extractOpenAITriviaPayload(response) {
  if (response.output_parsed) {
    return response.output_parsed;
  }

  if (Array.isArray(response.output)) {
    for (const item of response.output) {
      if (!item || !Array.isArray(item.content)) continue;
      for (const contentItem of item.content) {
        const maybeText = contentItem && (contentItem.text || contentItem.json);
        if (typeof maybeText === 'string') {
          try {
            return JSON.parse(maybeText);
          } catch (error) {
            // Try the next candidate.
          }
        }
        if (maybeText && typeof maybeText === 'object') {
          return maybeText;
        }
      }
    }
  }

  return null;
}

function normalizeGeneratedTriviaQuestion(item, idPrefix) {
  if (!item || typeof item.question !== 'string' || typeof item.correctAnswer !== 'string' || !Array.isArray(item.answers)) {
    return null;
  }

  const answers = item.answers
    .map((answer) => htmlDecode(String(answer).trim()))
    .filter(Boolean);
  const correctAnswer = htmlDecode(String(item.correctAnswer).trim());
  const question = htmlDecode(String(item.question).trim());

  if (!question || !correctAnswer || answers.length !== 4) return null;
  if (!answers.includes(correctAnswer)) return null;
  if (new Set(answers).size !== 4) return null;

  return {
    id: `${idPrefix}-${Math.random()}`,
    question,
    answers: shuffle(answers),
    correctAnswer,
  };
}

async function fetchETSemanticQuestions(topic) {
  if (!TRIVIA_API_KEY) return [];

  const url = `https://the-trivia-api.com/v2/questions?limit=12&query=${encodeURIComponent(topic)}`;
  const response = await httpsGetJSON(url, {
    headers: {
      'x-api-key': TRIVIA_API_KEY,
    },
  });

  return normalizeETQuestions(response).slice(0, 3);
}

function normalizeETQuestions(response) {

  if (!Array.isArray(response)) {
    return [];
  }

  return response
    .filter((item) => (
      item &&
      String(item.type || '').toLowerCase().includes('multiple') &&
      (typeof item.question === 'string' || (item.question && typeof item.question.text === 'string')) &&
      typeof item.correctAnswer === 'string' &&
      Array.isArray(item.incorrectAnswers) &&
      item.incorrectAnswers.length >= 3
    ))
    .map((item, index) => {
      const answers = shuffle([
        htmlDecode(item.correctAnswer),
        ...item.incorrectAnswers.slice(0, 3).map((answer) => htmlDecode(answer)),
      ]);

      return {
        id: item.id || `${Date.now()}-${index}-${Math.random()}`,
        question: htmlDecode(typeof item.question === 'string' ? item.question : item.question.text),
        answers,
        correctAnswer: htmlDecode(item.correctAnswer),
      };
    })
    .filter((item) => new Set(item.answers).size === 4);
}

function buildETFuzzyNeedles(topic) {
  return topic
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .map((part) => part.trim())
    .filter((part) => part.length >= 3);
}

function getETFallbackCategoryCandidates(topic) {
  const lower = topic.toLowerCase();
  const groups = [
    { category: 'music', checks: ['music', 'song', 'songs', 'band', 'bands', 'album', 'albums', 'singer', 'singers', 'rapper', 'pop', 'rock', 'hip hop', 'hip-hop'] },
    { category: 'film_and_tv', checks: ['movie', 'movies', 'film', 'films', 'tv', 'television', 'show', 'shows', 'series', 'disney', 'pixar', 'marvel', 'star wars', 'sitcom'] },
    { category: 'sport_and_leisure', checks: ['sport', 'sports', 'nba', 'nfl', 'soccer', 'football', 'baseball', 'hockey', 'tennis', 'golf', 'olympics', 'wrestling'] },
    { category: 'history', checks: ['history', 'historical', 'war', 'wars', 'roman', 'rome', 'president', 'presidents', 'ancient', 'medieval'] },
    { category: 'science', checks: ['science', 'space', 'planet', 'planets', 'physics', 'chemistry', 'biology', 'dinosaur', 'dinosaurs', 'animal', 'animals', 'shark', 'sharks'] },
    { category: 'geography', checks: ['geography', 'country', 'countries', 'city', 'cities', 'capital', 'capitals', 'world'] },
    { category: 'arts_and_literature', checks: ['book', 'books', 'author', 'authors', 'literature', 'art', 'arts', 'painting', 'paintings', 'poetry', 'mythology', 'greek'] },
    { category: 'general_knowledge', checks: ['food', 'foods', 'brand', 'brands', 'internet', 'technology', 'video game', 'video games', 'pokemon'] },
  ];

  const matches = groups
    .filter((group) => group.checks.some((check) => lower.includes(check)))
    .map((group) => group.category);

  if (matches.length > 0) return matches;
  return ['general_knowledge', 'science', 'history', 'geography', 'film_and_tv', 'music', 'sport_and_leisure', 'arts_and_literature'];
}

async function fetchETFallbackQuestions(topic) {
  const categories = getETFallbackCategoryCandidates(topic);
  const needles = buildETFuzzyNeedles(topic);
  const url = `https://the-trivia-api.com/v2/questions?limit=24&categories=${encodeURIComponent(categories.join(','))}`;
  const response = await httpsGetJSON(url);
  const normalized = normalizeETQuestions(response);

  const scored = normalized
    .map((question) => {
      const haystack = `${question.question} ${question.correctAnswer} ${question.answers.join(' ')}`.toLowerCase();
      let score = 0;
      needles.forEach((needle) => {
        if (haystack.includes(needle)) score += 3;
      });
      categories.forEach((category) => {
        if (haystack.includes(category.replace(/_/g, ' '))) score += 1;
      });
      return { question, score };
    })
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score);

  const uniqueQuestions = [];
  const seen = new Set();
  scored.forEach(({ question }) => {
    if (seen.has(question.question)) return;
    seen.add(question.question);
    uniqueQuestions.push(question);
  });

  return uniqueQuestions.slice(0, 3);
}

const rooms = {};

function generatePIN() {
  let pin;
  do {
    pin = Math.floor(1000 + Math.random() * 9000).toString();
  } while (rooms[pin]);
  return pin;
}

function clearRoomTimers(room) {
  if (room.timers) {
    room.timers.forEach((t) => clearInterval(t));
    room.timers = [];
  }
}

io.on('connection', (socket) => {

  // ── Lobby ──────────────────────────────────────────────────────

  socket.on('create-room', async ({ hostName }) => {
    const pin = generatePIN();
    const localIP = getLocalIP();
    const playerURL = process.env.PUBLIC_URL || `http://${localIP}:3000`;
    const qrDataURL = await QRCode.toDataURL(playerURL, { width: 200, margin: 2 });

    const hostPlayer = { id: socket.id, name: hostName };

    rooms[pin] = {
      pin,
      hostSocketId: socket.id,
      hostName,
      players: [hostPlayer],
      gameState: 'lobby',
      localIP,
      playerURL,
      qrDataURL,
      timers: [],
    };

    socket.join(pin);
    socket.data.pin = pin;
    socket.data.isHost = true;
    socket.data.name = hostName;

    socket.emit('room-created', { pin, playerURL, qrDataURL, players: [hostPlayer] });
  });

  socket.on('join-room', ({ pin, name }) => {
    const room = rooms[pin];

    if (!room) {
      socket.emit('join-error', { message: 'Room not found. Check the PIN.' });
      return;
    }
    if (room.gameState !== 'lobby') {
      socket.emit('join-error', { message: 'Game already in progress.' });
      return;
    }

    const nameTaken = room.players.some(
      (p) => p.name.toLowerCase() === name.toLowerCase()
    );
    if (nameTaken) {
      socket.emit('join-error', { message: 'That name is already taken.' });
      return;
    }

    const player = { id: socket.id, name };
    room.players.push(player);

    socket.join(pin);
    socket.data.pin = pin;
    socket.data.name = name;
    socket.data.isHost = false;

    socket.emit('joined-room', {
      name,
      players: room.players,
      pin,
      qrDataURL: room.qrDataURL,
      playerURL: room.playerURL,
      hostId: room.hostSocketId,
    });
    io.to(pin).emit('player-list-updated', { players: room.players, hostId: room.hostSocketId });
  });

  socket.on('transfer-host', ({ targetPlayerId }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    room.hostSocketId = targetPlayerId;
    socket.data.isHost = false;

    const targetSocket = io.sockets.sockets.get(targetPlayerId);
    if (targetSocket) targetSocket.data.isHost = true;

    io.to(targetPlayerId).emit('you-are-now-host');
    socket.emit('host-transferred');
    io.to(pin).emit('host-changed', { newHostId: targetPlayerId });
  });

  // ── Game selector ──────────────────────────────────────────────

  socket.on('start-game', ({ game, exposureChance }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    room.gameState = game;

    if (game === 'photo-roulette') {
      const scores = {};
      room.players.forEach(p => { scores[p.name] = 0; });
      room.pr = {
        photos: [],
        usedIds: new Set(),
        currentPhoto: null,
        guesses: {},
        scores,
      };
    }

    if (game === 'hot-takes') {
      const playerColors = {};
      room.players.forEach((p, i) => {
        playerColors[p.name] = PLAYER_COLORS[i % PLAYER_COLORS.length];
      });
      room.ht = {
        selectedQuestions: selectHTQuestions(room.players, 20),
        currentRound: 0,
        totalRounds: 20,
        votes: {},
        optionA: null,
        optionB: null,
        currentQuestionText: '',
        revealVotes: false,
        exposureChance: (typeof exposureChance === 'number') ? exposureChance : 0.10,
        playerColors,
      };
    }

    if (game === 'everything-trivia') {
      const scores = {};
      const stats = {};
      room.players.forEach((player) => {
        scores[player.name] = 0;
        stats[player.name] = getETPlayerStatsTemplate();
      });

      room.et = {
        topicsByPlayerId: {},
        randomizedTopics: [],
        currentSectionIndex: -1,
        currentQuestionIndex: -1,
        currentQuestion: null,
        currentAnswers: {},
        currentQuestionStartedAt: 0,
        scores,
        stats,
        sectionScoreboard: {},
      };
    }

    if (game === 'mafia') {
      room.mafia = {
        jokerEnabled: false,
        hiddenPollsUntilEnd: true,
        started: false,
        roles: {},
        alive: {},
        phase: 'setup',
        dayNumber: 1,
        nightNumber: 0,
        votes: {},
        revoteCandidates: [],
        isRevote: false,
        discussionTimeLeft: 180,
        voteTimeLeft: 30,
        eventMessage: 'Configure the game and start Day 1 when ready.',
        eventTone: 'neutral',
        deathLog: [],
        pendingPoll: null,
        lastPollResult: null,
        pollHistory: [],
        finalPolarizedPoll: null,
        pendingNight: {
          killerVotes: {},
          doctorTarget: null,
          pollVotes: {},
        },
        doctorLastProtectedId: null,
        pendingWinner: null,
        winner: null,
      };
    }

    io.to(pin).emit('game-started', { game });

    if (game === 'hot-takes') {
      startHTRound(pin);
    }

    if (game === 'everything-trivia') {
      emitETTopicStatus(pin);
    }

    if (game === 'mafia') {
      emitMafiaState(pin);
    }
  });

  socket.on('back-to-lobby', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    clearRoomTimers(room);
    room.gameState = 'lobby';
    room.qc = null;
    room.pr = null;
    room.ht = null;
    room.et = null;
    room.mafia = null;
    io.to(pin).emit('returned-to-lobby');
  });

  socket.on('back-to-game-select', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    clearRoomTimers(room);
    room.gameState = 'lobby';
    room.qc = null;
    room.pr = null;
    room.ht = null;
    room.et = null;
    room.mafia = null;
    io.to(pin).emit('returned-to-game-select');
  });

  // ── Questions & Challenges ─────────────────────────────────────

  socket.on('qc-start-collecting', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    const COLLECT_TIME = 60;

    room.qc = {
      submissions: [],
      playerOrder: [...room.players],
      currentPlayerIndex: 0,
      currentSubmission: null,
      usedIds: new Set(),
    };

    let timeLeft = COLLECT_TIME;
    io.to(pin).emit('qc-collecting', { timeLeft, totalTime: COLLECT_TIME });

    const timer = setInterval(() => {
      timeLeft--;
      io.to(pin).emit('qc-timer-tick', { timeLeft, totalTime: COLLECT_TIME });

      if (timeLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter((t) => t !== timer);
        startQCRound(pin);
      }
    }, 1000);

    room.timers.push(timer);
  });

  socket.on('qc-submit', ({ text, type }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.qc) return;

    const trimmed = text.trim();
    if (!trimmed) return;

    room.qc.submissions.push({
      id: Date.now() + Math.random(),
      text: trimmed,
      type,
    });

    io.to(pin).emit('qc-submission-count', { count: room.qc.submissions.length });
    socket.emit('qc-submit-ack');
  });

  socket.on('qc-close-submissions', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    clearRoomTimers(room);
    startQCRound(pin);
  });

  socket.on('qc-spin', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.qc) return;
    const currentPlayer = room.qc.playerOrder[room.qc.currentPlayerIndex];
    if (!currentPlayer || socket.id !== currentPlayer.id) return;

    const available = room.qc.submissions.filter((s) => !room.qc.usedIds.has(s.id));

    if (available.length === 0) {
      io.to(pin).emit('qc-game-over');
      clearRoomTimers(room);
      room.gameState = 'lobby';
      return;
    }

    const isLast = available.length === 1;
    const picked = available[Math.floor(Math.random() * available.length)];
    room.qc.usedIds.add(picked.id);
    room.qc.currentSubmission = picked;

    io.to(pin).emit('qc-spin-result', { picked, isLast });
  });

  socket.on('qc-finish', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;
    io.to(pin).emit('qc-game-over');
    clearRoomTimers(room);
    room.gameState = 'lobby';
  });

  socket.on('qc-next-player', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.qc) return;

    room.qc.currentPlayerIndex = (room.qc.currentPlayerIndex + 1) % room.qc.playerOrder.length;
    const currentPlayer = room.qc.playerOrder[room.qc.currentPlayerIndex];
    const available = room.qc.submissions
      .filter(s => !room.qc.usedIds.has(s.id))
      .map(s => ({ id: s.id, type: s.type }));
    io.to(pin).emit('qc-next-turn', { currentPlayer, available });
  });

  // ── Photo Roulette ─────────────────────────────────────────────

  socket.on('pr-upload-photo', ({ dataURL }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.pr) return;

    const myPhotos = room.pr.photos.filter(p => p.playerId === socket.id);
    if (myPhotos.length >= 3) {
      socket.emit('pr-upload-error', { message: 'You can only upload 3 photos.' });
      return;
    }

    room.pr.photos.push({
      id: Date.now() + Math.random(),
      playerId: socket.id,
      playerName: socket.data.name,
      dataURL,
    });

    socket.emit('pr-upload-ack', { myCount: myPhotos.length + 1 });
    io.to(pin).emit('pr-photo-count', { count: room.pr.photos.length });
  });

  socket.on('pr-close-uploads', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.pr) return;

    if (room.pr.photos.length === 0) {
      socket.emit('pr-no-photos');
      return;
    }

    startPRRound(pin);
  });

  socket.on('pr-guess', ({ guessedName }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.pr || !room.pr.currentPhoto) return;
    if (room.pr.guesses[socket.id]) return;

    room.pr.guesses[socket.id] = { guesserName: socket.data.name, guessedName };
    socket.emit('pr-guess-ack');

    const guessCount = Object.keys(room.pr.guesses).length;
    io.to(pin).emit('pr-guess-count', { count: guessCount, total: room.players.length });

    if (guessCount >= room.players.length) {
      clearRoomTimers(room);
      doPRReveal(pin);
    }
  });

  socket.on('pr-next-photo', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.pr) return;
    clearRoomTimers(room);
    startPRRound(pin);
  });

  socket.on('pr-reveal-now', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.pr) return;
    clearRoomTimers(room);
    doPRReveal(pin);
  });

  // ── Hot Takes ──────────────────────────────────────────────────

  socket.on('ht-vote', ({ votedFor }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.ht) return;
    if (room.ht.votes[socket.id]) return;

    room.ht.votes[socket.id] = votedFor;
    socket.emit('ht-vote-ack');

    const voteCount = Object.keys(room.ht.votes).length;
    io.to(pin).emit('ht-vote-count', { count: voteCount, total: room.players.length });

    if (voteCount >= room.players.length) {
      clearRoomTimers(room);
      doHTReveal(pin);
    }
  });

  // ── Helpers ────────────────────────────────────────────────────

  socket.on('et-submit-topic', async ({ topic }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.et) return;

    const trimmedTopic = String(topic || '').trim();
    if (!trimmedTopic) return;

    const player = room.players.find((p) => p.id === socket.id);
    if (!player) return;

    const requestId = Date.now() + Math.random();
    room.et.topicsByPlayerId[socket.id] = {
      playerId: socket.id,
      playerName: player.name,
      topic: trimmedTopic,
      questions: [],
      status: 'validating',
      requestId,
    };

    emitETTopicStatus(pin);

    try {
      const questions = await fetchETQuestionsForTopic(trimmedTopic);
      const currentEntry = room.et && room.et.topicsByPlayerId[socket.id];
      if (!currentEntry || currentEntry.requestId !== requestId) return;

      if (questions.length < 3) {
        currentEntry.status = 'needs-resubmit';
        currentEntry.questions = [];
        socket.emit('et-topic-invalid', {
          topic: trimmedTopic,
          message: `Couldn't find 3 solid questions for "${trimmedTopic}". Try a new topic.`,
        });
        emitETTopicStatus(pin);
        return;
      }

      currentEntry.status = 'ready';
      currentEntry.questions = questions;

      socket.emit('et-topic-accepted', { topic: trimmedTopic });
      emitETTopicStatus(pin);

      const allReady = room.players.every((roomPlayer) => {
        const entry = room.et.topicsByPlayerId[roomPlayer.id];
        return entry && entry.status === 'ready' && entry.questions.length === 3;
      });

      if (allReady) {
        startETGame(pin);
      }
    } catch (error) {
      const currentEntry = room.et && room.et.topicsByPlayerId[socket.id];
      if (!currentEntry || currentEntry.requestId !== requestId) return;

      currentEntry.status = 'needs-resubmit';
      currentEntry.questions = [];
      const reason = summarizeProviderError(error);
      console.error(`[Everything Trivia] Topic generation failed for "${trimmedTopic}": ${error && error.message ? error.message : error}`);
      socket.emit('et-topic-invalid', {
        topic: trimmedTopic,
        message: `${reason} Try a different topic, or check the server setup.`,
      });
      emitETTopicStatus(pin);
    }
  });

  socket.on('et-answer', ({ answer }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.et || !room.et.currentQuestion) return;
    if (room.et.currentAnswers[socket.id]) return;

    const answerText = String(answer || '');
    if (!room.et.currentQuestion.answers.includes(answerText)) return;

    room.et.currentAnswers[socket.id] = {
      playerId: socket.id,
      playerName: socket.data.name,
      answer: answerText,
      submittedAt: Date.now(),
    };

    socket.emit('et-answer-locked');
    io.to(pin).emit('et-answer-count', {
      count: Object.keys(room.et.currentAnswers).length,
      total: room.players.length,
    });

    if (Object.keys(room.et.currentAnswers).length >= room.players.length) {
      clearRoomTimers(room);
      doETReveal(pin);
    }
  });

  socket.on('mafia-update-settings', ({ jokerEnabled, hiddenPollsUntilEnd }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.mafia || room.mafia.started) return;
    room.mafia.jokerEnabled = !!jokerEnabled;
    room.mafia.hiddenPollsUntilEnd = hiddenPollsUntilEnd !== undefined ? !!hiddenPollsUntilEnd : room.mafia.hiddenPollsUntilEnd;
    emitMafiaState(pin);
  });

  socket.on('mafia-begin', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.mafia || room.mafia.started) return;
    if (room.players.length < 4) {
      room.mafia.eventMessage = 'Mafia needs at least 4 players to begin.';
      emitMafiaState(pin);
      return;
    }

    assignMafiaRoles(room);
    room.mafia.started = true;
    room.mafia.eventMessage = 'Roles assigned. Day 1 begins now.';
    room.mafia.eventTone = 'neutral';
    startMafiaDiscussion(pin);
  });

  socket.on('mafia-start-discussion', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.mafia || !room.mafia.started) return;
    startMafiaDiscussion(pin);
  });

  socket.on('mafia-start-voting', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.mafia || room.mafia.phase !== 'discussion') return;
    startMafiaVoting(pin, false, []);
  });

  socket.on('mafia-start-night', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.mafia || room.mafia.phase !== 'day-result') return;
    startMafiaNight(pin);
  });

  socket.on('mafia-cast-vote', ({ targetId }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.mafia || !['voting', 'revote'].includes(room.mafia.phase)) return;
    if (!room.mafia.alive[socket.id]) return;

    const allowedIds = room.mafia.isRevote
      ? room.mafia.revoteCandidates.filter((id) => id !== socket.id)
      : getMafiaLivingPlayers(room).map((player) => player.id).filter((id) => id !== socket.id);
    if (targetId !== '__abstain__' && !allowedIds.includes(targetId)) return;

    if (room.mafia.votes[socket.id] === targetId) {
      delete room.mafia.votes[socket.id];
    } else {
      room.mafia.votes[socket.id] = targetId;
    }
    emitMafiaState(pin);

    const voteCounts = getMafiaVoteCounts(room);
    const livingCount = getMafiaLivingPlayers(room).length;
    const hasMajority = Object.values(voteCounts).some((count) => count > livingCount / 2);
    if (hasMajority) {
      clearRoomTimers(room);
      resolveMafiaVote(pin, true);
    }
  });

  socket.on('mafia-night-action', ({ targetId }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.mafia || room.mafia.phase !== 'night' || !room.mafia.alive[socket.id]) return;

    const role = room.mafia.roles[socket.id];
    if (role === 'killer') {
      const allowedTargets = getMafiaLivingPlayers(room)
        .filter((player) => room.mafia.roles[player.id] !== 'killer')
        .map((player) => player.id);
      if (!allowedTargets.includes(targetId)) return;
      room.mafia.pendingNight.killerVotes[socket.id] = targetId;
      emitMafiaState(pin);
      return;
    }

    if (role === 'doctor') {
      const allowedTargets = getMafiaLivingPlayers(room)
        .map((player) => player.id)
        .filter((playerId) => playerId !== room.mafia.doctorLastProtectedId);
      if (!allowedTargets.includes(targetId)) return;
      room.mafia.pendingNight.doctorTarget = targetId;
      emitMafiaState(pin);
    }
  });

  socket.on('mafia-night-poll', ({ choice }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.mafia || room.mafia.phase !== 'night' || !room.mafia.alive[socket.id]) return;

    const role = room.mafia.roles[socket.id];
    if (!['villager', 'joker'].includes(role)) return;
    if (!room.mafia.pendingPoll || !room.mafia.pendingPoll.options.includes(choice)) return;

    room.mafia.pendingNight.pollVotes[socket.id] = choice;
    emitMafiaState(pin);
  });

  function startQCRound(pin) {
    const room = rooms[pin];
    if (!room || !room.qc) return;

    room.qc.submissions = shuffle(room.qc.submissions);

    if (room.qc.submissions.length === 0) {
      io.to(pin).emit('qc-no-submissions');
      room.gameState = 'lobby';
      return;
    }

    const currentPlayer = room.qc.playerOrder[0];
    const available = room.qc.submissions.map(s => ({ id: s.id, type: s.type }));
    io.to(pin).emit('qc-round-start', { currentPlayer, available });
  }

  function startPRRound(pin) {
    const room = rooms[pin];
    if (!room || !room.pr) return;

    const available = room.pr.photos.filter(p => !room.pr.usedIds.has(p.id));

    if (available.length === 0) {
      const scores = Object.entries(room.pr.scores)
        .map(([name, score]) => ({ name, score }))
        .sort((a, b) => b.score - a.score);
      io.to(pin).emit('pr-game-over', { scores });
      room.gameState = 'lobby';
      return;
    }

    const photo = available[Math.floor(Math.random() * available.length)];
    room.pr.usedIds.add(photo.id);
    room.pr.currentPhoto = photo;
    room.pr.guesses = {};

    const photoNumber = room.pr.photos.length - available.length + 1;
    const totalPhotos = room.pr.photos.length;

    // Build guess options — max 6, always include the photographer
    let guessOptions = room.players.map(p => p.name);
    if (guessOptions.length > 6) {
      const others = guessOptions.filter(n => n !== photo.playerName);
      guessOptions = shuffle([photo.playerName, ...shuffle(others).slice(0, 5)]);
    }

    // Everyone guesses — including the photographer (who should pick themselves)
    for (const player of room.players) {
      io.to(player.id).emit('pr-guess-prompt', {
        guessOptions,
        photoNumber,
        totalPhotos,
        photoData: photo.dataURL,
        isYourPhoto: player.id === photo.playerId,
      });
    }

    io.to(pin).emit('pr-guess-count', { count: 0, total: room.players.length });

    const GUESS_TIME = 8;
    let timeLeft = GUESS_TIME;
    io.to(pin).emit('pr-timer-tick', { timeLeft, totalTime: GUESS_TIME });

    const timer = setInterval(() => {
      timeLeft--;
      io.to(pin).emit('pr-timer-tick', { timeLeft, totalTime: GUESS_TIME });
      if (timeLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter(t => t !== timer);
        doPRReveal(pin);
      }
    }, 1000);

    room.timers.push(timer);
  }

  function doPRReveal(pin) {
    const room = rooms[pin];
    if (!room || !room.pr || !room.pr.currentPhoto) return;

    const photo = room.pr.currentPhoto;
    const guesses = Object.values(room.pr.guesses);
    const pointsThisRound = {};
    let foolPoints = 0;

    guesses.forEach(({ guesserName, guessedName }) => {
      if (guessedName === photo.playerName) {
        // Correct guess (including photographer picking themselves)
        room.pr.scores[guesserName] = (room.pr.scores[guesserName] || 0) + 1;
        pointsThisRound[guesserName] = (pointsThisRound[guesserName] || 0) + 1;
      } else if (guesserName !== photo.playerName) {
        // Only non-photographer wrong guesses fool the photographer
        foolPoints++;
      }
    });

    if (foolPoints > 0) {
      room.pr.scores[photo.playerName] = (room.pr.scores[photo.playerName] || 0) + foolPoints;
      pointsThisRound[photo.playerName] = (pointsThisRound[photo.playerName] || 0) + foolPoints;
    }

    const remaining = room.pr.photos.filter(p => !room.pr.usedIds.has(p.id));

    io.to(pin).emit('pr-reveal', {
      photographerName: photo.playerName,
      guesses,
      pointsThisRound,
      scores: room.pr.scores,
      hasMorePhotos: remaining.length > 0,
    });

    // Auto-advance to next photo after 8 seconds
    const PR_ADVANCE_TIME = 8;
    let prAdvLeft = PR_ADVANCE_TIME;

    const prAdvTimer = setInterval(() => {
      prAdvLeft--;
      io.to(pin).emit('pr-advance-tick', { timeLeft: prAdvLeft });
      if (prAdvLeft <= 0) {
        clearInterval(prAdvTimer);
        room.timers = room.timers.filter(t => t !== prAdvTimer);
        startPRRound(pin);
      }
    }, 1000);

    room.timers.push(prAdvTimer);
  }

  function startHTRound(pin) {
    const room = rooms[pin];
    if (!room || !room.ht) return;

    room.ht.currentRound++;

    if (room.ht.currentRound > room.ht.totalRounds) {
      io.to(pin).emit('ht-game-over');
      room.gameState = 'lobby';
      return;
    }

    room.ht.votes = {};
    room.ht.revealVotes = Math.random() < room.ht.exposureChance;

    const shuffledPlayers = shuffle([...room.players]);
    const optionA = shuffledPlayers[0];
    const optionB = shuffledPlayers[1];
    room.ht.optionA = optionA;
    room.ht.optionB = optionB;

    const q = room.ht.selectedQuestions[room.ht.currentRound - 1];
    let questionText = q.text;

    if (q.needsPlayer) {
      const others = room.players.filter(p => p.id !== optionA.id && p.id !== optionB.id);
      const ref = others.length > 0
        ? others[Math.floor(Math.random() * others.length)]
        : optionA;
      questionText = q.text.replace('{player}', ref.name);
    }

    room.ht.currentQuestionText = questionText;

    const roundData = {
      question: questionText,
      optionA: { id: optionA.id, name: optionA.name },
      optionB: { id: optionB.id, name: optionB.name },
      roundNumber: room.ht.currentRound,
      totalRounds: room.ht.totalRounds,
      playerColors: room.ht.playerColors,
    };

    io.to(pin).emit('ht-round-start', roundData);
    io.to(pin).emit('ht-vote-count', { count: 0, total: room.players.length });

    const VOTE_TIME = 8;
    let timeLeft = VOTE_TIME;
    io.to(pin).emit('ht-timer-tick', { timeLeft, totalTime: VOTE_TIME });

    const timer = setInterval(() => {
      timeLeft--;
      io.to(pin).emit('ht-timer-tick', { timeLeft, totalTime: VOTE_TIME });
      if (timeLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter(t => t !== timer);
        doHTReveal(pin);
      }
    }, 1000);

    room.timers.push(timer);
  }

  function doHTReveal(pin) {
    const room = rooms[pin];
    if (!room || !room.ht) return;

    const { votes, optionA, optionB, currentQuestionText, revealVotes, playerColors } = room.ht;

    let votesA = 0, votesB = 0;
    const voteDetails = [];

    Object.entries(votes).forEach(([playerId, votedFor]) => {
      const player = room.players.find(p => p.id === playerId);
      if (!player) return;
      if (votedFor === 'A') votesA++;
      else votesB++;

      if (revealVotes) {
        voteDetails.push({
          voterName: player.name,
          voterColor: playerColors[player.name] || '#ede9f8',
          votedForName: votedFor === 'A' ? optionA.name : optionB.name,
          votedForColor: votedFor === 'A'
            ? (playerColors[optionA.name] || '#cfb991')
            : (playerColors[optionB.name] || '#cfb991'),
        });
      }
    });

    const totalVoted = votesA + votesB;
    const pctA = totalVoted > 0 ? Math.round((votesA / totalVoted) * 100) : 50;
    const pctB = totalVoted > 0 ? 100 - pctA : 50;

    // Auto-advance countdown (shorter when votes are anonymous — less to read)
    const ADVANCE_TIME = revealVotes ? 8 : 6;

    io.to(pin).emit('ht-reveal', {
      question: currentQuestionText,
      optionA: { id: optionA.id, name: optionA.name },
      optionB: { id: optionB.id, name: optionB.name },
      pctA, pctB, votesA, votesB, totalVoted,
      revealVotes,
      voteDetails,
      playerColors,
      hasMoreRounds: room.ht.currentRound < room.ht.totalRounds,
      advanceTime: ADVANCE_TIME,
    });
    let advLeft = ADVANCE_TIME;

    io.to(pin).emit('ht-advance-tick', { timeLeft: advLeft });

    const advTimer = setInterval(() => {
      advLeft--;
      io.to(pin).emit('ht-advance-tick', { timeLeft: advLeft });
      if (advLeft <= 0) {
        clearInterval(advTimer);
        room.timers = room.timers.filter(t => t !== advTimer);
        startHTRound(pin);
      }
    }, 1000);

    room.timers.push(advTimer);
  }

  // ── Disconnect ─────────────────────────────────────────────────

  function emitETTopicStatus(pin) {
    const room = rooms[pin];
    if (!room || !room.et) return;

    const topicEntries = room.players.map((player) => {
      const entry = room.et.topicsByPlayerId[player.id];
      return {
        playerId: player.id,
        playerName: player.name,
        topic: entry ? entry.topic : '',
        status: entry ? entry.status : 'waiting',
      };
    });

    const readyCount = topicEntries.filter((entry) => entry.status === 'ready').length;
    io.to(pin).emit('et-topic-status', {
      readyCount,
      total: room.players.length,
      topics: topicEntries,
    });
  }

  function startETGame(pin) {
    const room = rooms[pin];
    if (!room || !room.et) return;
    if (room.et.randomizedTopics.length > 0) return;

    room.et.randomizedTopics = shuffle(
      room.players.map((player) => room.et.topicsByPlayerId[player.id])
    );
    room.et.currentSectionIndex = -1;
    room.et.currentQuestionIndex = -1;

    startETSection(pin);
  }

  function startETSection(pin) {
    const room = rooms[pin];
    if (!room || !room.et) return;

    room.et.currentSectionIndex += 1;
    room.et.currentQuestionIndex = -1;

    if (room.et.currentSectionIndex >= room.et.randomizedTopics.length) {
      finishETGame(pin);
      return;
    }

    const section = room.et.randomizedTopics[room.et.currentSectionIndex];
    room.et.sectionScoreboard = {};
    room.players.forEach((player) => {
      room.et.sectionScoreboard[player.name] = 0;
    });

    io.to(pin).emit('et-section-intro', {
      topic: section.topic,
      submittedBy: section.playerName,
      sectionNumber: room.et.currentSectionIndex + 1,
      totalSections: room.et.randomizedTopics.length,
    });

    const introTimer = setTimeout(() => {
      room.timers = room.timers.filter((timer) => timer !== introTimer);
      startETQuestion(pin);
    }, 3500);

    room.timers.push(introTimer);
  }

  function startETQuestion(pin) {
    const room = rooms[pin];
    if (!room || !room.et) return;

    room.et.currentQuestionIndex += 1;
    const section = room.et.randomizedTopics[room.et.currentSectionIndex];

    if (room.et.currentQuestionIndex >= section.questions.length) {
      const leaderboard = room.players
        .map((player) => ({
          name: player.name,
          score: room.et.scores[player.name] || 0,
          sectionScore: room.et.sectionScoreboard[player.name] || 0,
        }))
        .sort((a, b) => (b.sectionScore - a.sectionScore) || (b.score - a.score) || a.name.localeCompare(b.name));

      io.to(pin).emit('et-section-scoreboard', {
        topic: section.topic,
        sectionNumber: room.et.currentSectionIndex + 1,
        totalSections: room.et.randomizedTopics.length,
        leaderboard,
      });

      const scoreboardTimer = setTimeout(() => {
        room.timers = room.timers.filter((timer) => timer !== scoreboardTimer);
        startETSection(pin);
      }, 6000);

      room.timers.push(scoreboardTimer);
      return;
    }

    room.et.currentQuestion = section.questions[room.et.currentQuestionIndex];
    room.et.currentAnswers = {};
    room.et.currentQuestionStartedAt = Date.now();

    io.to(pin).emit('et-question-start', {
      topic: section.topic,
      submittedBy: section.playerName,
      sectionNumber: room.et.currentSectionIndex + 1,
      totalSections: room.et.randomizedTopics.length,
      questionNumber: room.et.currentQuestionIndex + 1,
      totalQuestionsInSection: section.questions.length,
      question: room.et.currentQuestion.question,
      answers: room.et.currentQuestion.answers,
    });

    io.to(pin).emit('et-answer-count', { count: 0, total: room.players.length });

    const ANSWER_TIME = 12;
    let timeLeft = ANSWER_TIME;
    io.to(pin).emit('et-timer-tick', { timeLeft, totalTime: ANSWER_TIME });

    const timer = setInterval(() => {
      timeLeft--;
      io.to(pin).emit('et-timer-tick', { timeLeft, totalTime: ANSWER_TIME });
      if (timeLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter((entry) => entry !== timer);
        doETReveal(pin);
      }
    }, 1000);

    room.timers.push(timer);
  }

  function doETReveal(pin) {
    const room = rooms[pin];
    if (!room || !room.et || !room.et.currentQuestion) return;

    const section = room.et.randomizedTopics[room.et.currentSectionIndex];
    const answersByPlayer = room.players.map((player) => {
      const answerEntry = room.et.currentAnswers[player.id];
      return {
        playerName: player.name,
        answer: answerEntry ? answerEntry.answer : null,
        isCorrect: answerEntry ? answerEntry.answer === room.et.currentQuestion.correctAnswer : false,
      };
    });

    const correctEntries = Object.values(room.et.currentAnswers)
      .filter((entry) => entry.answer === room.et.currentQuestion.correctAnswer)
      .sort((a, b) => a.submittedAt - b.submittedAt);

    const dynamicScores = buildETScores(correctEntries.length);
    const pointsAwarded = {};
    room.players.forEach((player) => {
      pointsAwarded[player.name] = 0;
    });

    correctEntries.forEach((entry, index) => {
      const points = dynamicScores[index] || 0;
      pointsAwarded[entry.playerName] += points;
      room.et.scores[entry.playerName] += points;
      room.et.sectionScoreboard[entry.playerName] += points;

      const statLine = room.et.stats[entry.playerName];
      statLine.correctAnswers += 1;
      if (index === 0) statLine.firstPlaceFinishes += 1;

      const responseMs = entry.submittedAt - room.et.currentQuestionStartedAt;
      if (statLine.fastestCorrectMs === null || responseMs < statLine.fastestCorrectMs) {
        statLine.fastestCorrectMs = responseMs;
      }
      if (room.et.sectionScoreboard[entry.playerName] > statLine.bestTopicSection.score) {
        statLine.bestTopicSection = {
          topic: section.topic,
          score: room.et.sectionScoreboard[entry.playerName],
        };
      }
    });

    if (correctEntries.length === 1) {
      const soloWinner = correctEntries[0].playerName;
      const soloBonus = 8;
      pointsAwarded[soloWinner] += soloBonus;
      room.et.scores[soloWinner] += soloBonus;
      room.et.sectionScoreboard[soloWinner] += soloBonus;

      const bestTopic = room.et.stats[soloWinner].bestTopicSection;
      if (room.et.sectionScoreboard[soloWinner] > bestTopic.score) {
        room.et.stats[soloWinner].bestTopicSection = {
          topic: section.topic,
          score: room.et.sectionScoreboard[soloWinner],
        };
      }
    }

    io.to(pin).emit('et-reveal', {
      topic: section.topic,
      submittedBy: section.playerName,
      questionNumber: room.et.currentQuestionIndex + 1,
      totalQuestionsInSection: section.questions.length,
      question: room.et.currentQuestion.question,
      correctAnswer: room.et.currentQuestion.correctAnswer,
      answersByPlayer,
      pointsAwarded,
      totalScores: room.et.scores,
      soloBonusPlayer: correctEntries.length === 1 ? correctEntries[0].playerName : null,
      hasMoreQuestionsInSection: room.et.currentQuestionIndex < section.questions.length - 1,
    });

    const revealTimer = setTimeout(() => {
      room.timers = room.timers.filter((timer) => timer !== revealTimer);
      startETQuestion(pin);
    }, 6000);

    room.timers.push(revealTimer);
  }

  function finishETGame(pin) {
    const room = rooms[pin];
    if (!room || !room.et) return;

    const rankings = room.players
      .map((player) => ({
        name: player.name,
        score: room.et.scores[player.name] || 0,
      }))
      .sort((a, b) => b.score - a.score);

    const statsSource = room.players.map((player) => ({
      name: player.name,
      ...room.et.stats[player.name],
    }));

    const mostCorrect = [...statsSource].sort((a, b) => b.correctAnswers - a.correctAnswers)[0] || null;
    const mostFirsts = [...statsSource].sort((a, b) => b.firstPlaceFinishes - a.firstPlaceFinishes)[0] || null;
    const fastestCorrect = [...statsSource]
      .filter((entry) => entry.fastestCorrectMs !== null)
      .sort((a, b) => a.fastestCorrectMs - b.fastestCorrectMs)[0] || null;
    const bestTopicSection = [...statsSource]
      .filter((entry) => entry.bestTopicSection.topic)
      .sort((a, b) => b.bestTopicSection.score - a.bestTopicSection.score)[0] || null;

    io.to(pin).emit('et-game-over', {
      rankings,
      stats: {
        mostCorrect: mostCorrect ? { name: mostCorrect.name, value: mostCorrect.correctAnswers } : null,
        mostFirsts: mostFirsts ? { name: mostFirsts.name, value: mostFirsts.firstPlaceFinishes } : null,
        fastestCorrect: fastestCorrect
          ? { name: fastestCorrect.name, value: fastestCorrect.fastestCorrectMs }
          : null,
        bestTopicSection: bestTopicSection
          ? {
              name: bestTopicSection.name,
              topic: bestTopicSection.bestTopicSection.topic,
              value: bestTopicSection.bestTopicSection.score,
            }
          : null,
      },
    });

    room.gameState = 'lobby';
  }

  function getMafiaLivingPlayers(room) {
    return room.players.filter((player) => room.mafia.alive[player.id]);
  }

  function getMafiaVoteCounts(room, sourceVotes = room.mafia.votes) {
    return Object.values(sourceVotes).reduce((acc, targetId) => {
      acc[targetId] = (acc[targetId] || 0) + 1;
      return acc;
    }, {});
  }

  function assignMafiaRoles(room) {
    const counts = getMafiaRoleCounts(room.players.length, room.mafia.jokerEnabled);
    const shuffledPlayers = shuffle([...room.players]);
    const roles = {};

    shuffledPlayers.slice(0, counts.killers).forEach((player) => { roles[player.id] = 'killer'; });
    let index = counts.killers;
    roles[shuffledPlayers[index].id] = 'doctor';
    index += 1;
    if (counts.joker) {
      roles[shuffledPlayers[index].id] = 'joker';
      index += 1;
    }
    shuffledPlayers.slice(index).forEach((player) => { roles[player.id] = 'villager'; });

    room.mafia.roles = roles;
    room.mafia.alive = {};
    room.players.forEach((player) => { room.mafia.alive[player.id] = true; });
  }

  function buildMafiaStateForPlayer(room, playerId) {
    const mafia = room.mafia;
    const me = room.players.find((player) => player.id === playerId);
    const myRole = mafia.roles[playerId] || null;
    const isAlive = !!mafia.alive[playerId];
    const livingPlayers = getMafiaLivingPlayers(room);
    const aliveKillers = livingPlayers.filter((player) => mafia.roles[player.id] === 'killer').length;
    const publicVoteCounts = getMafiaVoteCounts(room);
    const killers = room.players.filter((player) => mafia.roles[player.id] === 'killer');
    const killerTeam = myRole === 'killer' ? killers.map((player) => ({ id: player.id, name: player.name, alive: !!mafia.alive[player.id] })) : [];
    const canShowNightKillerMarkers = myRole === 'killer' && mafia.phase === 'night' && !mafia.pendingNight.killerVotes[playerId];

    let actionOptions = [];
    let actionType = null;
    if (mafia.phase === 'night' && isAlive) {
      if (myRole === 'killer') {
        actionType = 'killer';
        actionOptions = livingPlayers
          .filter((player) => mafia.roles[player.id] !== 'killer')
          .map((player) => ({ id: player.id, name: player.name }));
      } else if (myRole === 'doctor') {
        actionType = 'doctor';
        actionOptions = livingPlayers
          .filter((player) => player.id !== mafia.doctorLastProtectedId)
          .map((player) => ({ id: player.id, name: player.name }));
      } else if (['villager', 'joker'].includes(myRole) && mafia.pendingPoll) {
        actionType = 'poll';
        actionOptions = mafia.pendingPoll.options.map((option) => ({ id: option, name: option }));
      }
    }

    const killerVoteView = myRole === 'killer'
      ? killers.map((killer) => {
          const targetId = mafia.pendingNight.killerVotes[killer.id];
          const target = room.players.find((player) => player.id === targetId);
          return { killerName: killer.name, targetName: target ? target.name : 'No vote yet' };
        })
      : [];

    let selectedNightChoice = null;
    if (mafia.phase === 'night' && isAlive) {
      if (myRole === 'killer') selectedNightChoice = mafia.pendingNight.killerVotes[playerId] || null;
      else if (myRole === 'doctor') selectedNightChoice = mafia.pendingNight.doctorTarget || null;
      else if (['villager', 'joker'].includes(myRole)) selectedNightChoice = mafia.pendingNight.pollVotes[playerId] || null;
    }

    return {
      started: mafia.started,
      playerId,
      isHost: playerId === room.hostSocketId,
      phase: mafia.phase,
      dayNumber: mafia.dayNumber,
      nightNumber: mafia.nightNumber,
      aliveKillers,
      abstainVoteCount: publicVoteCounts.__abstain__ || 0,
      majorityNeeded: Math.floor(getMafiaLivingPlayers(room).length / 2) + 1,
      jokerEnabled: mafia.jokerEnabled,
      hiddenPollsUntilEnd: mafia.hiddenPollsUntilEnd,
      roleCounts: getMafiaRoleCounts(room.players.length, mafia.jokerEnabled),
      minPlayers: 4,
      myRole,
      isAlive,
      players: room.players.map((player) => ({
        id: player.id,
        name: player.name,
        alive: !!mafia.alive[player.id],
        voteCount: publicVoteCounts[player.id] || 0,
        isKnownKiller: myRole === 'killer' && mafia.roles[player.id] === 'killer',
      })),
      eventMessage: mafia.eventMessage,
      eventTone: mafia.eventTone || 'neutral',
      actionType,
      actionOptions,
      selectedNightChoice,
      currentVote: mafia.votes[playerId] || null,
      killerVoteView,
      revoteCandidates: mafia.revoteCandidates,
      timer: {
        discussion: mafia.discussionTimeLeft,
        voting: mafia.voteTimeLeft,
        night: mafia.nightTimeLeft || 0,
      },
      lastPollResult: mafia.lastPollResult,
      pendingPoll: mafia.pendingPoll ? { question: mafia.pendingPoll.question, options: mafia.pendingPoll.options } : null,
      canHostAdvanceDiscussion: playerId === room.hostSocketId && mafia.phase === 'discussion',
      canHostStartGame: playerId === room.hostSocketId && mafia.phase === 'setup' && room.players.length >= 4,
      canHostStartNight: playerId === room.hostSocketId && mafia.phase === 'day-result',
      canHostStartDiscussion: false,
      winner: mafia.winner,
      killerTeam,
      canShowNightKillerMarkers,
      finalPolarizedPoll: mafia.phase === 'game-over' ? mafia.finalPolarizedPoll : null,
      revealedRoles: mafia.phase === 'game-over'
        ? room.players.map((player) => ({ name: player.name, role: mafia.roles[player.id] }))
        : [],
    };
  }

  function emitMafiaState(pin) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;
    room.players.forEach((player) => {
      io.to(player.id).emit('mafia-state', buildMafiaStateForPlayer(room, player.id));
    });
  }

  function startMafiaDiscussion(pin, options = {}) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;
    clearRoomTimers(room);
    if (options.incrementDay) {
      room.mafia.dayNumber += 1;
    }
    room.mafia.phase = 'discussion';
    room.mafia.votes = {};
    room.mafia.isRevote = false;
    room.mafia.revoteCandidates = [];
    room.mafia.discussionTimeLeft = 180;
    if (!options.keepEventMessage) {
      room.mafia.eventMessage = room.mafia.lastPollResult
        ? `Day ${room.mafia.dayNumber}. Review the night poll and discuss.`
        : `Day ${room.mafia.dayNumber}. Discuss before voting.`;
      room.mafia.eventTone = 'neutral';
    }
    emitMafiaState(pin);

    const timer = setInterval(() => {
      room.mafia.discussionTimeLeft = Math.max(0, room.mafia.discussionTimeLeft - 1);
      emitMafiaState(pin);
      if (room.mafia.discussionTimeLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter((entry) => entry !== timer);
        startMafiaVoting(pin, false, []);
      }
    }, 1000);
    room.timers.push(timer);
  }

  function startMafiaWinnerSplash(pin, winner) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;
    clearRoomTimers(room);
    room.mafia.winner = winner;
    room.mafia.phase = 'winner-splash';
    room.mafia.eventMessage = winner.text;
    room.mafia.eventTone = winner.team === 'town' ? 'good' : winner.team === 'joker' ? 'joker' : 'bad';
    emitMafiaState(pin);

    const timer = setTimeout(() => {
      room.timers = room.timers.filter((entry) => entry !== timer);
      if (!room.mafia) return;
      endMafiaGame(pin);
    }, 5000);
    room.timers.push(timer);
  }

  function startMafiaVoting(pin, isRevote, candidates) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;
    clearRoomTimers(room);
    room.mafia.phase = isRevote ? 'revote' : 'voting';
    room.mafia.isRevote = isRevote;
    room.mafia.revoteCandidates = candidates;
    room.mafia.votes = {};
    room.mafia.voteTimeLeft = isRevote ? 20 : 30;
    room.mafia.eventMessage = isRevote ? 'Revote among the tied players.' : 'Vote to eliminate a suspect.';
    room.mafia.eventTone = 'neutral';
    emitMafiaState(pin);

    const timer = setInterval(() => {
      room.mafia.voteTimeLeft = Math.max(0, room.mafia.voteTimeLeft - 1);
      emitMafiaState(pin);
      if (room.mafia.voteTimeLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter((entry) => entry !== timer);
        resolveMafiaVote(pin, false);
      }
    }, 1000);
    room.timers.push(timer);
  }

  function queueMafiaNightStart(pin, delayMs = 6000) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;

    const timer = setTimeout(() => {
      room.timers = room.timers.filter((entry) => entry !== timer);
      if (!room.mafia || room.mafia.phase !== 'day-result') return;
      startMafiaNight(pin);
    }, delayMs);

    room.timers.push(timer);
  }

  function resolveMafiaVote(pin, majorityReached) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;

    const voteCounts = getMafiaVoteCounts(room);
    const entries = Object.entries(voteCounts).sort((a, b) => b[1] - a[1]);
    const livingCount = getMafiaLivingPlayers(room).length;
    const majorityNeeded = Math.floor(livingCount / 2) + 1;
    let targetId = null;

    if (majorityReached) {
      const winner = entries.find(([, count]) => count > livingCount / 2);
      targetId = winner ? winner[0] : null;
      if (targetId === '__abstain__') {
        room.mafia.phase = 'day-result';
        room.mafia.eventMessage = 'The town chose not to eliminate anyone.';
        room.mafia.eventTone = 'good';
        emitMafiaState(pin);
        queueMafiaNightStart(pin);
        return;
      }
    } else if (!entries.length) {
      room.mafia.phase = 'day-result';
      room.mafia.eventMessage = 'No votes were cast. Nobody was eliminated.';
      room.mafia.eventTone = 'good';
      emitMafiaState(pin);
      queueMafiaNightStart(pin);
      return;
    } else {
      const topCount = entries[0][1];
      if (topCount < majorityNeeded) {
        room.mafia.phase = 'day-result';
        room.mafia.eventMessage = 'No majority was reached.\nNo elimination.';
        room.mafia.eventTone = 'good';
        emitMafiaState(pin);
        queueMafiaNightStart(pin);
        return;
      }
      const topEntries = entries.filter(([, count]) => count === topCount);
      if (topEntries.some(([id]) => id === '__abstain__')) {
        room.mafia.phase = 'day-result';
        room.mafia.eventMessage = 'No elimination. Abstain was tied for the lead.';
        room.mafia.eventTone = 'good';
        emitMafiaState(pin);
        queueMafiaNightStart(pin);
        return;
      }
      const tiedIds = topEntries.map(([id]) => id);
      if (tiedIds.length > 1) {
        if (room.mafia.isRevote) {
          room.mafia.phase = 'day-result';
          room.mafia.eventMessage = 'The revote tied. Nobody was eliminated.';
          room.mafia.eventTone = 'good';
          emitMafiaState(pin);
          queueMafiaNightStart(pin);
          return;
        }
        startMafiaVoting(pin, true, tiedIds);
        return;
      }
      targetId = entries[0][0];
    }

    if (!targetId) return;
    room.mafia.alive[targetId] = false;
    const targetPlayer = room.players.find((player) => player.id === targetId);
    const eliminatedWasKiller = room.mafia.roles[targetId] === 'killer';
    room.mafia.deathLog.push({ type: 'vote', playerName: targetPlayer.name, role: room.mafia.roles[targetId], day: room.mafia.dayNumber });

    if (room.mafia.roles[targetId] === 'joker') {
      startMafiaWinnerSplash(pin, { team: 'joker', text: `${targetPlayer.name} wins as the Joker!` });
      return;
    }

    const win = getMafiaWin(room);
    if (win) {
      room.mafia.phase = 'day-result';
      room.mafia.eventMessage = `${targetPlayer.name} was voted out.\n${eliminatedWasKiller ? 'They were a killer.' : 'They were not a killer.'}`;
      room.mafia.eventTone = eliminatedWasKiller ? 'good' : 'bad';
      emitMafiaState(pin);

      const timer = setTimeout(() => {
        room.timers = room.timers.filter((entry) => entry !== timer);
        if (!room.mafia) return;
        startMafiaWinnerSplash(pin, win);
      }, 3500);
      room.timers.push(timer);
      return;
    }

    room.mafia.phase = 'day-result';
    room.mafia.eventMessage = `${targetPlayer.name} was voted out.\n${eliminatedWasKiller ? 'They were a killer.' : 'They were not a killer.'}`;
    room.mafia.eventTone = eliminatedWasKiller ? 'good' : 'bad';
    emitMafiaState(pin);
    queueMafiaNightStart(pin);
  }

  function startMafiaNight(pin) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;
    clearRoomTimers(room);
    room.mafia.phase = 'night';
    room.mafia.nightNumber += 1;
    room.mafia.nightTimeLeft = 25;
    room.mafia.pendingPoll = pickRandom(MAFIA_NIGHT_POLLS);
    room.mafia.pendingNight = { killerVotes: {}, doctorTarget: null, pollVotes: {} };
    room.mafia.eventMessage = `Night ${room.mafia.nightNumber}. Make your move.`;
    room.mafia.eventTone = 'neutral';
    emitMafiaState(pin);

    const timer = setInterval(() => {
      room.mafia.nightTimeLeft = Math.max(0, room.mafia.nightTimeLeft - 1);
      emitMafiaState(pin);
      if (room.mafia.nightTimeLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter((entry) => entry !== timer);
        resolveMafiaNight(pin);
      }
    }, 1000);
    room.timers.push(timer);
  }

  function resolveMafiaNight(pin) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;

    const killCounts = getMafiaVoteCounts(room, room.mafia.pendingNight.killerVotes);
    const killEntries = Object.entries(killCounts).sort((a, b) => b[1] - a[1]);
    let targetId = null;
    if (killEntries.length) {
      const topCount = killEntries[0][1];
      const tiedTop = killEntries.filter(([, count]) => count === topCount).map(([id]) => id);
      targetId = pickRandom(tiedTop);
    }

    const savedId = room.mafia.pendingNight.doctorTarget;
    room.mafia.doctorLastProtectedId = savedId || null;

    let killedPlayer = null;
    if (targetId && targetId !== savedId && room.mafia.alive[targetId]) {
      room.mafia.alive[targetId] = false;
      killedPlayer = room.players.find((player) => player.id === targetId);
      room.mafia.deathLog.push({ type: 'night', playerName: killedPlayer.name, role: room.mafia.roles[targetId], night: room.mafia.nightNumber });
    }

    const pollResult = {
      question: room.mafia.pendingPoll.question,
      counts: room.mafia.pendingPoll.options.map((option) => ({
        option,
        count: Object.values(room.mafia.pendingNight.pollVotes).filter((vote) => vote === option).length,
      })),
      totalVotes: Object.keys(room.mafia.pendingNight.pollVotes).length,
      nightNumber: room.mafia.nightNumber,
    };
    room.mafia.pollHistory.push(pollResult);
    room.mafia.lastPollResult = room.mafia.hiddenPollsUntilEnd ? null : pollResult;

    const win = getMafiaWin(room);
    if (win) {
      startMafiaWinnerSplash(pin, win);
      return;
    }

    room.mafia.eventMessage = killedPlayer
      ? `${killedPlayer.name} was killed overnight.\n${room.mafia.roles[targetId] === 'killer' ? 'They were a killer.' : 'They were not a killer.'}`
      : 'Nobody died last night.';
    room.mafia.eventTone = killedPlayer ? 'bad' : 'good';
    startMafiaDiscussion(pin, { incrementDay: true, keepEventMessage: true });
  }

  function getMafiaWin(room) {
    const alivePlayers = getMafiaLivingPlayers(room);
    const aliveKillers = alivePlayers.filter((player) => room.mafia.roles[player.id] === 'killer').length;
    const aliveNonKillers = alivePlayers.length - aliveKillers;

    if (aliveKillers === 0) {
      return { team: 'town', text: 'Town wins!' };
    }
    if (aliveKillers >= aliveNonKillers) {
      return { team: 'killers', text: 'Killers win!' };
    }
    return null;
  }

  function endMafiaGame(pin) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;
    clearRoomTimers(room);
    room.mafia.phase = 'game-over';
    room.mafia.eventMessage = room.mafia.winner ? room.mafia.winner.text : 'Game over.';
    room.mafia.eventTone = room.mafia.winner?.team === 'town' ? 'good' : room.mafia.winner?.team === 'joker' ? 'joker' : 'bad';
    room.mafia.finalPolarizedPoll = room.mafia.hiddenPollsUntilEnd ? getMafiaMostPolarizedPoll(room) : null;
    emitMafiaState(pin);
  }

  socket.on('disconnect', () => {
    const pin = socket.data.pin;
    if (!pin || !rooms[pin]) return;

    const room = rooms[pin];

    // Host is now a player too — remove from players list
    room.players = room.players.filter((p) => p.id !== socket.id);

    if (socket.data.isHost) {
      if (room.players.length > 0) {
        const newHost = room.players[0];
        room.hostSocketId = newHost.id;
        const newHostSocket = io.sockets.sockets.get(newHost.id);
        if (newHostSocket) newHostSocket.data.isHost = true;
        io.to(newHost.id).emit('you-are-now-host');
        io.to(pin).emit('host-changed', { newHostId: newHost.id });
      } else {
        clearRoomTimers(room);
        delete rooms[pin];
        return;
      }
    }

    // Send updated list after hostId is finalized
    io.to(pin).emit('player-list-updated', { players: room.players, hostId: room.hostSocketId });
  });
});

const PORT = Number(process.env.PORT) || 3000;
server.listen(PORT, '0.0.0.0', () => {
  const ip = getLocalIP();
  console.log('\n================================');
  console.log('   Johnbox Games is running!');
  console.log('================================');
  console.log(`   Everyone joins → http://${ip}:${PORT}`);
  console.log('================================\n');
});
