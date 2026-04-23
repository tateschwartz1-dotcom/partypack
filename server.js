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

function normalizeBasePath(value = '') {
  const trimmed = String(value || '').trim();
  if (!trimmed || trimmed === '/') return '';
  const withLeadingSlash = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
  return withLeadingSlash.replace(/\/+$/, '');
}

function escapeRegExp(value = '') {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function joinBasePath(basePath, route = '/') {
  const normalizedRoute = route.startsWith('/') ? route : `/${route}`;
  return `${basePath}${normalizedRoute}` || '/';
}

function normalizePublicUrl(value = '') {
  return String(value || '').trim().replace(/\/+$/, '');
}

function sendHtmlWithBasePath(res, fileName, basePath) {
  const htmlPath = path.join(__dirname, 'public', fileName);
  const html = fs
    .readFileSync(htmlPath, 'utf8')
    .replace(/__BASE_PATH__/g, basePath);
  res.type('html').send(html);
}

const BASE_PATH = '';
const LEGACY_BASE_PATHS = ['/johnbox'].map(normalizeBasePath).filter(Boolean);
const SOCKET_PATH = joinBasePath(BASE_PATH, '/socket.io');
const PUBLIC_URL = normalizePublicUrl(process.env.PUBLIC_URL || '');
const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e6, path: SOCKET_PATH });
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const ANTHROPIC_TRIVIA_MODEL = process.env.ANTHROPIC_TRIVIA_MODEL || 'claude-haiku-4-5-20251001';
const ANTHROPIC_COACH_MODEL = process.env.ANTHROPIC_COACH_MODEL || ANTHROPIC_TRIVIA_MODEL;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
const OPENAI_TRIVIA_MODEL = process.env.OPENAI_TRIVIA_MODEL || 'gpt-5-mini';
const TRIVIA_API_KEY = process.env.TRIVIA_API_KEY || '';
const DEBATE_MIN_PLAYERS = 3;
const REJOIN_GRACE_MS = 90000;

LEGACY_BASE_PATHS.forEach((legacyBasePath) => {
  app.get(new RegExp(`^${escapeRegExp(legacyBasePath)}(?:/.*)?$`), (req, res) => {
    const redirectPath = req.path.slice(legacyBasePath.length) || '/';
    const queryIndex = req.originalUrl.indexOf('?');
    const query = queryIndex >= 0 ? req.originalUrl.slice(queryIndex) : '';
    res.redirect(301, `${redirectPath}${query}`);
  });
});

app.use('/', express.static('public'));

app.get(joinBasePath(BASE_PATH, '/'), (req, res) => {
  sendHtmlWithBasePath(res, 'player.html', BASE_PATH);
});

app.get(joinBasePath(BASE_PATH, '/host'), (req, res) => {
  res.redirect(joinBasePath(BASE_PATH, '/'));
});

app.get(joinBasePath(BASE_PATH, '/display'), (req, res) => {
  sendHtmlWithBasePath(res, 'display.html', BASE_PATH);
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
  '#f87171', '#fb923c', '#d97706', '#4ade80',
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

// ── Debate Topics ────────────────────────────────────────────────

const DEBATE_TOPICS_LIGHT = [
  "Pool vs Beach",
  "Soda vs Juice",
  "Rain vs Snow",
  "Cats vs Dogs",
  "Pancakes vs Waffles",
  "Pizza vs Tacos",
  "Movies vs TV Shows",
  "Paper Books vs Audiobooks",
  "Morning People vs Night Owls",
  "Texting vs Calling",
  "Board Games vs Video Games",
  "Mountains vs Ocean",
  "City Life vs Country Life",
  "Stairs vs Elevator",
  "Group Projects vs Solo Projects",
  "Plan the Trip vs Wing the Trip",
  "Breakfast vs Dinner",
  "Road Trips vs Flights",
  "Concerts vs Sporting Events",
  "Theme Parks vs Water Parks",
  "Camping vs Hotels",
  "Cooking at Home vs Eating Out",
  "Jeans vs Sweatpants",
  "Early Arrival vs Fashionably Late",
  "Big Parties vs Small Hangouts",
  "Dressing Up vs Dressing Comfortable",
  "Shopping Online vs Shopping In Person",
  "Desk Job vs Outdoor Job",
  "Halloween vs Valentine's Day",
];

const DEBATE_TOPICS_HEAVY = [
  "Nature vs Nurture",
  "Forgiveness vs Accountability",
  "Public Funding for the Arts vs Public Funding for Sports",
  "Voting Should Be Mandatory vs Voting Should Be Optional",
  "Social Media Does More Good vs More Harm",
  "Prioritizing Mental Health vs Prioritizing Productivity",
  "Intentions Matter More vs Outcomes Matter More",
  "Short-Term Sacrifice vs Long-Term Happiness",
  "Competition Makes People Better vs Cooperation Makes People Better",
  "Raise Kids Strict vs Raise Kids Free",
  "Standardized Testing Helps vs Standardized Testing Hurts",
  "Technology Connects Us vs Technology Isolates Us",
];

const DEBATE_TOPICS_OSMIUM = [
  "It Is Better to Have Loved and Lost vs Better to Have Never Loved",
  "Save One Person You Love vs Save Five Strangers",
  "Bringing a Child Into This World Is Ethical vs Unethical",
  "Justice Requires Punishment vs Justice Requires Rehabilitation",
  "Humans Are Fundamentally Good vs Humans Are Fundamentally Self-Interested",
  "Erase a Painful Memory vs Keep Every Memory You Have",
  "Know the Exact Day You Die vs Never Know",
  "Love Is a Choice vs Love Is a Feeling",
  "Free Will Exists vs Free Will Is an Illusion",
  "Humanity Is Improving vs Humanity Is Repeating Itself",
  "Love Requires Sacrifice vs Love Should Not Require Sacrifice",
  "Parents Owe Children Everything vs Children Owe Parents Everything",
  "Choose Your Family vs Honor the Family You Got",
  "Better to Be Feared vs Better to Be Loved",
];

const DEBATE_BANNED_WORD_GROUPS = [
  ['the'],
  ['it'],
  ['and'],
  ['but'],
  ['um'],
  ['because', 'why', 'reason'],
  ['like'],
  ['you', 'your'],
  ['so'],
  ['really'],
  ['is', 'are'],
  ['be'],
  ['that', 'this'],
  ['just'],
  ['I', 'me'],
  ['my', 'mine'],
  ['okay', 'yeah', 'yes'],
  ['no', 'not'],
  ['well'],
  ['what'],
  ['who'],
  ['when'],
  ['how'],
  ['if'],
  ['then', 'now'],
  ['here', 'there'],
  ['thing', 'stuff'],
  ['good', 'best', 'better'],
  ['bad', 'worst', 'worse'],
  ['big', 'more', 'most'],
  ['small', 'less', 'least'],
  ['always', 'never'],
  ['everyone', 'someone', 'people'],
  ['anything', 'all', 'any'],
  ['right', 'true', 'real'],
  ['wrong', 'false'],
  ['point', 'argument', 'opinion'],
  ['question', 'answer'],
  ['either', 'neither'],
  ['both', 'each'],
  ['should', 'could', 'would'],
  ['might', 'must', 'can'],
  ['will', 'cannot'],
  ['issue', 'problem'],
  ['get', 'got'],
  ['make', 'made'],
  ['take', 'took'],
  ['go', 'went'],
  ['know', 'knew'],
  ['think', 'thought'],
  ['feel', 'felt', 'seem'],
  ['say', 'said'],
  ['use', 'used'],
  ['want', 'need'],
  ['try', 'tried'],
  ['keep'],
  ['find', 'found'],
  ['put', 'let'],
  ['happen', 'become'],
  ['we', 'us', 'our'],
  ['their', 'they', 'them'],
  ['one'],
  ['bro', 'dude'],
  ['crazy', 'sus'],
  ['a'],
  ['to', 'too', 'two'],
  ['of'],
  ['in'],
  ['for'],
  ['with'],
  ['on'],
  ['at'],
  ['by'],
  ['from'],
  ['as'],
  ['or'],
  ['have'],
  ['do'],
  ['Words that start with the letter T'],
  ['Words that start with the letter S'],
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

function pickDebateBannedEntries(count = 2) {
  const pool = [...DEBATE_BANNED_WORD_GROUPS];
  const selected = [];
  while (selected.length < count && pool.length) {
    const index = Math.floor(Math.random() * pool.length);
    const [group] = pool.splice(index, 1);
    selected.push(group.join(' / '));
  }
  return selected;
}

function randInt(min, max) {
  return Math.floor(min + Math.random() * (max - min + 1));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function formatMoney(value) {
  return `$${Math.max(0, value || 0)}`;
}

function getAuctionPlayer(room, playerId) {
  return room.players.find((player) => player.id === playerId) || null;
}

function getAuctionPlayerName(room, playerId) {
  return getAuctionPlayer(room, playerId)?.name || 'Someone';
}

function getAuctionScoreboard(room) {
  const auction = room.auction;
  return room.players.map((player) => ({
    id: player.id,
    name: player.name,
    money: auction.money[player.id] || 0,
    score: auction.scores[player.id] || 0,
    modifiers: auction.modifiers[player.id] || [],
  }));
}

function getAuctionVoteSummary(votes = {}) {
  const values = Object.values(votes);
  return {
    yes: values.filter((vote) => vote === 'yes').length,
    no: values.filter((vote) => vote === 'no').length,
    real: values.filter((vote) => vote === 'real').length,
    madeup: values.filter((vote) => vote === 'madeup').length,
    total: values.length,
  };
}

function getAuctionItemIcon(item) {
  if (!item) return '❔';
  if (item.kind === 'points') return '⭐';
  if (item.kind === 'power') return '⚡';
  if (item.kind === 'minigame') return '🎲';
  if (item.kind === 'mystery') return '❔';
  return '🔨';
}

function makeAuctionPointItem() {
  const tiers = [
    { key: 'small', min: 2, max: 4 },
    { key: 'medium', min: 4, max: 6 },
    { key: 'large', min: 6, max: 9 },
    { key: 'premium', min: 9, max: 11 },
  ];
  const tier = pickRandom(tiers);
  const points = randInt(tier.min, tier.max);
  return {
    kind: 'points',
    name: 'Points',
    points,
    summary: `Gain ${points} point${points === 1 ? '' : 's'}.`,
  };
}

function normalizeAuctionSettings(auctionSettings = {}, playerCount = 0) {
  const fallback = Math.max(1, playerCount * AUCTION_LISTINGS_PER_PLAYER);
  const rawTotal = Number.parseInt(auctionSettings.totalListings, 10);
  return {
    totalListings: Math.max(1, Math.min(40, Number.isFinite(rawTotal) ? rawTotal : fallback)),
  };
}

function makeAuctionPowerItem(players, lotsAfterCurrent = 0) {
  let powers = [
    { type: 'steal', name: 'Power', targetCount: 1, amount: randInt(2, 5) },
    { type: 'lose', name: 'Power', targetCount: 1, amount: randInt(2, 5) },
    { type: 'swap', name: 'Power', targetCount: 1 },
    { type: 'drink', name: 'Power', targetCount: 1 },
    { type: 'advantage', name: 'Power', targetCount: 0, count: 1 },
    { type: 'disadvantage', name: 'Power', targetCount: 1, count: 1 },
    { type: 'advantage2', name: 'Power', targetCount: 0, count: 2 },
    { type: 'disadvantage2', name: 'Power', targetCount: Math.min(2, Math.max(1, players.length - 1)), count: 1 },
  ];
  powers = powers.filter((power) => {
    if (['advantage', 'disadvantage'].includes(power.type)) return lotsAfterCurrent >= 1;
    if (['advantage2', 'disadvantage2'].includes(power.type)) return lotsAfterCurrent >= 2;
    return true;
  });
  const power = pickRandom(powers);
  let summary = '';
  if (power.type === 'steal') summary = `Steal ${power.amount} point${power.amount === 1 ? '' : 's'} from a player.`;
  if (power.type === 'lose') summary = `Make a player lose ${power.amount} point${power.amount === 1 ? '' : 's'}.`;
  if (power.type === 'swap') summary = 'Swap point totals with a player.';
  if (power.type === 'drink') summary = 'Make a player get you a drink.';
  if (power.type === 'advantage') summary = 'Get an advantage in your next minigame.';
  if (power.type === 'disadvantage') summary = 'Give a player a disadvantage in their next minigame.';
  if (power.type === 'advantage2') summary = 'Get advantages in your next two minigames.';
  if (power.type === 'disadvantage2') summary = 'Give two players a disadvantage in their next minigames.';
  let targetPrompt = '';
  if (power.type === 'steal') targetPrompt = `Choose who to steal ${power.amount} point${power.amount === 1 ? '' : 's'} from.`;
  if (power.type === 'lose') targetPrompt = `Choose who to lose ${power.amount} point${power.amount === 1 ? '' : 's'}.`;
  if (power.type === 'swap') targetPrompt = 'Choose who to swap point totals with.';
  if (power.type === 'drink') targetPrompt = 'Choose who gets you a drink.';
  if (power.type === 'disadvantage') targetPrompt = 'Choose who gets a disadvantage.';
  if (power.type === 'disadvantage2') targetPrompt = 'Choose two players to get disadvantages.';
  return { kind: 'power', ...power, summary, targetPrompt };
}

function makeAuctionMinigameItem(modifier = null, forcedType = null) {
  const gameType = forcedType || pickRandom(['toss', 'stack', 'find', 'flip', 'question', 'lie']);
  const item = { kind: 'minigame', gameType, modifier, points: randInt(6, 12), name: '', summary: '', prompt: '', timerSeconds: 0 };
  const isAdv = modifier === 'advantage';
  const isDis = modifier === 'disadvantage';

  if (gameType === 'toss') {
    const strides = isAdv ? 1 : isDis ? 3 : randInt(1, 3);
    item.points = strides === 1 ? randInt(5, 7) : strides === 2 ? randInt(7, 10) : randInt(10, 14);
    item.name = 'Toss';
    item.summary = 'Get ready to test your accuracy.';
    item.prompt = `Place a phone-sized target on the floor. Take ${strides} large stride${strides === 1 ? '' : 's'} away. Toss a small object once. If it lands and rests touching the target, you win.`;
  } else if (gameType === 'stack') {
    const objectCount = isAdv ? 3 : isDis ? 5 : randInt(3, 5);
    const seconds = isAdv ? 17 : isDis ? 9 : pickRandom([10, 11, 12, 13, 14, 15]);
    const letters = [pickRandom(AUCTION_COMMON_LETTERS)];
    const randomCount = isDis ? 2 : (Math.random() < 0.35 ? 2 : 1);
    while (letters.length < randomCount + 1) {
      const letter = pickRandom(AUCTION_RANDOM_LETTERS);
      if (!letters.includes(letter)) letters.push(letter);
    }
    item.points = clamp(objectCount + Math.round((16 - seconds) / 2) + letters.length + 3, 6, 15);
    item.name = 'Stack';
    item.summary = `Stack ${objectCount} objects. The objects must all start with a letter on the letter list. The list will be revealed when the timer starts.`;
    item.prompt = `You have ${seconds} seconds to stack ${objectCount} objects. The stack must stand by itself for 3 seconds. Every object must start with one of these letters: ${letters.join(', ')}.`;
    item.letters = letters;
    item.timerSeconds = seconds;
  } else if (gameType === 'find') {
    const objectCount = isAdv ? 3 : isDis ? 5 : randInt(3, 5);
    const seconds = isAdv ? 17 : isDis ? 9 : pickRandom([10, 11, 12, 13, 14, 15]);
    let category = pickRandom(AUCTION_FIND_CATEGORIES);
    if (category.includes('{letter}')) category = category.replace('{letter}', pickRandom(AUCTION_RANDOM_LETTERS));
    item.points = clamp(objectCount + Math.round((16 - seconds) / 2) + 4, 6, 15);
    item.name = 'Find';
    item.summary = `Touch ${objectCount} objects and say what they are out loud. The objects must fit in the category revealed when the timer starts.`;
    item.prompt = `You have ${seconds} seconds to touch ${objectCount} objects that fit this category: ${category}. Say each object out loud when you touch it.`;
    item.timerSeconds = seconds;
  } else if (gameType === 'flip') {
    const attempts = isAdv ? 3 : isDis ? 1 : randInt(1, 3);
    item.points = attempts === 1 ? randInt(8, 12) : attempts === 2 ? randInt(6, 10) : randInt(5, 8);
    item.name = 'Flip';
    item.summary = 'Get ready to test your flippage.';
    item.prompt = `You get ${attempts} chance${attempts === 1 ? '' : 's'} to do a bottle flip. If you do not have a bottle, the group may choose a different object for you to flip.`;
  } else if (gameType === 'question') {
    const question = pickRandom(isAdv ? AUCTION_ADVANTAGE_QUESTIONS : isDis ? AUCTION_DISADVANTAGE_QUESTIONS : AUCTION_NORMAL_QUESTIONS);
    item.points = isDis ? randInt(10, 15) : isAdv ? randInt(5, 8) : randInt(7, 12);
    item.name = 'Question';
    item.summary = 'Answer a question. The group votes if you answered it well.';
    item.prompt = question;
  } else if (gameType === 'lie') {
    const words = shuffle(AUCTION_LIE_WORDS).slice(0, 3);
    const task = Math.random() < 0.5 ? 'real' : 'madeup';
    item.points = isDis ? randInt(10, 15) : randInt(8, 13);
    item.name = 'Lie?';
    item.summary = isDis
      ? 'You will be told to either say two words on the list or say two words you made up. The group tries to guess which prompt you had.'
      : 'You will be told to either say a word on the list or say a word you made up. The group tries to guess which prompt you had.';
    item.prompt = 'Follow the prompt. Voters guess whether the spoken word or words are real or made up.';
    item.lie = { words, task, requiredWords: isDis ? 2 : 1, disadvantage: isDis };
  }

  if (modifier === 'advantage') item.summary = `Advantage: ${item.summary}`;
  if (modifier === 'disadvantage') item.summary = `Disadvantage: ${item.summary}`;
  return item;
}

function makeAuctionBaseItem(room, allowMinigame = true, forcedKind = null, lotsAfterCurrent = 0) {
  const roll = Math.random();
  const kind = forcedKind || (roll < 0.4 ? 'points' : roll < 0.7 ? 'minigame' : roll < 0.9 ? 'power' : pickRandom(['points', 'power', 'minigame']));
  if (kind === 'minigame' && allowMinigame) return makeAuctionMinigameItem();
  if (kind === 'power') return makeAuctionPowerItem(room.players, lotsAfterCurrent);
  return makeAuctionPointItem();
}

function makeAuctionListing(room) {
  const lotsAfterCurrent = Math.max(0, (room.auction?.totalListings || 0) - (room.auction?.listingNumber || 0));
  const specialHeavy = Math.random() < 0.1;
  const isDouble = Math.random() < (specialHeavy ? 0.5 : 0.18);
  const isMystery = Math.random() < (specialHeavy ? 0.45 : 0.2);
  const items = [];
  items.push(makeAuctionBaseItem(room, true, null, lotsAfterCurrent));
  if (isDouble) {
    const allowMinigame = !items.some((item) => item.kind === 'minigame');
    items.push(makeAuctionBaseItem(room, allowMinigame, null, lotsAfterCurrent));
  }

  const mysteryMask = items.map(() => false);
  if (isMystery) {
    if (isDouble) {
      const mode = pickRandom(['one', 'all']);
      if (mode === 'all') mysteryMask.fill(true);
      else mysteryMask[randInt(0, items.length - 1)] = true;
    } else {
      mysteryMask[0] = true;
    }
  }

  const visibleNames = items.map((item, index) => mysteryMask[index] ? 'Mystery Item' : item.name);
  return {
    id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    isDouble,
    isMystery: mysteryMask.some(Boolean),
    mysteryMask,
    items,
    name: isDouble ? `Double Lot: ${visibleNames.join(' + ')}` : visibleNames[0],
  };
}

function publicAuctionListing(listing, reveal = false) {
  if (!listing) return null;
  const pointLabel = (item) => `${item.points} point${item.points === 1 ? '' : 's'}`;
  const publicName = (item) => item.kind === 'minigame' ? `${item.name} Minigame` : item.kind === 'points' ? pointLabel(item) : item.kind === 'power' ? 'Power' : item.name;
  const publicSummary = (item) => item.kind === 'minigame' ? `Worth ${pointLabel(item)}.` : item.kind === 'points' ? '' : item.summary;
  const visibleName = (item, index) => listing.mysteryMask[index] ? 'Mystery Item' : publicName(item);
  const listingName = reveal
    ? (listing.isDouble ? `Double Lot: ${listing.items.map((item) => publicName(item)).join(' + ')}` : publicName(listing.items[0]))
    : (listing.isDouble ? `Double Lot: ${listing.items.map(visibleName).join(' + ')}` : visibleName(listing.items[0], 0));
  return {
    id: listing.id,
    name: listingName,
    isDouble: listing.isDouble,
    isMystery: listing.isMystery,
    items: listing.items.map((item, index) => {
      const hidden = !reveal && listing.mysteryMask[index];
      return hidden
        ? { kind: 'mystery', icon: '❔', name: 'Mystery Item', summary: 'Revealed after purchase.' }
        : {
            kind: item.kind,
            gameType: item.gameType,
            icon: getAuctionItemIcon(item),
            name: publicName(item),
            points: item.points,
            summary: publicSummary(item),
            rulesSummary: item.summary,
            prompt: reveal ? item.prompt : null,
            timerSeconds: reveal ? item.timerSeconds : 0,
            powerType: item.type,
            targetCount: item.targetCount || 0,
            targetPrompt: item.targetPrompt || '',
            letters: reveal ? item.letters || [] : [],
          };
    }),
  };
}

function emitAuctionState(pin) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  const auction = room.auction;
  const revealListing = ['sold', 'targeting', 'challenge-ready', 'challenge-active', 'voting', 'result'].includes(auction.phase);
  const base = {
    phase: auction.phase,
    listingNumber: auction.listingNumber,
    totalListings: auction.totalListings,
    listing: publicAuctionListing(auction.currentListing, revealListing),
    currentItemIndex: auction.currentItemIndex || 0,
    currentItem: auction.currentItem ? publicAuctionListing({ ...auction.currentListing, items: [auction.currentItem], mysteryMask: [false], isDouble: false, isMystery: false }, true)?.items[0] : null,
    highBidderId: auction.highBidderId,
    highBidderName: getAuctionPlayerName(room, auction.highBidderId),
    currentBid: auction.currentBid,
    timerLeft: auction.timerLeft,
    message: auction.message || '',
    buyerId: auction.buyerId,
    buyerName: getAuctionPlayerName(room, auction.buyerId),
    soldPrice: auction.soldPrice || 0,
    scoreboard: getAuctionScoreboard(room),
    votes: getAuctionVoteSummary(auction.votes),
    voteCount: Object.keys(auction.votes || {}).length,
    targetCount: auction.targetCount || 0,
    selectedTargets: auction.selectedTargets || [],
    winners: auction.winners || [],
    noSale: auction.noSale || false,
    tutorial: auction.phase === 'tutorial' ? {
      title: "Bidder's Auction",
      steps: [
        `Everyone starts with ${formatMoney(AUCTION_STARTING_MONEY)}.`,
        'Bid on each listing with +$1, +$3 or +$5.',
        'Bidding ends when nobody bids for 6 seconds.',
        'Listings can give points, powers or minigames.',
        'After all listings, the most points wins.',
      ],
    } : null,
  };
  room.players.forEach((player) => {
    const secret = auction.currentItem?.kind === 'minigame' && auction.currentItem?.gameType === 'lie' && auction.buyerId === player.id && auction.phase !== 'challenge-ready'
      ? auction.currentItem.lie
      : null;
    io.to(player.id).emit('auction-state', { ...base, myId: player.id, myVote: auction.votes[player.id] || null, myLieSecret: secret });
  });
  if (room.displaySockets) {
    room.displaySockets.forEach((socketId) => {
      io.to(socketId).emit('auction-state', { ...base, myId: socketId, isDisplay: true });
    });
  }
}

function startAuctionTutorial(pin) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  clearRoomTimers(room);
  const auction = room.auction;
  auction.phase = 'tutorial';
  auction.timerLeft = 0;
  auction.message = 'Rules first. Then bids.';
  emitAuctionState(pin);
}

function startAuctionListing(pin) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  clearRoomTimers(room);
  const auction = room.auction;
  if (auction.listingNumber >= auction.totalListings) {
    endAuctionGame(pin);
    return;
  }
  auction.listingNumber += 1;
  auction.phase = 'preview';
  auction.currentListing = makeAuctionListing(room);
  auction.currentItemIndex = 0;
  auction.currentItem = null;
  auction.currentBid = 0;
  auction.highBidderId = null;
  auction.buyerId = null;
  auction.soldPrice = 0;
  auction.noSale = false;
  auction.votes = {};
  auction.selectedTargets = [];
  auction.targetCount = 0;
  auction.timerLeft = 0;
  auction.message = 'Lot revealed.';
  emitAuctionState(pin);
}

function startAuctionBidding(pin) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  clearRoomTimers(room);
  const auction = room.auction;
  auction.phase = 'bidding';
  auction.timerLeft = AUCTION_BID_SECONDS;
  auction.message = 'Bid.';
  emitAuctionState(pin);
  const timer = setInterval(() => {
    auction.timerLeft -= 1;
    auction.message = auction.timerLeft === 2 ? 'Going once...' : auction.timerLeft === 1 ? 'Going twice...' : 'Bid.';
    if (auction.timerLeft <= 0) {
      clearInterval(timer);
      room.timers = room.timers.filter((entry) => entry !== timer);
      closeAuctionBidding(pin);
      return;
    }
    emitAuctionState(pin);
  }, 1000);
  room.timers.push(timer);
}

function closeAuctionBidding(pin) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  clearRoomTimers(room);
  const auction = room.auction;
  auction.phase = 'sold';
  auction.timerLeft = AUCTION_SOLD_SECONDS;
  if (!auction.highBidderId) {
    auction.noSale = true;
    auction.message = "It wasn't sold.";
  } else {
    auction.noSale = false;
    auction.buyerId = auction.highBidderId;
    auction.soldPrice = auction.currentBid;
    auction.money[auction.buyerId] = Math.max(0, (auction.money[auction.buyerId] || 0) - auction.currentBid);
    auction.message = `Sold to ${getAuctionPlayerName(room, auction.buyerId)} for ${formatMoney(auction.currentBid)}.`;
  }
  emitAuctionState(pin);
  const timer = setTimeout(() => {
    room.timers = room.timers.filter((entry) => entry !== timer);
    if (!auction.highBidderId) startAuctionListing(pin);
    else resolveNextAuctionItem(pin);
  }, AUCTION_SOLD_SECONDS * 1000);
  room.timers.push(timer);
}

function popAuctionModifier(auction, playerId) {
  const queue = auction.modifiers[playerId] || [];
  const modifier = queue.shift() || null;
  auction.modifiers[playerId] = queue;
  return modifier;
}

function resolveNextAuctionItem(pin) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  clearRoomTimers(room);
  const auction = room.auction;
  const item = auction.currentListing.items[auction.currentItemIndex];
  if (!item) {
    queueAuctionNext(pin);
    return;
  }
  auction.currentItem = item;
  auction.votes = {};
  auction.selectedTargets = [];
  auction.targetCount = 0;

  if (item.kind === 'points') {
    auction.scores[auction.buyerId] = (auction.scores[auction.buyerId] || 0) + item.points;
    auction.phase = 'result';
    auction.message = `${getAuctionPlayerName(room, auction.buyerId)} gains ${item.points} point${item.points === 1 ? '' : 's'}.`;
    finishAuctionItem(pin);
    return;
  }

  if (item.kind === 'power') {
    if (!item.targetCount) {
      applyAuctionPower(pin, item, []);
      return;
    }
    auction.phase = 'targeting';
    auction.targetCount = item.targetCount;
    auction.message = `${getAuctionPlayerName(room, auction.buyerId)}, choose your target.`;
    emitAuctionState(pin);
    return;
  }

  if (item.kind === 'minigame') {
    item.modifier = popAuctionModifier(auction, auction.buyerId);
    const adjusted = makeAuctionMinigameItem(item.modifier, item.gameType);
    Object.assign(item, adjusted);
    auction.phase = 'challenge-ready';
    auction.timerLeft = 0;
    auction.message = item.modifier === 'advantage'
      ? `${getAuctionPlayerName(room, auction.buyerId)} has an advantage.`
      : item.modifier === 'disadvantage'
        ? `${getAuctionPlayerName(room, auction.buyerId)} has a disadvantage.`
        : `${getAuctionPlayerName(room, auction.buyerId)} is up.`;
    emitAuctionState(pin);
  }
}

function startAuctionChallenge(pin) {
  const room = rooms[pin];
  if (!room || !room.auction || !room.auction.currentItem) return;
  clearRoomTimers(room);
  const auction = room.auction;
  const item = auction.currentItem;
  auction.phase = item.timerSeconds > 0 ? 'challenge-active' : 'voting';
  auction.timerLeft = item.timerSeconds || 0;
  auction.message = item.gameType === 'lie' ? 'Lie?' : item.name;
  auction.votes = {};
  emitAuctionState(pin);
  if (item.timerSeconds > 0) {
    const timer = setInterval(() => {
      auction.timerLeft -= 1;
      if (auction.timerLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter((entry) => entry !== timer);
        auction.phase = 'voting';
        auction.message = 'Vote pass or fail.';
      }
      emitAuctionState(pin);
    }, 1000);
    room.timers.push(timer);
  }
}

function applyAuctionPower(pin, item, targetIds) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  const auction = room.auction;
  const buyerId = auction.buyerId;
  const buyerName = getAuctionPlayerName(room, buyerId);
  const targets = targetIds.filter((id) => id && id !== buyerId && room.players.some((player) => player.id === id));
  const targetName = getAuctionPlayerName(room, targets[0]);
  if (item.type === 'steal') {
    const stolen = Math.min(item.amount, auction.scores[targets[0]] || 0);
    auction.scores[targets[0]] = Math.max(0, (auction.scores[targets[0]] || 0) - stolen);
    auction.scores[buyerId] = (auction.scores[buyerId] || 0) + stolen;
    auction.message = `${buyerName} steals ${stolen} point${stolen === 1 ? '' : 's'} from ${targetName}.`;
  } else if (item.type === 'lose') {
    const lost = Math.min(item.amount, auction.scores[targets[0]] || 0);
    auction.scores[targets[0]] = Math.max(0, (auction.scores[targets[0]] || 0) - lost);
    auction.message = `${targetName} loses ${lost} point${lost === 1 ? '' : 's'}.`;
  } else if (item.type === 'swap') {
    const buyerScore = auction.scores[buyerId] || 0;
    auction.scores[buyerId] = auction.scores[targets[0]] || 0;
    auction.scores[targets[0]] = buyerScore;
    auction.message = `${buyerName} swaps scores with ${targetName}.`;
  } else if (item.type === 'drink') {
    auction.message = `${targetName} owes ${buyerName} a drink.`;
  } else if (item.type === 'advantage' || item.type === 'advantage2') {
    const count = item.type === 'advantage2' ? 2 : 1;
    for (let i = 0; i < count; i++) auction.modifiers[buyerId].push('advantage');
    auction.message = `${buyerName} banks ${count === 1 ? 'an advantage' : 'two advantages'}.`;
  } else if (item.type === 'disadvantage' || item.type === 'disadvantage2') {
    targets.slice(0, item.targetCount).forEach((id) => auction.modifiers[id].push('disadvantage'));
    auction.message = `${buyerName} gives disadvantage to ${targets.slice(0, item.targetCount).map((id) => getAuctionPlayerName(room, id)).join(' and ')}.`;
  }
  auction.phase = 'result';
  finishAuctionItem(pin);
}

function finishAuctionItem(pin) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  emitAuctionState(pin);
  const timer = setTimeout(() => {
    room.timers = room.timers.filter((entry) => entry !== timer);
    room.auction.currentItemIndex += 1;
    resolveNextAuctionItem(pin);
  }, AUCTION_RESULT_SECONDS * 1000);
  room.timers.push(timer);
}

function getAuctionLieMultiplier(voterCount) {
  if (voterCount <= 3) return 4;
  if (voterCount <= 5) return 3;
  return 2;
}

function resolveAuctionVote(pin) {
  const room = rooms[pin];
  if (!room || !room.auction || !room.auction.currentItem) return;
  clearRoomTimers(room);
  const auction = room.auction;
  const item = auction.currentItem;
  const votes = auction.votes || {};
  const voterCount = room.players.filter((player) => player.id !== auction.buyerId).length;
  if (item.gameType === 'lie') {
    const fooled = Object.values(votes).filter((vote) => vote !== item.lie.task).length;
    const majorityFooled = fooled > voterCount / 2;
    const raw = fooled * getAuctionLieMultiplier(voterCount);
    const points = item.lie.disadvantage && !majorityFooled ? 0 : Math.min(item.points, raw);
    auction.scores[auction.buyerId] = (auction.scores[auction.buyerId] || 0) + points;
    auction.phase = 'result';
    auction.message = `${getAuctionPlayerName(room, auction.buyerId)} fooled ${fooled} player${fooled === 1 ? '' : 's'} and earns ${points} point${points === 1 ? '' : 's'}.`;
    finishAuctionItem(pin);
    return;
  }
  const yes = Object.values(votes).filter((vote) => vote === 'yes').length;
  const passed = yes > voterCount / 2;
  const points = passed ? item.points : 0;
  auction.scores[auction.buyerId] = (auction.scores[auction.buyerId] || 0) + points;
  auction.phase = 'result';
  auction.message = passed
    ? `${getAuctionPlayerName(room, auction.buyerId)} passes and earns ${points} point${points === 1 ? '' : 's'}.`
    : `${getAuctionPlayerName(room, auction.buyerId)} does not pass.\nNo points.`;
  finishAuctionItem(pin);
}

function queueAuctionNext(pin) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  const timer = setTimeout(() => {
    room.timers = room.timers.filter((entry) => entry !== timer);
    startAuctionListing(pin);
  }, AUCTION_RESULT_SECONDS * 1000);
  room.timers.push(timer);
}

function endAuctionGame(pin) {
  const room = rooms[pin];
  if (!room || !room.auction) return;
  clearRoomTimers(room);
  const auction = room.auction;
  const maxScore = Math.max(...Object.values(auction.scores));
  auction.winners = room.players.filter((player) => (auction.scores[player.id] || 0) === maxScore).map((player) => player.name);
  auction.phase = 'game-over';
  auction.message = auction.winners.length === 1 ? `Congratulations, ${auction.winners[0]}!\nI won't eat your soul.` : `${auction.winners.join(' and ')} tie for the win.`;
  emitAuctionState(pin);
}

function normalizeDebateSettings(debateSettings = {}) {
  const tiers = debateSettings.tiers || {};
  const normalizedTiers = {
    light: !!tiers.light,
    heavy: !!tiers.heavy,
    osmium: !!tiers.osmium,
  };
  if (!normalizedTiers.light && !normalizedTiers.heavy && !normalizedTiers.osmium) {
    normalizedTiers.light = true;
  }
  const parsedCount = Number.parseInt(debateSettings.forbiddenGroupCount, 10);
  return {
    tiers: normalizedTiers,
    limitedMode: !!debateSettings.limitedMode,
    forbiddenGroupCount: Math.max(1, Math.min(5, Number.isFinite(parsedCount) ? parsedCount : 2)),
  };
}

function normalizeDraftBoardSettings(settings = {}) {
  const rawCategory = String(settings.category || '').trim();
  const rawMetric = String(settings.metric || '').trim();
  const rawPickCount = Number.parseInt(settings.pickCount, 10);
  return {
    category: rawCategory.slice(0, 80),
    metric: rawMetric.slice(0, 80) || DRAFT_BOARD_DEFAULT_METRIC,
    pickCount: Math.max(2, Math.min(5, Number.isFinite(rawPickCount) ? rawPickCount : 3)),
    coachEnabled: settings.coachEnabled !== false && !!ANTHROPIC_API_KEY,
  };
}

function normalizeDraftPickText(value = '') {
  return String(value || '')
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

function buildDraftBoardOrder(players, pickCount) {
  const order = [];
  for (let round = 0; round < pickCount; round++) {
    const roundPlayers = round % 2 === 0 ? players : [...players].reverse();
    roundPlayers.forEach((player) => order.push(player.id));
  }
  return order;
}

function createDraftBoardState(room, settings) {
  const teamsByPlayerId = {};
  const scores = {};
  room.players.forEach((player) => {
    teamsByPlayerId[player.id] = [];
    scores[player.id] = 0;
  });
  return {
    settings,
    phase: 'drafting',
    draftOrder: buildDraftBoardOrder(room.players, settings.pickCount),
    currentTurnIndex: 0,
    teamsByPlayerId,
    picks: [],
    votes: {},
    scores,
    voteScores: {},
    playerChoiceWinners: [],
    coachResult: null,
    message: '',
  };
}

function buildDraftBoardStatePayload(room, viewerId = null) {
  const db = room.draftBoard;
  if (!db) return null;
  const players = room.players.map((player, index) => ({
    id: player.id,
    name: player.name,
    color: PLAYER_COLORS[index % PLAYER_COLORS.length],
    picks: db.teamsByPlayerId[player.id] || [],
    score: db.scores[player.id] || 0,
    voteScore: db.voteScores[player.id] || 0,
  }));
  const currentDrafterId = db.draftOrder[db.currentTurnIndex] || null;
  const eligibleVotes = viewerId
    ? players.filter((player) => player.id !== viewerId).map((player) => ({ id: player.id, name: player.name, color: player.color }))
    : [];
  const upcomingPickIds = db.draftOrder.slice(db.currentTurnIndex + 1, db.currentTurnIndex + 6);
  return {
    phase: db.phase,
    settings: db.settings,
    players,
    draftOrder: db.draftOrder,
    currentTurnIndex: db.currentTurnIndex,
    currentDrafterId,
    currentDrafterName: players.find((player) => player.id === currentDrafterId)?.name || '',
    upcomingPicks: upcomingPickIds.map((playerId) => {
      const player = players.find((entry) => entry.id === playerId);
      return player ? { id: player.id, name: player.name, color: player.color } : null;
    }).filter(Boolean),
    picks: db.picks,
    pickCount: db.settings.pickCount,
    voteCount: Object.keys(db.votes || {}).length,
    totalVotes: Math.max(0, room.players.length),
    myVote: viewerId ? (db.votes[viewerId] || null) : null,
    eligibleVotes,
    playerChoiceWinners: db.playerChoiceWinners,
    coachResult: db.coachResult,
    message: db.message,
    isCoachAvailable: !!ANTHROPIC_API_KEY,
  };
}

function emitDraftBoardState(pin) {
  const room = rooms[pin];
  if (!room || !room.draftBoard) return;
  room.players.forEach((player) => {
    io.to(player.id).emit('draft-board-state', buildDraftBoardStatePayload(room, player.id));
  });
  if (room.displaySockets) {
    room.displaySockets.forEach((socketId) => {
      io.to(socketId).emit('draft-board-state', buildDraftBoardStatePayload(room, null));
    });
  }
}

function advanceDraftBoardAfterPick(pin) {
  const room = rooms[pin];
  if (!room || !room.draftBoard) return;
  const db = room.draftBoard;
  db.currentTurnIndex += 1;
  if (db.currentTurnIndex >= db.draftOrder.length) {
    if (room.players.length <= 2) {
      db.phase = 'coach';
      db.message = db.settings.coachEnabled ? 'Coach is reviewing the boards...' : DRAFT_BOARD_COACH_ERROR;
      emitDraftBoardState(pin);
      finishDraftBoardGame(pin);
      return;
    }
    db.phase = 'voting';
    db.message = `Draft complete. Vote: ${db.settings.metric}.`;
  }
  emitDraftBoardState(pin);
}

function resolveDraftBoardVotes(pin) {
  const room = rooms[pin];
  if (!room || !room.draftBoard) return;
  const db = room.draftBoard;
  const voteScores = {};
  room.players.forEach((player) => { voteScores[player.id] = 0; });
  Object.values(db.votes).forEach((playerId) => {
    if (voteScores[playerId] !== undefined) voteScores[playerId] += 1;
  });
  db.voteScores = voteScores;
  const maxScore = Math.max(...Object.values(voteScores));
  db.playerChoiceWinners = maxScore > 0
    ? room.players.filter((player) => voteScores[player.id] === maxScore).map((player) => player.id)
    : [];
  db.playerChoiceWinners.forEach((playerId) => {
    db.scores[playerId] = (db.scores[playerId] || 0) + 1;
  });
}

function extractAnthropicText(response) {
  if (!response || !Array.isArray(response.content)) return '';
  return response.content
    .filter((item) => item && item.type === 'text' && typeof item.text === 'string')
    .map((item) => item.text)
    .join('\n')
    .trim();
}

function parseJSONFromText(text = '') {
  try {
    return JSON.parse(text);
  } catch (error) {
    const match = String(text).match(/\{[\s\S]*\}/);
    if (!match) return null;
    try {
      return JSON.parse(match[0]);
    } catch (innerError) {
      return null;
    }
  }
}

async function fetchDraftBoardCoachResult(room) {
  if (!ANTHROPIC_API_KEY || !room?.draftBoard) return null;
  const db = room.draftBoard;
  const teams = room.players.map((player) => ({
    playerName: player.name,
    picks: db.teamsByPlayerId[player.id] || [],
  }));
  const body = {
    model: ANTHROPIC_COACH_MODEL,
    max_tokens: 220,
    system: [
      'You are Coach, a concise judge for a party draft game.',
      'Pick exactly one winning team based only on the category, grading metric, and drafted picks.',
      'Do not invent picks. Be fair. Be straight and to the point.',
      'Return valid JSON only with shape {"winnerName":"...","explanation":"..."}',
      'The explanation must be 30 words or fewer.',
    ].join(' '),
    messages: [
      {
        role: 'user',
        content: JSON.stringify({ category: db.settings.category, gradingMetric: db.settings.metric, teams }),
      },
    ],
  };
  const response = await httpsPostJSON('https://api.anthropic.com/v1/messages', body, {
    'x-api-key': ANTHROPIC_API_KEY,
    'anthropic-version': '2023-06-01',
    'content-type': 'application/json',
  });
  if (response.error) {
    throw new Error(response.error.message || 'Anthropic Coach request failed');
  }
  const parsed = parseJSONFromText(extractAnthropicText(response));
  if (!parsed || typeof parsed.winnerName !== 'string') return null;
  const winner = room.players.find((player) => player.name.toLowerCase() === parsed.winnerName.trim().toLowerCase());
  if (!winner) return null;
  const explanation = String(parsed.explanation || '').trim().split(/\s+/).slice(0, 30).join(' ');
  return {
    winnerId: winner.id,
    winnerName: winner.name,
    explanation: explanation || 'Coach likes this board best.',
  };
}

async function finishDraftBoardGame(pin) {
  const room = rooms[pin];
  if (!room || !room.draftBoard) return;
  const db = room.draftBoard;
  if (!['voting', 'coach'].includes(db.phase)) return;
  if (db.phase === 'voting') resolveDraftBoardVotes(pin);
  const needsCoachOnly = room.players.length <= 2;
  db.phase = db.settings.coachEnabled ? 'coach' : 'reveal';
  db.message = db.settings.coachEnabled ? 'Coach is reviewing the boards...' : (needsCoachOnly ? DRAFT_BOARD_COACH_ERROR : '');
  emitDraftBoardState(pin);
  if (!db.settings.coachEnabled) return;

  try {
    const coachResult = await fetchDraftBoardCoachResult(room);
    if (!room.draftBoard || room.draftBoard !== db) return;
    if (coachResult) {
      db.coachResult = coachResult;
      db.scores[coachResult.winnerId] = (db.scores[coachResult.winnerId] || 0) + 1;
      db.message = '';
    } else {
      db.coachResult = null;
      db.message = DRAFT_BOARD_COACH_ERROR;
    }
  } catch (error) {
    if (!room.draftBoard || room.draftBoard !== db) return;
    db.coachResult = null;
    db.message = DRAFT_BOARD_COACH_ERROR;
  }
  db.phase = 'reveal';
  emitDraftBoardState(pin);
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
const DEBATE_OPENING_SECONDS = 30;
const DEBATE_REBUTTAL_SECONDS = 15;
const DEBATE_VOTE_SECONDS = 20;
const DEBATE_CATCH_SECONDS = 10;
const AUCTION_MIN_PLAYERS = 3;
const DRAFT_BOARD_MIN_PLAYERS = 2;
const DRAFT_BOARD_DEFAULT_CATEGORY = '';
const DRAFT_BOARD_DEFAULT_METRIC = 'Best';
const DRAFT_BOARD_COACH_ERROR = 'Coach had clipboard issues and could not make a pick.';
const AUCTION_STARTING_MONEY = 100;
const AUCTION_LISTINGS_PER_PLAYER = 2;
const AUCTION_PREVIEW_SECONDS = 4;
const AUCTION_BID_SECONDS = 6;
const AUCTION_SOLD_SECONDS = 3;
const AUCTION_RESULT_SECONDS = 4;
const AUCTION_BID_INCREMENTS = [1, 3, 5];

const AUCTION_FIND_CATEGORIES = [
  'red', 'blue', 'green', 'black', 'white', 'yellow',
  'metal', 'plastic', 'paper', 'fabric', 'glass', 'wood',
  'round', 'flat', 'soft', 'shiny',
  'contains the letter {letter} in its name',
  'something that opens',
  'something that you can drink out of',
  'something that rolls',
];

const AUCTION_COMMON_LETTERS = ['S', 'T', 'B', 'C', 'M', 'P', 'R'];
const AUCTION_RANDOM_LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
const AUCTION_NORMAL_QUESTIONS = [
  'What is a version of yourself you miss?',
  'What is something small that changed how you see someone?',
  'What do you think people misunderstand about you?',
  'What is a compliment you still remember?',
  'What is a fear you have outgrown?',
  'What is something you pretend not to care about?',
  'What is something you wish people noticed about you?',
  'What is a memory that feels warmer than it should?',
  'What is something you used to want badly, but don’t anymore?',
  'What is something you wish you were braver about?',
  'What is a lesson you learned later than you wish you had?',
  'What is something you wish you could tell your younger self?',
  'What is something you changed your mind about recently?',
  'What is something you care about more than people realize?',
  'What is a feeling you have gotten better at handling?',
  'What is something you wish lasted longer?',
];
const AUCTION_ADVANTAGE_QUESTIONS = [
  'What is a small thing that instantly improves your day?',
  'What is a place you always like going back to?',
  'What is a food that feels comforting?',
  'What is a song, movie, or show you associate with a good memory?',
  'What is something you are weirdly good at?',
  'What is a tiny win you had recently?',
  'What is something that makes a room feel better?',
  'What is a harmless thing you are picky about?',
  'What is a tradition you like?',
];
const AUCTION_DISADVANTAGE_QUESTIONS = [
  'What is something you are trying to forgive yourself for?',
  'What is something you wish you handled differently?',
  'What is a truth about yourself you resisted for a while?',
  'What is something you miss but know you cannot go back to?',
  'What is a pattern you are trying to break?',
  'What is an apology you still think about?',
  'What is something you wish you had said when you had the chance?',
];
const AUCTION_LIE_WORDS = [
  'apple', 'blanket', 'candle', 'marble', 'button', 'carpet', 'ladder', 'window', 'pickle', 'ribbon',
  'pocket', 'pillow', 'mirror', 'basket', 'noodle', 'rocket', 'bubble', 'velvet', 'pebble', 'kettle',
  'lantern', 'locket', 'wobble', 'thimble', 'bramble', 'glimmer', 'crinkle', 'spigot', 'goblet', 'hinge',
  'fiddle', 'trinket', 'orbit', 'snarl', 'burlap', 'quiver', 'dimple', 'sprig', 'yonder', 'knuckle',
  'clover', 'muffle', 'nugget', 'pollen', 'rivet', 'twine', 'zipper', 'curfew', 'dawdle', 'ember',
  'freckle', 'holler', 'jostle', 'kindle', 'ladle', 'meadow', 'nimble', 'oodles', 'quibble', 'rubble',
  'simmer', 'tumble', 'umpire', 'vessel', 'wicker', 'yelp', 'pancake', 'shoebox', 'thunder', 'mailbox',
  'oatmeal', 'popcorn', 'toothbrush', 'suitcase', 'snowball', 'doorknob', 'cupcake', 'flashlight',
  'bookmark', 'sidewalk', 'jellyfish', 'moonlight', 'sandwich', 'raincoat', 'teaspoon', 'backpack',
  'shoelace', 'campfire', 'haystack', 'corkscrew', 'seashell', 'windmill', 'drumstick', 'bathrobe',
  'birdhouse', 'notebook',
];

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

function remapPlayerId(room, oldId, newId) {
  if (oldId === newId) return;
  if (room.hostSocketId === oldId) room.hostSocketId = newId;

  const remapKey = (obj) => {
    if (obj && oldId in obj) { obj[newId] = obj[oldId]; delete obj[oldId]; }
  };
  const remapVal = (obj) => {
    if (!obj) return;
    for (const k of Object.keys(obj)) { if (obj[k] === oldId) obj[k] = newId; }
  };

  if (room.mafia) {
    const m = room.mafia;
    remapKey(m.roles); remapKey(m.alive); remapKey(m.votes); remapVal(m.votes);
    if (m.pendingNight) {
      remapKey(m.pendingNight.killerVotes); remapVal(m.pendingNight.killerVotes);
      if (m.pendingNight.doctorTarget === oldId) m.pendingNight.doctorTarget = newId;
      remapKey(m.pendingNight.pollVotes);
    }
    if (m.revoteCandidates) m.revoteCandidates = m.revoteCandidates.map(id => id === oldId ? newId : id);
  }

  if (room.debate && room.debate.currentRound) {
    const r = room.debate.currentRound;
    if (r.debaterA?.id === oldId) r.debaterA.id = newId;
    if (r.debaterB?.id === oldId) r.debaterB.id = newId;
    if (r.firstPickId === oldId) r.firstPickId = newId;
    if (r.pendingPick === oldId) r.pendingPick = newId;
    if (r.sideMap) remapKey(r.sideMap);
    if (r.votes) { remapKey(r.votes); remapVal(r.votes); }
    if (r.turnOrder) r.turnOrder = r.turnOrder.map(id => id === oldId ? newId : id);
    if (r.catchAttempts) remapKey(r.catchAttempts);
    if (r.activeCatch?.catcherId === oldId) r.activeCatch.catcherId = newId;
    if (r.revealWinnerId === oldId) r.revealWinnerId = newId;
  }

  if (room.pr) {
    remapKey(room.pr.guesses);
    if (room.pr.currentPhoto?.playerId === oldId) room.pr.currentPhoto.playerId = newId;
    room.pr.photos?.forEach(p => { if (p.playerId === oldId) p.playerId = newId; });
  }

  if (room.ht) remapKey(room.ht.votes);

  if (room.et) {
    remapKey(room.et.topicsByPlayerId);
    remapKey(room.et.currentAnswers);
  }

  if (room.draftBoard) {
    const db = room.draftBoard;
    if (db.teamsByPlayerId) remapKey(db.teamsByPlayerId);
    if (db.votes) { remapKey(db.votes); remapVal(db.votes); }
    if (db.scores) remapKey(db.scores);
    if (db.voteScores) remapKey(db.voteScores);
    if (db.draftOrder) db.draftOrder = db.draftOrder.map(id => id === oldId ? newId : id);
    if (db.picks) db.picks.forEach((pick) => { if (pick.playerId === oldId) pick.playerId = newId; });
    if (db.playerChoiceWinners) db.playerChoiceWinners = db.playerChoiceWinners.map(id => id === oldId ? newId : id);
    if (db.coachResult?.winnerId === oldId) db.coachResult.winnerId = newId;
  }

  if (room.qc) {
    room.qc.submissions?.forEach(s => { if (s.playerId === oldId) s.playerId = newId; });
    room.qc.playerOrder?.forEach(p => { if (p.id === oldId) p.id = newId; });
  }

  if (room.auction) {
    const a = room.auction;
    remapKey(a.money); remapKey(a.scores); remapKey(a.modifiers); remapKey(a.votes);
    if (a.highBidderId === oldId) a.highBidderId = newId;
    if (a.buyerId === oldId) a.buyerId = newId;
    if (a.selectedTargets) a.selectedTargets = a.selectedTargets.map(id => id === oldId ? newId : id);
  }
}

function reemitStateToPlayer(pin, socket) {
  const room = rooms[pin];
  if (!room) return;

  socket.emit('joined-room', {
    name: socket.data.name,
    players: room.players,
    pin,
    qrDataURL: room.qrDataURL,
    playerURL: room.playerURL,
    hostId: room.hostSocketId,
    isHost: socket.data.isHost,
  });

  const gs = room.gameState;
  if (gs === 'lobby') return;

  if (gs === 'game-select') {
    socket.emit('returned-to-game-select');
    socket.emit('game-select-shown');
    return;
  }

  if (gs === 'mafia' && room.mafia) {
    socket.emit('game-started', { game: 'mafia', resume: true });
    socket.emit('mafia-state', buildMafiaStateForPlayer(room, socket.id));
    return;
  }

  if (gs === 'debate' && room.debate) {
    socket.emit('game-started', { game: 'debate-setup', debateSettings: room.debate.settings, resume: true });
    emitDebateState(pin);
    return;
  }

  if (gs === 'hot-takes-setup') {
    socket.emit('game-started', { game: 'hot-takes-setup', exposureChance: room.pendingHTSettings?.exposureChance ?? 0.10, resume: true });
    socket.emit('ht-show-rules', room.pendingHTSettings || { exposureChance: 0.10 });
    return;
  }

  if (gs === 'hot-takes' && room.ht) {
    socket.emit('game-started', { game: 'hot-takes', resume: true });
    if (room.ht.optionA && room.ht.optionB) {
      socket.emit('ht-round-start', {
        question: room.ht.currentQuestionText,
        optionA: { id: room.ht.optionA.id, name: room.ht.optionA.name },
        optionB: { id: room.ht.optionB.id, name: room.ht.optionB.name },
        roundNumber: room.ht.currentRound,
        totalRounds: room.ht.totalRounds,
        playerColors: room.ht.playerColors,
      });
    }
    return;
  }

  if (gs === 'photo-roulette' && room.pr) {
    socket.emit('game-started', { game: 'photo-roulette', resume: true });
    socket.emit('pr-photo-count', { count: room.pr.photos.length });
    if (room.pr.currentPhoto) {
      const photo = room.pr.currentPhoto;
      let guessOptions = room.players.map(p => p.name);
      if (guessOptions.length > 6) {
        const others = guessOptions.filter(n => n !== photo.playerName);
        guessOptions = shuffle([photo.playerName, ...shuffle(others).slice(0, 5)]);
      }
      socket.emit('pr-guess-prompt', {
        guessOptions,
        photoNumber: room.pr.usedIds.size,
        totalPhotos: room.pr.photos.length,
        photoData: photo.dataURL,
        isYourPhoto: socket.id === photo.playerId,
      });
    }
    return;
  }

  if (gs === 'everything-trivia' && room.et) {
    socket.emit('game-started', { game: 'everything-trivia', resume: true });
    if (room.et.currentQuestion) {
      const section = room.et.randomizedTopics[room.et.currentSectionIndex];
      socket.emit('et-question-start', {
        topic: section.topic,
        submittedBy: section.playerName,
        sectionNumber: room.et.currentSectionIndex + 1,
        totalSections: room.et.randomizedTopics.length,
        questionNumber: room.et.currentQuestionIndex + 1,
        totalQuestionsInSection: section.questions.length,
        question: room.et.currentQuestion.question,
        answers: room.et.currentQuestion.answers,
      });
      socket.emit('et-answer-count', {
        count: Object.keys(room.et.currentAnswers).length,
        total: room.players.length,
      });
    } else {
      const topics = room.players.map(p => {
        const d = room.et.topicsByPlayerId[p.id] || {};
        return { playerName: p.name, topic: d.topic || null, status: d.status || 'waiting' };
      });
      socket.emit('et-topic-status', {
        readyCount: topics.filter(t => t.status === 'ready').length,
        total: room.players.length,
        topics,
      });
    }
    return;
  }

  if (gs === 'questions-challenges' && room.qc) {
    socket.emit('game-started', { game: 'questions-challenges', resume: true });
    const cp = room.qc.playerOrder[room.qc.currentPlayerIndex];
    if (cp) {
      const available = room.qc.submissions.filter(s => !room.qc.usedIds.has(s.id));
      socket.emit('qc-round-start', { currentPlayer: cp, available });
    }
    return;
  }

  if (gs === 'auction-setup') {
    socket.emit('game-started', { game: 'auction-setup', auctionSettings: room.pendingAuctionSettings, resume: true });
    socket.emit('auction-show-rules', { auctionSettings: room.pendingAuctionSettings });
    return;
  }

  if (gs === 'auction' && room.auction) {
    socket.emit('game-started', { game: 'auction', resume: true });
    emitAuctionState(pin);
    return;
  }

  if (gs === 'draft-board-setup') {
    socket.emit('game-started', { game: 'draft-board-setup', draftBoardSettings: room.pendingDraftBoardSettings, coachAvailable: !!ANTHROPIC_API_KEY, resume: true });
    socket.emit('draft-board-show-rules', { draftBoardSettings: room.pendingDraftBoardSettings, coachAvailable: !!ANTHROPIC_API_KEY });
    return;
  }

  if (gs === 'draft-board' && room.draftBoard) {
    socket.emit('game-started', { game: 'draft-board', resume: true });
    emitDraftBoardState(pin);
    return;
  }
}

io.on('connection', (socket) => {
  socket.use(([event], next) => {
    if (socket.data.isDisplay && event !== 'join-as-display') return;
    next();
  });

  // ── Lobby ──────────────────────────────────────────────────────

  socket.on('create-room', async ({ hostName }) => {
    const pin = generatePIN();
    const localIP = getLocalIP();
    const playerURL = PUBLIC_URL || `http://${localIP}:3000/`;
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
      displaySockets: new Set(),
      recentlyLeft: {},
      pendingHTSettings: { exposureChance: 0.10 },
      pendingDraftBoardSettings: normalizeDraftBoardSettings(),
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

    const rejoinEntry = room.recentlyLeft && room.recentlyLeft[name];

    if (room.gameState !== 'lobby' && room.gameState !== 'game-select' && room.gameState !== 'debate-setup' && room.gameState !== 'auction-setup' && room.gameState !== 'hot-takes-setup' && room.gameState !== 'draft-board-setup' && !rejoinEntry) {
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

    if (rejoinEntry) {
      remapPlayerId(room, rejoinEntry.oldSocketId, socket.id);
      socket.data.isHost = room.hostSocketId === socket.id;
      delete room.recentlyLeft[name];
      io.to(pin).emit('player-list-updated', { players: room.players, hostId: room.hostSocketId });
      reemitStateToPlayer(pin, socket);
      return;
    }

    socket.emit('joined-room', {
      name,
      players: room.players,
      pin,
      qrDataURL: room.qrDataURL,
      playerURL: room.playerURL,
      hostId: room.hostSocketId,
      isHost: false,
    });
    io.to(pin).emit('player-list-updated', { players: room.players, hostId: room.hostSocketId });
    if (room.gameState === 'game-select') {
      socket.emit('returned-to-game-select');
      socket.emit('game-select-shown');
    }
    if (room.gameState === 'debate-setup') {
      socket.emit('game-started', { game: 'debate-setup', debateSettings: room.pendingDebateSettings });
      socket.emit('debate-show-rules', { debateSettings: room.pendingDebateSettings });
    }
    if (room.gameState === 'hot-takes-setup') {
      socket.emit('game-started', { game: 'hot-takes-setup', exposureChance: room.pendingHTSettings?.exposureChance ?? 0.10 });
      socket.emit('ht-show-rules', room.pendingHTSettings || { exposureChance: 0.10 });
    }
    if (room.gameState === 'auction-setup') {
      socket.emit('game-started', { game: 'auction-setup', auctionSettings: room.pendingAuctionSettings });
      socket.emit('auction-show-rules', { auctionSettings: room.pendingAuctionSettings });
    }
    if (room.gameState === 'draft-board-setup') {
      socket.emit('game-started', { game: 'draft-board-setup', draftBoardSettings: room.pendingDraftBoardSettings, coachAvailable: !!ANTHROPIC_API_KEY });
      socket.emit('draft-board-show-rules', { draftBoardSettings: room.pendingDraftBoardSettings, coachAvailable: !!ANTHROPIC_API_KEY });
    }
  });

  socket.on('join-as-display', ({ pin }) => {
    const room = rooms[pin];
    if (!room) {
      socket.emit('display-error', { message: 'Room not found. Check the PIN.' });
      return;
    }

    socket.join(pin);
    socket.data.pin = pin;
    socket.data.isDisplay = true;

    if (!room.displaySockets) room.displaySockets = new Set();
    room.displaySockets.add(socket.id);

    socket.emit('display-joined', {
      pin,
      players: room.players,
      qrDataURL: room.qrDataURL,
      playerURL: room.playerURL,
      gameState: room.gameState,
      hostId: room.hostSocketId,
    });
    if (room.gameState === 'auction' && room.auction) {
      socket.emit('game-started', { game: 'auction' });
      emitAuctionState(pin);
    }
  });

  socket.on('join-as-spectator', ({ pin }) => {
    const room = rooms[pin];
    if (!room) {
      socket.emit('spectator-error', { message: 'Room not found. Check the PIN.' });
      return;
    }
    socket.join(pin);
    socket.data.pin = pin;
    socket.data.isSpectator = true;
    socket.emit('spectator-joined', {
      pin,
      players: room.players,
      qrDataURL: room.qrDataURL,
      playerURL: room.playerURL,
      hostId: room.hostSocketId,
    });
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

  // ── Light mode ────────────────────────────────────────────────

  // ── Game selector ──────────────────────────────────────────────

  socket.on('show-game-select', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    clearRoomTimers(room);
    room.gameState = 'game-select';
    room.qc = null;
    room.pr = null;
    room.ht = null;
    room.et = null;
    room.mafia = null;
    room.debate = null;
    room.auction = null;
    room.draftBoard = null;
    room.pendingHTSettings = { exposureChance: 0.10 };
    room.pendingDebateSettings = null;
    room.pendingMafiaSettings = null;
    room.pendingAuctionSettings = null;
    room.pendingDraftBoardSettings = normalizeDraftBoardSettings();
    io.to(pin).emit('returned-to-game-select');
    io.to(pin).emit('game-select-shown');
  });

  socket.on('start-game', ({ game, exposureChance, debateSettings, auctionSettings, draftBoardSettings }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    if (game === 'debate' && room.players.length < DEBATE_MIN_PLAYERS) {
      socket.emit('game-start-error', {
        game,
        message: `Debateish needs at least ${DEBATE_MIN_PLAYERS} players to start.`,
      });
      return;
    }

    if (game === 'auction' && room.players.length < AUCTION_MIN_PLAYERS) {
      socket.emit('game-start-error', {
        game,
        message: `Bidder's Auction needs at least ${AUCTION_MIN_PLAYERS} players to start.`,
      });
      return;
    }

    if (game === 'hot-takes' && room.players.length < 2) {
      socket.emit('game-start-error', {
        game,
        message: 'Secret Superlatives needs at least 2 players to start.',
      });
      return;
    }

    if (game === 'draft-board' && room.players.length < DRAFT_BOARD_MIN_PLAYERS) {
      socket.emit('game-start-error', {
        game,
        message: `Draft Board needs at least ${DRAFT_BOARD_MIN_PLAYERS} players to start.`,
      });
      return;
    }

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
      const exposure = (typeof exposureChance === 'number')
        ? Math.max(0, Math.min(1, exposureChance))
        : (room.pendingHTSettings?.exposureChance ?? 0.10);
      room.pendingHTSettings = { exposureChance: exposure };
      room.ht = {
        selectedQuestions: selectHTQuestions(room.players, 20),
        currentRound: 0,
        totalRounds: 20,
        votes: {},
        optionA: null,
        optionB: null,
        currentQuestionText: '',
        revealVotes: false,
        exposureChance: exposure,
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
      const settings = room.pendingMafiaSettings || { jokerEnabled: false, hiddenPollsUntilEnd: true };
      room.mafia = {
        jokerEnabled: !!settings.jokerEnabled,
        hiddenPollsUntilEnd: settings.hiddenPollsUntilEnd !== undefined ? !!settings.hiddenPollsUntilEnd : true,
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

    if (game === 'debate') {
      const scores = {};
      const playerColors = {};
      room.players.forEach((p, i) => {
        scores[p.name] = 0;
        playerColors[p.name] = PLAYER_COLORS[i % PLAYER_COLORS.length];
      });
      const settings = normalizeDebateSettings(debateSettings);
      room.pendingDebateSettings = settings;
      room.debate = {
        settings,
        scores,
        playerColors,
        debatedThisCycle: new Set(),
        topicPool: [],
        usedTopics: new Set(),
        pendingWinner: null,
        currentRound: null,
      };
    }

    if (game === 'auction') {
      const settings = normalizeAuctionSettings(auctionSettings || room.pendingAuctionSettings, room.players.length);
      room.pendingAuctionSettings = settings;
      const money = {};
      const scores = {};
      const modifiers = {};
      room.players.forEach((player) => {
        money[player.id] = AUCTION_STARTING_MONEY;
        scores[player.id] = 0;
        modifiers[player.id] = [];
      });
      room.auction = {
        listingNumber: 0,
        totalListings: settings.totalListings,
        phase: 'preview',
        currentListing: null,
        currentItemIndex: 0,
        currentItem: null,
        currentBid: 0,
        highBidderId: null,
        buyerId: null,
        soldPrice: 0,
        noSale: false,
        timerLeft: 0,
        message: '',
        money,
        scores,
        modifiers,
        votes: {},
        selectedTargets: [],
        targetCount: 0,
        winners: [],
      };
    }

    if (game === 'draft-board') {
      const settings = normalizeDraftBoardSettings(draftBoardSettings || room.pendingDraftBoardSettings);
      if (!settings.category) {
        socket.emit('game-start-error', {
          game,
          message: 'Write a category or choose one below.',
        });
        return;
      }
      if (room.players.length <= 2 && ANTHROPIC_API_KEY) settings.coachEnabled = true;
      room.pendingDraftBoardSettings = settings;
      room.draftBoard = createDraftBoardState(room, settings);
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

    if (game === 'debate') {
      buildDebateTopicPool(pin);
      startDebateRound(pin);
    }

    if (game === 'auction') {
      startAuctionListing(pin);
    }

    if (game === 'draft-board') {
      emitDraftBoardState(pin);
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
    room.debate = null;
    room.auction = null;
    room.draftBoard = null;
    room.pendingHTSettings = { exposureChance: 0.10 };
    room.pendingDebateSettings = null;
    room.pendingMafiaSettings = null;
    room.pendingAuctionSettings = null;
    room.pendingDraftBoardSettings = normalizeDraftBoardSettings();
    io.to(pin).emit('returned-to-lobby');
  });

  socket.on('back-to-game-select', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    clearRoomTimers(room);
    room.gameState = 'game-select';
    room.qc = null;
    room.pr = null;
    room.ht = null;
    room.et = null;
    room.mafia = null;
    room.debate = null;
    room.auction = null;
    room.draftBoard = null;
    room.pendingHTSettings = { exposureChance: 0.10 };
    room.pendingDebateSettings = null;
    room.pendingMafiaSettings = null;
    room.pendingAuctionSettings = null;
    room.pendingDraftBoardSettings = normalizeDraftBoardSettings();
    io.to(pin).emit('returned-to-game-select');
    io.to(pin).emit('game-select-shown');
  });

  // ── Questions & Challenges ─────────────────────────────────────

  socket.on('qc-start-collecting', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;

    room.qc = {
      submissions: [],
      playerOrder: [...room.players],
      currentPlayerIndex: 0,
      currentSubmission: null,
      usedIds: new Set(),
    };

    io.to(pin).emit('qc-collecting');
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

  // ── Secret Superlatives (legacy ht events) ─────────────────────

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
    room.pendingMafiaSettings = {
      jokerEnabled: room.mafia.jokerEnabled,
      hiddenPollsUntilEnd: room.mafia.hiddenPollsUntilEnd,
    };
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
    room.pendingMafiaSettings = {
      jokerEnabled: room.mafia.jokerEnabled,
      hiddenPollsUntilEnd: room.mafia.hiddenPollsUntilEnd,
    };
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

  socket.on('mafia-skip-vote', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.mafia || !['voting', 'revote'].includes(room.mafia.phase)) return;
    clearRoomTimers(room);
    resolveMafiaVote(pin, false);
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
    clearRoomTimers(room);

    const available = room.pr.photos.filter(p => !room.pr.usedIds.has(p.id));

    if (available.length === 0) {
      const scores = Object.entries(room.pr.scores)
        .map(([name, score]) => ({ name, score }))
        .sort((a, b) => b.score - a.score);
      io.to(pin).emit('pr-game-over', { scores });
      room.gameState = 'lobby';
      room.pr.phase = 'over';
      return;
    }

    const photo = available[Math.floor(Math.random() * available.length)];
    room.pr.usedIds.add(photo.id);
    room.pr.currentPhoto = photo;
    room.pr.guesses = {};
    room.pr.phase = 'guessing';

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
    if (room.displaySockets && room.displaySockets.size > 0) {
      room.displaySockets.forEach((socketId) => {
        io.to(socketId).emit('pr-guess-prompt', {
          guessOptions,
          photoNumber,
          totalPhotos,
          photoData: photo.dataURL,
          isYourPhoto: false,
        });
        io.to(socketId).emit('pr-display-photo', { photoData: photo.dataURL, photoNumber, totalPhotos });
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
    if (room.pr.phase === 'revealed') return;
    clearRoomTimers(room);
    room.pr.phase = 'revealed';

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
    const PR_ADVANCE_TIME = 5;
    room.pr.revealId = (room.pr.revealId || 0) + 1;
    const revealId = room.pr.revealId;

    io.to(pin).emit('pr-reveal', {
      photographerName: photo.playerName,
      guesses,
      pointsThisRound,
      scores: room.pr.scores,
      hasMorePhotos: remaining.length > 0,
      advanceTime: PR_ADVANCE_TIME,
      revealId,
    });

    // Auto-advance to next photo after 5 seconds
    let prAdvLeft = PR_ADVANCE_TIME;

    const prAdvTimer = setInterval(() => {
      prAdvLeft--;
      io.to(pin).emit('pr-advance-tick', { timeLeft: prAdvLeft, revealId });
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

    if (room.players.length < 2) {
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
      canHostSkipVote: playerId === room.hostSocketId && ['voting', 'revote'].includes(mafia.phase),
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

  function buildMafiaDisplayState(room) {
    const mafia = room.mafia;
    const publicVoteCounts = getMafiaVoteCounts(room);
    return {
      started: mafia.started,
      phase: mafia.phase,
      dayNumber: mafia.dayNumber,
      nightNumber: mafia.nightNumber,
      jokerEnabled: mafia.jokerEnabled,
      players: room.players.map((player) => ({
        id: player.id,
        name: player.name,
        alive: !!mafia.alive[player.id],
        voteCount: publicVoteCounts[player.id] || 0,
      })),
      eventMessage: mafia.eventMessage,
      eventTone: mafia.eventTone || 'neutral',
      revoteCandidates: mafia.revoteCandidates,
      timer: {
        discussion: mafia.discussionTimeLeft,
        voting: mafia.voteTimeLeft,
        night: mafia.nightTimeLeft || 0,
      },
      winner: mafia.winner,
      revealedRoles: mafia.phase === 'game-over'
        ? room.players.map((player) => ({ name: player.name, role: mafia.roles[player.id] }))
        : [],
    };
  }

  function buildMafiaPlayerViewForDisplay(room, socketId) {
    const state = buildMafiaStateForPlayer(room, socketId);
    if (state.started && !['game-over', 'winner-splash'].includes(state.phase)) {
      state.isAlive = true;
    }
    return state;
  }

  function emitMafiaState(pin) {
    const room = rooms[pin];
    if (!room || !room.mafia) return;
    room.players.forEach((player) => {
      io.to(player.id).emit('mafia-state', buildMafiaStateForPlayer(room, player.id));
    });
    if (room.displaySockets && room.displaySockets.size > 0) {
      const displayState = buildMafiaDisplayState(room);
      room.displaySockets.forEach((socketId) => {
        io.to(socketId).emit('mafia-state', buildMafiaPlayerViewForDisplay(room, socketId));
        io.to(socketId).emit('mafia-display-state', displayState);
      });
    }
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
    const lastDeath = room.mafia.deathLog[room.mafia.deathLog.length - 1] || null;
    if (lastDeath) winner.lastDeath = lastDeath;
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

    room.mafia.eventMessage = killedPlayer
      ? `${killedPlayer.name} was killed overnight.\n${room.mafia.roles[targetId] === 'killer' ? 'They were a killer.' : 'They were not a killer.'}`
      : 'Nobody died last night.';
    room.mafia.eventTone = killedPlayer ? 'bad' : 'good';
    room.mafia.phase = 'night-result';
    emitMafiaState(pin);

    const win = getMafiaWin(room);
    const timer = setTimeout(() => {
      room.timers = room.timers.filter((entry) => entry !== timer);
      if (!room.mafia) return;
      if (win) {
        startMafiaWinnerSplash(pin, win);
      } else {
        startMafiaDiscussion(pin, { incrementDay: true, keepEventMessage: true });
      }
    }, win ? 4500 : 5000);
    room.timers.push(timer);
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

  // ── Debate helpers ─────────────────────────────────────────────

  function buildDebateTopicPool(pin) {
    const room = rooms[pin];
    if (!room || !room.debate) return;
    const { tiers } = room.debate.settings;
    let pool = [];
    if (tiers.light) pool = pool.concat(DEBATE_TOPICS_LIGHT);
    if (tiers.heavy) pool = pool.concat(DEBATE_TOPICS_HEAVY);
    if (tiers.osmium) pool = pool.concat(DEBATE_TOPICS_OSMIUM);
    if (!pool.length) pool = [...DEBATE_TOPICS_LIGHT];
    room.debate.topicPool = shuffle(pool);
  }

  function rollDebateTopic(pin) {
    const room = rooms[pin];
    if (!room || !room.debate) return null;
    const { debate } = room;
    let available = debate.topicPool.filter(t => !debate.usedTopics.has(t));
    if (!available.length) {
      debate.usedTopics = new Set();
      buildDebateTopicPool(pin);
      available = [...debate.topicPool];
    }
    const topic = available[Math.floor(Math.random() * available.length)];
    debate.usedTopics.add(topic);
    const parts = topic.split(' vs ');
    return { topic, topicLeft: parts[0] || topic, topicRight: parts[1] || topic };
  }

  function pickDebaterPair(pin) {
    const room = rooms[pin];
    if (!room || !room.debate) return null;
    const { debate, players } = room;
    let eligible = players.filter(p => !debate.debatedThisCycle.has(p.name));
    if (eligible.length < 2) {
      debate.debatedThisCycle = new Set();
      eligible = [...players];
    }
    const minScore = Math.min(...eligible.map(p => debate.scores[p.name] || 0));
    const lowestPlayers = eligible.filter(p => (debate.scores[p.name] || 0) === minScore);
    const firstPick = pickRandom(lowestPlayers);
    const others = eligible.filter(p => p.id !== firstPick.id);
    const opponent = pickRandom(others);
    debate.debatedThisCycle.add(firstPick.name);
    debate.debatedThisCycle.add(opponent.name);
    return { firstPick, opponent };
  }

  function emitDebateState(pin) {
    const room = rooms[pin];
    if (!room || !room.debate) return;
    const { debate } = room;
    const r = debate.currentRound;
    const voterCount = r && r.debaterA && r.debaterB
      ? room.players.filter(p => p.id !== r.debaterA.id && p.id !== r.debaterB.id).length
      : 0;
    const currentSpeakerId = r && r.turnOrder && r.turnOrder.length === 2
      ? r.turnOrder[r.speakIdx % 2]
      : null;
    io.to(pin).emit('debate-state', {
      phase: r ? r.phase : 'waiting',
      subPhase: r ? r.subPhase : null,
      speakIdx: r ? r.speakIdx : 0,
      topic: r ? r.topic : null,
      topicLeft: r ? r.topicLeft : null,
      topicRight: r ? r.topicRight : null,
      debaterA: r ? r.debaterA : null,
      debaterB: r ? r.debaterB : null,
      firstPickId: r ? r.firstPickId : null,
      firstPickType: r ? r.firstPickType : null,
      pendingPick: r ? r.pendingPick : null,
      sideMap: r ? r.sideMap : {},
      turnOrder: r ? r.turnOrder : [],
      currentSpeakerId,
      timerLeft: r ? r.timerLeft : 0,
      bannedWords: r ? r.bannedWords : null,
      catchAttempts: r ? r.catchAttempts : {},
      activeCatch: r ? r.activeCatch : null,
      catchResult: r ? r.catchResult : null,
      votes: r ? r.votes : {},
      voteCount: r ? Object.keys(r.votes).length : 0,
      totalVoterCount: voterCount,
      scores: debate.scores,
      lastRoundScorers: debate.lastRoundScorers || [],
      playerColors: debate.playerColors,
      pendingWinner: debate.pendingWinner,
      settings: debate.settings,
      rerolled: r ? r.rerolled : false,
      revealWinnerId: r ? r.revealWinnerId : null,
      revealReason: r ? r.revealReason : null,
      awaitingSideRepick: r ? !!r.awaitingSideRepick : false,
    });
  }

  function startDebateRound(pin) {
    const room = rooms[pin];
    if (!room || !room.debate) return;
    const { debate } = room;
    if (debate.pendingWinner) {
      debate.currentRound = { phase: 'game-over', speakIdx: 0, sideMap: {}, turnOrder: [], catchAttempts: {}, votes: {}, timerLeft: 0, subPhase: null, bannedWords: null, activeCatch: null, catchResult: null, rerolled: false, revealWinnerId: null, revealReason: null };
      emitDebateState(pin);
      return;
    }
    const pair = pickDebaterPair(pin);
    if (!pair) return;
    const { firstPick, opponent } = pair;
    const topicData = rollDebateTopic(pin);
    if (!topicData) return;
    const bannedWords = debate.settings.limitedMode
      ? pickDebateBannedEntries(debate.settings.forbiddenGroupCount || 2)
      : null;
    debate.currentRound = {
      phase: 'picking',
      subPhase: null,
      speakIdx: 0,
      debaterA: { id: firstPick.id, name: firstPick.name, color: '#2563eb' },
      debaterB: { id: opponent.id, name: opponent.name, color: '#dc2626' },
      firstPickId: firstPick.id,
      firstPickType: null,
      pendingPick: firstPick.id,
      sideMap: {},
      turnOrder: [],
      topic: topicData.topic,
      topicLeft: topicData.topicLeft,
      topicRight: topicData.topicRight,
      bannedWords,
      timerLeft: 0,
      catchAttempts: {},
      activeCatch: null,
      catchResult: null,
      votes: {},
      rerolled: false,
      awaitingSideRepick: false,
      revealWinnerId: null,
      revealReason: null,
    };
    emitDebateState(pin);
  }

  function startDebateTimer(pin) {
    const room = rooms[pin];
    if (!room || !room.debate) return;
    const r = room.debate.currentRound;
    if (!r) return;
    clearRoomTimers(room);
    const isRebuttal = r.speakIdx >= 2;
    const timerMax = isRebuttal ? DEBATE_REBUTTAL_SECONDS : DEBATE_OPENING_SECONDS;
    r.timerLeft = timerMax;
    r.subPhase = 'active';
    emitDebateState(pin);
    const timer = setInterval(() => {
      r.timerLeft--;
      io.to(pin).emit('debate-tick', { timerLeft: r.timerLeft, timerMax });
      if (r.timerLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter(t => t !== timer);
        advanceDebateSpeakIdx(pin);
      }
    }, 1000);
    room.timers.push(timer);
  }

  function advanceDebateSpeakIdx(pin) {
    const room = rooms[pin];
    if (!room || !room.debate) return;
    const r = room.debate.currentRound;
    if (!r) return;
    r.speakIdx++;
    if (r.speakIdx >= 4) {
      r.phase = 'voting';
      r.subPhase = null;
      r.votes = {};
      r.timerLeft = DEBATE_VOTE_SECONDS;
      emitDebateState(pin);
      const timer = setInterval(() => {
        r.timerLeft--;
        io.to(pin).emit('debate-tick', { timerLeft: r.timerLeft, timerMax: DEBATE_VOTE_SECONDS });
        if (r.timerLeft <= 0) {
          clearInterval(timer);
          room.timers = room.timers.filter(t => t !== timer);
          resolveDebateVote(pin);
        }
      }, 1000);
      room.timers.push(timer);
    } else {
      r.phase = 'speaking';
      r.subPhase = 'pre';
      r.timerLeft = 0;
      emitDebateState(pin);
    }
  }

  function resolveDebateVote(pin) {
    const room = rooms[pin];
    if (!room || !room.debate) return;
    clearRoomTimers(room);
    const r = room.debate.currentRound;
    if (!r) return;
    let votesA = 0, votesB = 0;
    Object.values(r.votes).forEach(id => {
      if (id === r.debaterA.id) votesA++;
      else if (id === r.debaterB.id) votesB++;
    });
    let winnerId = null;
    if (votesA > votesB) winnerId = r.debaterA.id;
    else if (votesB > votesA) winnerId = r.debaterB.id;
    endDebateRound(pin, winnerId, 'vote');
  }

  function startDebateCatchVote(pin, catcherId) {
    const room = rooms[pin];
    if (!room || !room.debate) return;
    clearRoomTimers(room);
    const r = room.debate.currentRound;
    if (!r) return;
    const catcher = room.players.find(p => p.id === catcherId);
    r.catchAttempts[catcherId] = true;
    r.activeCatch = { catcherId, catcherName: catcher?.name || '', votes: {}, timerLeft: DEBATE_CATCH_SECONDS };
    r.phase = 'catch-vote';
    emitDebateState(pin);
    const timer = setInterval(() => {
      r.activeCatch.timerLeft--;
      io.to(pin).emit('debate-tick', { timerLeft: r.activeCatch.timerLeft, timerMax: DEBATE_CATCH_SECONDS });
      if (r.activeCatch.timerLeft <= 0) {
        clearInterval(timer);
        room.timers = room.timers.filter(t => t !== timer);
        resolveDebateCatchVote(pin);
      }
    }, 1000);
    room.timers.push(timer);
  }

  function resolveDebateCatchVote(pin) {
    const room = rooms[pin];
    if (!room || !room.debate) return;
    clearRoomTimers(room);
    const r = room.debate.currentRound;
    if (!r || !r.activeCatch) return;
    const { catcherId, votes } = r.activeCatch;
    let yes = 0, no = 0;
    Object.values(votes).forEach(v => { if (v === 'yes') yes++; else no++; });
    const catchConfirmed = yes >= no;
    const currentSpeakerId = r.turnOrder[r.speakIdx % 2];
    const winnerId = catchConfirmed ? catcherId : currentSpeakerId;
    r.catchResult = { yes, no, catchConfirmed, catcherId, speakerId: currentSpeakerId };
    r.activeCatch = null;
    endDebateRound(pin, winnerId, 'catch');
  }

  function endDebateRound(pin, winnerId, reason) {
    const room = rooms[pin];
    if (!room || !room.debate) return;
    clearRoomTimers(room);
    const { debate } = room;
    const r = debate.currentRound;
    if (!r) return;
    if (winnerId === null) {
      debate.scores[r.debaterA.name] = (debate.scores[r.debaterA.name] || 0) + 0.5;
      debate.scores[r.debaterB.name] = (debate.scores[r.debaterB.name] || 0) + 0.5;
      debate.lastRoundScorers = [r.debaterA.name, r.debaterB.name];
    } else {
      const winnerName = room.players.find(p => p.id === winnerId)?.name
        || (winnerId === r.debaterA.id ? r.debaterA.name : r.debaterB.name);
      debate.scores[winnerName] = (debate.scores[winnerName] || 0) + 1;
      debate.lastRoundScorers = [winnerName];
    }
    if (!debate.pendingWinner) {
      const maxScore = Math.max(...Object.values(debate.scores));
      if (maxScore >= 5) {
        const leaders = Object.entries(debate.scores).filter(([, s]) => s >= 5);
        const topScore = Math.max(...leaders.map(([, s]) => s));
        const top = leaders.filter(([, s]) => s === topScore);
        debate.pendingWinner = top[Math.floor(Math.random() * top.length)][0];
      }
    }
    r.phase = 'reveal';
    r.revealWinnerId = winnerId;
    r.revealReason = reason;
    emitDebateState(pin);
    const revealDurationMs = (reason === 'catch' || reason === 'vote') ? 5000 : 8000;
    const timer = setTimeout(() => {
      room.timers = room.timers.filter(t => t !== timer);
      if (!room.debate || !room.debate.currentRound) return;
      room.debate.currentRound.phase = 'round-end';
      emitDebateState(pin);
    }, revealDurationMs);
    room.timers.push(timer);
  }

  // ── Debate socket events ───────────────────────────────────────

  socket.on('debate-pick-choice', ({ choice }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.debate) return;
    const r = room.debate.currentRound;
    if (!r || r.phase !== 'picking' || r.pendingPick !== socket.id) return;

    const isFirstPick = socket.id === r.firstPickId;
    if (r.awaitingSideRepick) {
      if (choice !== 'left' && choice !== 'right') return;
      const otherId = socket.id === r.debaterA.id ? r.debaterB.id : r.debaterA.id;
      r.sideMap = {};
      r.sideMap[socket.id] = choice;
      r.sideMap[otherId] = choice === 'left' ? 'right' : 'left';
      r.pendingPick = null;
      r.awaitingSideRepick = false;
      r.phase = 'topic-reveal';
      emitDebateState(pin);
      return;
    }

    if (isFirstPick) {
      if (choice === 'left' || choice === 'right') {
        r.firstPickType = 'side';
        r.sideMap[socket.id] = choice;
      } else if (choice === 'first' || choice === 'second') {
        r.firstPickType = 'order';
        r.turnOrder = choice === 'first' ? [socket.id, null] : [null, socket.id];
      } else return;
      const otherId = socket.id === r.debaterA.id ? r.debaterB.id : r.debaterA.id;
      r.pendingPick = otherId;
    } else {
      if (r.firstPickType === 'side') {
        const firstPickerId = r.firstPickId;
        if (choice === 'first') r.turnOrder = [socket.id, firstPickerId];
        else r.turnOrder = [firstPickerId, socket.id];
        const usedSide = r.sideMap[firstPickerId];
        r.sideMap[socket.id] = usedSide === 'left' ? 'right' : 'left';
      } else if (r.firstPickType === 'order') {
        r.sideMap[socket.id] = choice === 'left' ? 'left' : 'right';
        r.sideMap[r.firstPickId] = choice === 'left' ? 'right' : 'left';
        const nullIdx = r.turnOrder.indexOf(null);
        if (nullIdx !== -1) r.turnOrder[nullIdx] = socket.id;
      } else return;
      r.pendingPick = null;
      r.phase = 'topic-reveal';
    }
    emitDebateState(pin);
  });

  socket.on('debate-reroll-topic', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.debate) return;
    const r = room.debate.currentRound;
    if (!r || r.phase !== 'topic-reveal' || r.rerolled) return;
    const topicData = rollDebateTopic(pin);
    if (!topicData) return;
    r.topic = topicData.topic;
    r.topicLeft = topicData.topicLeft;
    r.topicRight = topicData.topicRight;
    r.rerolled = true;
    const sidePickerId = r.firstPickType === 'side'
      ? r.firstPickId
      : (r.firstPickId === r.debaterA.id ? r.debaterB.id : r.debaterA.id);
    r.sideMap = {};
    r.pendingPick = sidePickerId;
    r.awaitingSideRepick = true;
    r.phase = 'picking';
    emitDebateState(pin);
  });

  socket.on('debate-confirm-topic', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.debate) return;
    const r = room.debate.currentRound;
    if (!r || r.phase !== 'topic-reveal') return;
    r.phase = 'speaking';
    r.subPhase = 'pre';
    r.speakIdx = 0;
    emitDebateState(pin);
  });

  socket.on('debate-start-speaking', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.debate) return;
    const r = room.debate.currentRound;
    if (!r || r.phase !== 'speaking' || r.subPhase !== 'pre') return;
    const currentSpeakerId = r.turnOrder[r.speakIdx % 2];
    if (socket.id !== currentSpeakerId) return;
    startDebateTimer(pin);
  });

  socket.on('debate-catch', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.debate || !room.debate.settings.limitedMode) return;
    const r = room.debate.currentRound;
    if (!r || r.phase !== 'speaking' || r.subPhase !== 'active') return;
    const isDebater = socket.id === r.debaterA.id || socket.id === r.debaterB.id;
    if (!isDebater) return;
    const currentSpeakerId = r.turnOrder[r.speakIdx % 2];
    if (socket.id === currentSpeakerId) return;
    if (r.catchAttempts[socket.id]) return;
    startDebateCatchVote(pin, socket.id);
  });

  socket.on('debate-catch-vote', ({ valid }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.debate) return;
    const r = room.debate.currentRound;
    if (!r || r.phase !== 'catch-vote' || !r.activeCatch) return;
    const isDebater = socket.id === r.debaterA.id || socket.id === r.debaterB.id;
    if (isDebater) return;
    if (r.activeCatch.votes[socket.id]) return;
    r.activeCatch.votes[socket.id] = valid ? 'yes' : 'no';
    emitDebateState(pin);
    const voterCount = room.players.filter(p => p.id !== r.debaterA.id && p.id !== r.debaterB.id).length;
    if (Object.keys(r.activeCatch.votes).length >= voterCount) resolveDebateCatchVote(pin);
  });

  socket.on('debate-vote', ({ forId }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.debate) return;
    const r = room.debate.currentRound;
    if (!r || r.phase !== 'voting') return;
    const isDebater = socket.id === r.debaterA.id || socket.id === r.debaterB.id;
    if (isDebater || r.votes[socket.id]) return;
    r.votes[socket.id] = forId;
    emitDebateState(pin);
    const voterCount = room.players.filter(p => p.id !== r.debaterA.id && p.id !== r.debaterB.id).length;
    if (Object.keys(r.votes).length >= voterCount) resolveDebateVote(pin);
  });

  socket.on('debate-next-round', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.debate) return;
    const r = room.debate.currentRound;
    if (!r || r.phase !== 'round-end') return;
    startDebateRound(pin);
  });

  socket.on('debate-show-rules', ({ debateSettings } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;
    const fallbackSettings = { tiers: { light: true, heavy: false, osmium: false }, limitedMode: false, forbiddenGroupCount: 2 };
    room.pendingDebateSettings = normalizeDebateSettings(debateSettings || room.pendingDebateSettings || fallbackSettings);
    room.gameState = 'debate-setup';
    io.to(pin).emit('game-started', { game: 'debate-setup', debateSettings: room.pendingDebateSettings });
    io.to(pin).emit('debate-show-rules', { debateSettings: room.pendingDebateSettings });
  });

  socket.on('debate-settings-preview', ({ debateSettings } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !debateSettings) return;
    room.pendingDebateSettings = normalizeDebateSettings(debateSettings);
    io.to(pin).emit('debate-settings-updated', { debateSettings: room.pendingDebateSettings });
  });

  socket.on('ht-show-rules', ({ exposureChance } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;
    const exposure = (typeof exposureChance === 'number')
      ? Math.max(0, Math.min(1, exposureChance))
      : (room.pendingHTSettings?.exposureChance ?? 0.10);
    room.pendingHTSettings = { exposureChance: exposure };
    room.gameState = 'hot-takes-setup';
    io.to(pin).emit('game-started', { game: 'hot-takes-setup', exposureChance: exposure });
    io.to(pin).emit('ht-show-rules', room.pendingHTSettings);
  });

  socket.on('ht-settings-preview', ({ exposureChance } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;
    const exposure = (typeof exposureChance === 'number')
      ? Math.max(0, Math.min(1, exposureChance))
      : 0.10;
    room.pendingHTSettings = { exposureChance: exposure };
    io.to(pin).emit('ht-settings-updated', room.pendingHTSettings);
  });

  socket.on('auction-show-rules', ({ auctionSettings } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;
    room.pendingAuctionSettings = normalizeAuctionSettings(auctionSettings || room.pendingAuctionSettings, room.players.length);
    room.gameState = 'auction-setup';
    io.to(pin).emit('game-started', { game: 'auction-setup', auctionSettings: room.pendingAuctionSettings });
    io.to(pin).emit('auction-show-rules', { auctionSettings: room.pendingAuctionSettings });
  });

  socket.on('auction-settings-preview', ({ auctionSettings } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !auctionSettings) return;
    room.pendingAuctionSettings = normalizeAuctionSettings(auctionSettings, room.players.length);
    io.to(pin).emit('auction-settings-updated', { auctionSettings: room.pendingAuctionSettings });
  });

  socket.on('draft-board-show-rules', ({ draftBoardSettings } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;
    room.pendingDraftBoardSettings = normalizeDraftBoardSettings(draftBoardSettings || room.pendingDraftBoardSettings);
    room.gameState = 'draft-board-setup';
    io.to(pin).emit('game-started', { game: 'draft-board-setup', draftBoardSettings: room.pendingDraftBoardSettings, coachAvailable: !!ANTHROPIC_API_KEY });
    io.to(pin).emit('draft-board-show-rules', { draftBoardSettings: room.pendingDraftBoardSettings, coachAvailable: !!ANTHROPIC_API_KEY });
  });

  socket.on('draft-board-settings-preview', ({ draftBoardSettings } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !draftBoardSettings) return;
    room.pendingDraftBoardSettings = normalizeDraftBoardSettings(draftBoardSettings);
    io.to(pin).emit('draft-board-settings-updated', { draftBoardSettings: room.pendingDraftBoardSettings, coachAvailable: !!ANTHROPIC_API_KEY });
  });

  socket.on('draft-board-submit-pick', ({ pick } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.draftBoard || room.gameState !== 'draft-board') return;
    const db = room.draftBoard;
    if (db.phase !== 'drafting') return;
    const activePlayerId = db.draftOrder[db.currentTurnIndex];
    if (activePlayerId !== socket.id) {
      socket.emit('draft-board-pick-error', { message: 'It is not your pick yet.' });
      return;
    }
    const cleanPick = String(pick || '').trim().replace(/\s+/g, ' ').slice(0, 50);
    if (!cleanPick) {
      socket.emit('draft-board-pick-error', { message: 'Type a pick first.' });
      return;
    }
    const normalized = normalizeDraftPickText(cleanPick);
    if (db.picks.some((entry) => entry.normalized === normalized)) {
      socket.emit('draft-board-pick-error', { message: 'That exact pick is already on the board.' });
      return;
    }
    const player = room.players.find((entry) => entry.id === socket.id);
    if (!player) return;
    if (!db.teamsByPlayerId[socket.id]) db.teamsByPlayerId[socket.id] = [];
    const pickEntry = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      playerId: socket.id,
      playerName: player.name,
      text: cleanPick,
      normalized,
    };
    db.picks.push(pickEntry);
    db.teamsByPlayerId[socket.id].push(cleanPick);
    db.message = '';
    advanceDraftBoardAfterPick(pin);
  });

  socket.on('draft-board-undo-pick', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id || !room.draftBoard) return;
    const db = room.draftBoard;
    if (!['drafting', 'voting'].includes(db.phase) || db.picks.length === 0) return;
    if (db.phase === 'voting' && Object.keys(db.votes || {}).length > 0) return;
    db.phase = 'drafting';
    const lastPick = db.picks.pop();
    if (lastPick && db.teamsByPlayerId[lastPick.playerId]) {
      db.teamsByPlayerId[lastPick.playerId].pop();
    }
    db.currentTurnIndex = Math.max(0, db.currentTurnIndex - 1);
    db.message = lastPick ? `Undid ${lastPick.playerName}'s pick: ${lastPick.text}` : '';
    emitDraftBoardState(pin);
  });

  socket.on('draft-board-submit-vote', ({ votedForId } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.draftBoard || room.gameState !== 'draft-board') return;
    const db = room.draftBoard;
    if (db.phase !== 'voting') return;
    const validIds = new Set(room.players.filter((player) => player.id !== socket.id).map((player) => player.id));
    if (!validIds.has(votedForId)) {
      socket.emit('draft-board-vote-error', { message: 'Choose one opponent board.' });
      return;
    }
    db.votes[socket.id] = votedForId;
    db.message = '';
    emitDraftBoardState(pin);
    if (Object.keys(db.votes).length >= room.players.length) {
      finishDraftBoardGame(pin);
    }
  });

  socket.on('draft-board-play-again', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || room.hostSocketId !== socket.id) return;
    room.draftBoard = null;
    room.pendingDraftBoardSettings = normalizeDraftBoardSettings(room.pendingDraftBoardSettings);
    room.gameState = 'draft-board-setup';
    io.to(pin).emit('game-started', { game: 'draft-board-setup', draftBoardSettings: room.pendingDraftBoardSettings, coachAvailable: !!ANTHROPIC_API_KEY });
    io.to(pin).emit('draft-board-show-rules', { draftBoardSettings: room.pendingDraftBoardSettings, coachAvailable: !!ANTHROPIC_API_KEY });
  });

  socket.on('auction-bid', ({ increment }) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.auction || room.auction.phase !== 'bidding') return;
    const amount = AUCTION_BID_INCREMENTS.includes(Number(increment)) ? Number(increment) : 1;
    const auction = room.auction;
    if (auction.highBidderId === socket.id) return;
    const nextBid = auction.currentBid + amount;
    if ((auction.money[socket.id] || 0) < nextBid) return;
    auction.currentBid = nextBid;
    auction.highBidderId = socket.id;
    auction.timerLeft = AUCTION_BID_SECONDS;
    auction.message = `${getAuctionPlayerName(room, socket.id)} bids ${formatMoney(nextBid)}.`;
    emitAuctionState(pin);
  });

  socket.on('auction-select-targets', ({ targetIds } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.auction || room.auction.phase !== 'targeting') return;
    const auction = room.auction;
    if (socket.id !== auction.buyerId) return;
    const item = auction.currentItem;
    if (!item || item.kind !== 'power') return;
    const ids = Array.isArray(targetIds) ? targetIds : [];
    const unique = [...new Set(ids)].filter((id) => id !== auction.buyerId && room.players.some((player) => player.id === id));
    if (unique.length < (item.targetCount || 1)) return;
    applyAuctionPower(pin, item, unique.slice(0, item.targetCount || 1));
  });

  socket.on('auction-vote', ({ vote } = {}) => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.auction || room.auction.phase !== 'voting') return;
    const auction = room.auction;
    if (socket.id === auction.buyerId) return;
    const item = auction.currentItem;
    if (!item || item.kind !== 'minigame') return;
    const allowed = item.gameType === 'lie' ? ['real', 'madeup'] : ['yes', 'no'];
    if (!allowed.includes(vote)) return;
    auction.votes[socket.id] = vote;
    emitAuctionState(pin);
    const voterCount = room.players.filter((player) => player.id !== auction.buyerId).length;
    if (Object.keys(auction.votes).length >= voterCount) resolveAuctionVote(pin);
  });

  socket.on('auction-start', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.auction || room.hostSocketId !== socket.id || room.auction.phase !== 'tutorial') return;
    startAuctionListing(pin);
  });

  socket.on('auction-ready', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.auction || room.auction.phase !== 'challenge-ready') return;
    if (socket.id !== room.auction.buyerId && socket.id !== room.hostSocketId) return;
    startAuctionChallenge(pin);
  });

  socket.on('auction-start-bidding', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.auction || room.hostSocketId !== socket.id || room.auction.phase !== 'preview') return;
    startAuctionBidding(pin);
  });

  socket.on('auction-skip', () => {
    const pin = socket.data.pin;
    const room = rooms[pin];
    if (!room || !room.auction || room.hostSocketId !== socket.id) return;
    const phase = room.auction.phase;
    if (phase === 'tutorial') startAuctionListing(pin);
    else if (phase === 'preview') startAuctionBidding(pin);
    else if (phase === 'bidding') closeAuctionBidding(pin);
    else if (phase === 'sold') {
      if (!room.auction.highBidderId) startAuctionListing(pin);
      else resolveNextAuctionItem(pin);
    } else if (phase === 'challenge-ready') startAuctionChallenge(pin);
    else if (phase === 'challenge-active') {
      clearRoomTimers(room);
      room.auction.phase = 'voting';
      room.auction.message = 'Vote pass or fail.';
      emitAuctionState(pin);
    } else if (phase === 'voting') resolveAuctionVote(pin);
    else if (phase === 'result') {
      clearRoomTimers(room);
      room.auction.currentItemIndex += 1;
      resolveNextAuctionItem(pin);
    }
  });

  // ── Disconnect ─────────────────────────────────────────────────

  socket.on('disconnect', () => {
    const pin = socket.data.pin;
    if (!pin || !rooms[pin]) return;

    const room = rooms[pin];

    if (socket.data.isDisplay) {
      if (room.displaySockets) room.displaySockets.delete(socket.id);
      return;
    }

    if (socket.data.isSpectator) return;

    // Save for potential mid-game rejoin (90s window)
    if (socket.data.name) {
      const leftName = socket.data.name;
      room.recentlyLeft[leftName] = { oldSocketId: socket.id, leftAt: Date.now(), wasHost: !!socket.data.isHost };
      setTimeout(() => {
        const currentRoom = rooms[pin];
        const entry = currentRoom?.recentlyLeft?.[leftName];
        if (!currentRoom || !entry || entry.oldSocketId !== socket.id) return;
        delete currentRoom.recentlyLeft[leftName];

        if (currentRoom.players.length === 0) {
          clearRoomTimers(currentRoom);
          delete rooms[pin];
          return;
        }

        if (currentRoom.gameState === 'debate' && currentRoom.debate) {
          const r = currentRoom.debate.currentRound;
          if (r && r.debaterA && r.debaterB) {
            const isDebaterA = socket.id === r.debaterA.id;
            const isDebaterB = socket.id === r.debaterB.id;
            const isMidRound = ['speaking', 'catch-vote', 'voting'].includes(r.phase);
            if ((isDebaterA || isDebaterB) && isMidRound) {
              const winnerId = isDebaterA ? r.debaterB.id : r.debaterA.id;
              endDebateRound(pin, winnerId, 'disconnect');
            }
          }
          if (currentRoom.players.length < 3) {
            clearRoomTimers(currentRoom);
            currentRoom.gameState = 'lobby';
            currentRoom.debate = null;
            io.to(pin).emit('debate-stopped', { reason: 'A player left and there are not enough players to continue (need at least 3).' });
            return;
          }
        }

        if (currentRoom.gameState === 'auction' && currentRoom.auction) {
          if (currentRoom.auction.highBidderId === socket.id && currentRoom.auction.phase === 'bidding') {
            currentRoom.auction.highBidderId = null;
            currentRoom.auction.currentBid = 0;
            currentRoom.auction.message = 'High bidder left. Bidding resets.';
            emitAuctionState(pin);
          }
          if (currentRoom.players.length < AUCTION_MIN_PLAYERS) {
            clearRoomTimers(currentRoom);
            currentRoom.gameState = 'lobby';
            currentRoom.auction = null;
            io.to(pin).emit('auction-stopped', { reason: `Bidder's Auction ended because there are fewer than ${AUCTION_MIN_PLAYERS} players.` });
          }
        }

        if (currentRoom.gameState === 'draft-board' && currentRoom.draftBoard) {
          clearRoomTimers(currentRoom);
          currentRoom.gameState = 'lobby';
          currentRoom.draftBoard = null;
          io.to(pin).emit('draft-board-stopped', { reason: 'Draft Board ended because a player left.' });
        }
      }, REJOIN_GRACE_MS);
    }

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
        if (room.gameState === 'lobby') {
          clearRoomTimers(room);
          delete rooms[pin];
        }
        return;
      }
    }

    // Debate disconnect handling
    if (!room.recentlyLeft?.[socket.data.name] && room.gameState === 'debate' && room.debate) {
      const r = room.debate.currentRound;
      if (r && r.debaterA && r.debaterB) {
        const isDebaterA = socket.id === r.debaterA.id;
        const isDebaterB = socket.id === r.debaterB.id;
        const isMidRound = ['speaking', 'catch-vote', 'voting'].includes(r.phase);
        if ((isDebaterA || isDebaterB) && isMidRound) {
          const winnerId = isDebaterA ? r.debaterB.id : r.debaterA.id;
          endDebateRound(pin, winnerId, 'disconnect');
        }
      }
      if (room.players.length < 3) {
        clearRoomTimers(room);
        room.gameState = 'lobby';
        room.debate = null;
        io.to(pin).emit('debate-stopped', { reason: 'A player left and there are not enough players to continue (need at least 3).' });
        return;
      }
    }

    if (!room.recentlyLeft?.[socket.data.name] && room.gameState === 'auction' && room.auction) {
      if (room.auction.highBidderId === socket.id && room.auction.phase === 'bidding') {
        room.auction.highBidderId = null;
        room.auction.currentBid = 0;
        room.auction.message = 'High bidder left. Bidding resets.';
        emitAuctionState(pin);
      }
      if (room.players.length < AUCTION_MIN_PLAYERS) {
        clearRoomTimers(room);
        room.gameState = 'lobby';
        room.auction = null;
        io.to(pin).emit('auction-stopped', { reason: `Bidder's Auction ended because there are fewer than ${AUCTION_MIN_PLAYERS} players.` });
        return;
      }
    }

    if (!room.recentlyLeft?.[socket.data.name] && room.gameState === 'draft-board' && room.draftBoard) {
      if (room.players.length < DRAFT_BOARD_MIN_PLAYERS) {
        clearRoomTimers(room);
        room.gameState = 'lobby';
        room.draftBoard = null;
        io.to(pin).emit('draft-board-stopped', { reason: `Draft Board ended because there are fewer than ${DRAFT_BOARD_MIN_PLAYERS} players.` });
        return;
      }
      emitDraftBoardState(pin);
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
