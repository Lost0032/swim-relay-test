import { WebSocketServer } from 'ws';
import { randomUUID } from 'crypto';

const PORT = process.env.PORT || 8080;

// Максимум игроков в одном лобби (можно переопределить переменной окружения MAX_PLAYERS).
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 8;
const MAX_NAME_LEN = 24;
// Защита от флуда: сообщений в секунду от одного соединения (relay для геймплея входит сюда).
const MAX_MSGS_PER_SEC = 300;

// maxPayload: сообщения больше 64 КБ рвут соединение (защита от мусора).
const wss = new WebSocketServer({ port: PORT, maxPayload: 64 * 1024 });

const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

// code -> { hostId, players: Map(playerId -> { ws, name, profile }), map, bannedIds: Set, bannedIps: Set, gameStarted: bool }
const rooms = new Map();

// ---------------------------------------------------------------- хелперы

function makeCode() {
	let code;
	do {
		code = '';
		for (let i = 0; i < 5; i++) {
			code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
		}
	} while (rooms.has(code));
	return code;
}

function send(ws, obj) {
	if (ws.readyState === ws.OPEN) {
		ws.send(JSON.stringify(obj));
	}
}

function broadcastRoom(room, obj, excludeId = null) {
	for (const [id, player] of room.players) {
		if (id !== excludeId) send(player.ws, obj);
	}
}

// За прокси (Render) req.socket.remoteAddress — это адрес прокси, одинаковый у ВСЕХ игроков:
// бан по такому "IP" банил бы всех сразу. Реальный адрес приходит в заголовках прокси.
function clientIp(req) {
	const cf = req.headers['cf-connecting-ip'];
	if (typeof cf === 'string' && cf) return cf.trim();
	const xff = req.headers['x-forwarded-for'];
	if (typeof xff === 'string' && xff) return xff.split(',')[0].trim();
	return req.socket.remoteAddress;
}

function cleanStr(v, max) {
	return typeof v === 'string' ? v.slice(0, max) : '';
}

function cleanName(v) {
	return cleanStr(v, MAX_NAME_LEN).trim() || 'Player';
}

function cleanMap(v) {
	return typeof v === 'string' && /^[A-Za-z0-9_-]{1,40}$/.test(v) ? v : null;
}

function cleanNum01(v) {
	const n = Number(v);
	return Number.isFinite(n) ? Math.min(Math.max(n, 0), 1) : 0;
}

// Словарь "имя части -> число 0..1" (оттенки/насыщенность скина оружия). Null-прототип и лимиты — от мусора.
function cleanNumMap(m, maxKeys) {
	const out = Object.create(null);
	if (!m || typeof m !== 'object') return out;
	for (const [k, v] of Object.entries(m).slice(0, maxKeys)) {
		const key = cleanStr(k, 48);
		if (key) out[key] = cleanNum01(v);
	}
	return out;
}

// Скин оружия (только перекраска): { n: имя, r: редкость, h: {часть: оттенок}, s: {часть: насыщенность} }.
// Скины с пользовательской текстурой не передаются (файл есть только у владельца) — клиент шлёт null.
function cleanWeaponSkin(sk) {
	if (!sk || typeof sk !== 'object') return null;
	const h = cleanNumMap(sk.h, 12);
	if (Object.keys(h).length === 0) return null;
	return { n: cleanStr(sk.n, 40), r: cleanStr(sk.r, 32), h, s: cleanNumMap(sk.s, 12) };
}

// Запись об оружии: { id, slot, skin }.
function cleanWeaponEntries(v, maxItems) {
	if (!Array.isArray(v)) return [];
	const out = [];
	for (const e of v.slice(0, maxItems)) {
		if (!e || typeof e !== 'object') continue;
		const id = cleanStr(e.id, 64);
		if (!id) continue;
		out.push({ id, slot: cleanStr(e.slot, 16), skin: cleanWeaponSkin(e.skin) });
	}
	return out;
}

// Профиль игрока: то, что другие видят в лобби. Клиент присылает его при входе и при смене экипировки.
// Лёгкие поля (level, skin, weapons) уходят всем в player_list; инвентарь (все оружия игрока) — по запросу get_inventory.
function cleanProfile(p) {
	p = p && typeof p === 'object' ? p : {};
	const level = Math.min(Math.max(Math.floor(Number(p.level)) || 1, 1), 999);
	return {
		level,
		skin: cleanStr(p.skin, 64),
		weapons: cleanWeaponEntries(p.weapons, 8),
		inventory: cleanWeaponEntries(p.inventory, 100),
	};
}

function publicProfile(profile) {
	return { level: profile.level, skin: profile.skin, weapons: profile.weapons };
}

function playerListPayload(room) {
	return {
		type: 'player_list',
		max_players: MAX_PLAYERS,
		players: [...room.players.entries()].map(([id, p]) => ({
			id,
			name: p.name,
			is_host: id === room.hostId,
			profile: publicProfile(p.profile),
		})),
	};
}

// Шлёт сообщение и закрывает соединение чуть позже, чтобы клиент успел
// получить текст причины (kicked/banned) до разрыва сокета.
function disconnectPlayer(ws, payload) {
	send(ws, payload);
	setTimeout(() => {
		try { ws.close(); } catch {}
	}, 100);
}

function leaveRoom(ws) {
	const code = ws.roomCode;
	ws.roomCode = null;
	const room = rooms.get(code);
	if (!room) return;

	room.players.delete(ws.playerId);

	if (room.players.size === 0) {
		rooms.delete(code);
		return;
	}

	if (room.hostId === ws.playerId) {
		room.hostId = [...room.players.keys()][0];
	}

	broadcastRoom(room, playerListPayload(room));
}

// ---------------------------------------------------------------- соединения

wss.on('connection', (ws, req) => {
	const playerId = randomUUID();
	ws.playerId = playerId;
	ws.roomCode = null;
	ws.ip = clientIp(req);

	ws.msgCount = 0;
	const rateTimer = setInterval(() => { ws.msgCount = 0; }, 1000);

	ws.on('error', () => {});

	ws.on('message', (raw) => {
		if (++ws.msgCount > MAX_MSGS_PER_SEC) return;

		let msg;
		try {
			msg = JSON.parse(raw.toString());
		} catch {
			return;
		}
		if (!msg || typeof msg !== 'object') return;

		switch (msg.type) {
			case 'create_room': {
				if (ws.roomCode) leaveRoom(ws);

				const code = makeCode();
				const room = {
					hostId: playerId,
					players: new Map(),
					map: null,
					bannedIds: new Set(),
					bannedIps: new Set(),
					gameStarted: false,
				};
				room.players.set(playerId, { ws, name: cleanName(msg.name), profile: cleanProfile(msg.profile) });
				rooms.set(code, room);
				ws.roomCode = code;

				send(ws, { type: 'room_created', code, player_id: playerId, is_host: true, max_players: MAX_PLAYERS });
				send(ws, playerListPayload(room));
				break;
			}

			case 'join_room': {
				const code = cleanStr(msg.code, 10).toUpperCase();
				const room = rooms.get(code);
				if (!room) {
					send(ws, { type: 'error', message: 'Лобби с таким кодом не найдено' });
					return;
				}

				if (room.bannedIds.has(playerId) || room.bannedIps.has(ws.ip)) {
					send(ws, { type: 'join_rejected', reason: 'Вы забанены в этом лобби' });
					return;
				}

				if (room.gameStarted) {
					send(ws, { type: 'join_rejected', reason: 'Игра уже началась, дождитесь конца' });
					return;
				}

				if (ws.roomCode === code) return; // уже в этой комнате

				if (room.players.size >= MAX_PLAYERS) {
					send(ws, { type: 'join_rejected', reason: `Лобби заполнено (${room.players.size}/${MAX_PLAYERS})` });
					return;
				}

				if (ws.roomCode) leaveRoom(ws);

				room.players.set(playerId, { ws, name: cleanName(msg.name), profile: cleanProfile(msg.profile) });
				ws.roomCode = code;

				send(ws, { type: 'room_joined', code, player_id: playerId, is_host: false, max_players: MAX_PLAYERS });
				if (room.map) send(ws, { type: 'map_selected', map: room.map });
				broadcastRoom(room, playerListPayload(room));
				break;
			}

			case 'set_profile': {
				const room = rooms.get(ws.roomCode);
				const player = room && room.players.get(playerId);
				if (!player) return;
				player.profile = cleanProfile(msg.profile);
				broadcastRoom(room, playerListPayload(room));
				break;
			}

			case 'get_inventory': {
				const room = rooms.get(ws.roomCode);
				if (!room) return;
				const target = room.players.get(cleanStr(msg.player_id, 64));
				if (!target) return;
				send(ws, { type: 'inventory', player_id: msg.player_id, inventory: target.profile.inventory });
				break;
			}

			case 'select_map': {
				const room = rooms.get(ws.roomCode);
				if (!room || room.hostId !== playerId) return;
				const map = cleanMap(msg.map);
				if (!map) return;
				room.map = map;
				broadcastRoom(room, { type: 'map_selected', map: room.map });
				break;
			}

			case 'start_game': {
				const room = rooms.get(ws.roomCode);
				if (!room || room.hostId !== playerId || room.gameStarted) return;

				room.gameStarted = true;
				const orderedIds = [...room.players.keys()];

				broadcastRoom(room, {
					type: 'game_started',
					map: room.map || 'default',
					players: orderedIds,
				});
				break;
			}

			// Хост возвращает лобби из матча — иначе в комнату больше никто не сможет зайти
			// (gameStarted остаётся true навсегда).
			case 'return_to_lobby': {
				const room = rooms.get(ws.roomCode);
				if (!room || room.hostId !== playerId) return;
				room.gameStarted = false;
				broadcastRoom(room, { type: 'back_to_lobby' });
				broadcastRoom(room, playerListPayload(room));
				break;
			}

			case 'kick': {
				const room = rooms.get(ws.roomCode);
				if (!room || room.hostId !== playerId) return;
				const targetId = msg.player_id;
				if (!targetId || targetId === playerId) return;

				const target = room.players.get(targetId);
				if (!target) return;

				room.players.delete(targetId);
				target.ws.roomCode = null;
				broadcastRoom(room, playerListPayload(room));

				disconnectPlayer(target.ws, { type: 'kicked', reason: 'Вас кикнул хост' });
				break;
			}

			case 'ban': {
				const room = rooms.get(ws.roomCode);
				if (!room || room.hostId !== playerId) return;
				const targetId = msg.player_id;
				if (!targetId || targetId === playerId) return;

				const target = room.players.get(targetId);
				if (!target) return;

				room.bannedIds.add(targetId);
				room.bannedIps.add(target.ws.ip);

				room.players.delete(targetId);
				target.ws.roomCode = null;
				broadcastRoom(room, playerListPayload(room));

				disconnectPlayer(target.ws, { type: 'banned', reason: 'Вас забанил хост в этом лобби' });
				break;
			}

			case 'relay': {
				const room = rooms.get(ws.roomCode);
				if (!room) return;
				broadcastRoom(room, { type: 'relay', from: playerId, data: msg.data }, playerId);
				break;
			}

			case 'leave_room': {
				leaveRoom(ws);
				break;
			}

			case 'ping': {
				break;
			}
		}
	});

	ws.on('close', () => {
		clearInterval(rateTimer);
		leaveRoom(ws);
	});
});

console.log(`Relay server listening on port ${PORT} (max ${MAX_PLAYERS} players per lobby)`);
