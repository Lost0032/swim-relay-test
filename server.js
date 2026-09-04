import { WebSocketServer } from 'ws';
import { randomUUID } from 'crypto';

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

// code -> { hostId, players: Map(playerId -> { ws, name }), map, bannedIds: Set, gameStarted: bool }
const rooms = new Map();

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

function playerListPayload(room) {
	return {
		type: 'player_list',
		players: [...room.players.entries()].map(([id, p]) => ({
			id,
			name: p.name,
			is_host: id === room.hostId,
		})),
	};
}

wss.on('connection', (ws) => {
	const playerId = randomUUID();
	ws.playerId = playerId;
	ws.roomCode = null;

	ws.on('message', (raw) => {
		let msg;
		try {
			msg = JSON.parse(raw.toString());
		} catch {
			return;
		}

		switch (msg.type) {
			case 'create_room': {
				const code = makeCode();
				const room = {
					hostId: playerId,
					players: new Map(),
					map: null,
					bannedIds: new Set(),
					gameStarted: false,
				};
				room.players.set(playerId, { ws, name: msg.name || 'Player' });
				rooms.set(code, room);
				ws.roomCode = code;

				send(ws, { type: 'room_created', code, player_id: playerId, is_host: true });
				send(ws, playerListPayload(room));
				break;
			}

			case 'join_room': {
				const code = (msg.code || '').toUpperCase();
				const room = rooms.get(code);
				if (!room) {
					send(ws, { type: 'error', message: 'Лобби с таким кодом не найдено' });
					return;
				}

				if (room.bannedIds.has(playerId)) {
					send(ws, { type: 'join_rejected', reason: 'Вы забанены в этом лобби' });
					return;
				}

				if (room.gameStarted) {
					send(ws, { type: 'join_rejected', reason: 'Игра уже началась, дождитесь конца' });
					return;
				}

				room.players.set(playerId, { ws, name: msg.name || 'Player' });
				ws.roomCode = code;

				send(ws, { type: 'room_joined', code, player_id: playerId, is_host: false });
				if (room.map) send(ws, { type: 'map_selected', map: room.map });
				broadcastRoom(room, playerListPayload(room));
				break;
			}

			case 'select_map': {
				const room = rooms.get(ws.roomCode);
				if (!room || room.hostId !== playerId) return;
				room.map = msg.map;
				broadcastRoom(room, { type: 'map_selected', map: room.map });
				break;
			}

			case 'start_game': {
				const room = rooms.get(ws.roomCode);
				if (!room || room.hostId !== playerId) return;

				room.gameStarted = true;

				const orderedIds = [...room.players.keys()];

				broadcastRoom(room, {
					type: 'game_started',
					map: room.map || 'default',
					players: orderedIds,
				});
				break;
			}

			case 'kick': {
				const room = rooms.get(ws.roomCode);
				if (!room || room.hostId !== playerId) return; // только хост кикает
				const targetId = msg.player_id;
				if (!targetId || targetId === playerId) return; // нельзя кикнуть себя

				const target = room.players.get(targetId);
				if (!target) return;

				send(target.ws, { type: 'kicked', reason: 'Вас кикнул хост' });
				room.players.delete(targetId);
				target.ws.roomCode = null;
				target.ws.close();

				broadcastRoom(room, playerListPayload(room));
				break;
			}

			case 'ban': {
				const room = rooms.get(ws.roomCode);
				if (!room || room.hostId !== playerId) return; // только хост банит
				const targetId = msg.player_id;
				if (!targetId || targetId === playerId) return; // нельзя забанить себя

				const target = room.players.get(targetId);
				if (!target) return;

				room.bannedIds.add(targetId);

				send(target.ws, { type: 'banned', reason: 'Вас забанил хост в этом лобби' });
				room.players.delete(targetId);
				target.ws.roomCode = null;
				target.ws.close();

				broadcastRoom(room, playerListPayload(room));
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
		leaveRoom(ws);
	});
});

function leaveRoom(ws) {
	const room = rooms.get(ws.roomCode);
	if (!room) return;

	room.players.delete(ws.playerId);

	if (room.players.size === 0) {
		rooms.delete(ws.roomCode);
		return;
	}

	if (room.hostId === ws.playerId) {
		room.hostId = [...room.players.keys()][0];
	}

	broadcastRoom(room, playerListPayload(room));
	ws.roomCode = null;
}

console.log(`Relay server listening on port ${PORT}`);
