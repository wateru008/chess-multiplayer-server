const WebSocket = require("ws");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;

const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

const server = new WebSocket.Server({
  port: PORT
});

const waitingPlayers = [];
const matches = new Map();

console.log(`Chess multiplayer server running on port ${PORT}`);

async function initializeDatabase() {
  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS players (
        id SERIAL PRIMARY KEY,
        game_name VARCHAR(50) UNIQUE NOT NULL,
        wins INTEGER NOT NULL DEFAULT 0,
        losses INTEGER NOT NULL DEFAULT 0,
        draws INTEGER NOT NULL DEFAULT 0,
        games_played INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    console.log("PostgreSQL database initialized.");
  } catch (error) {
    console.error("Database initialization failed:", error);
  }
}

initializeDatabase();

function send(player, message) {
  if (player.readyState === WebSocket.OPEN) {
    player.send(JSON.stringify(message));
  }
}

function createMatch(player1, player2) {
  const matchId = `match_${Date.now()}_${Math.random()
    .toString(36)
    .slice(2, 8)}`;

  const white = Math.random() < 0.5 ? player1 : player2;
  const black = white === player1 ? player2 : player1;

  const match = {
    id: matchId,
    white,
    black,
    state: "active"
  };

  matches.set(matchId, match);

  white.matchId = matchId;
  black.matchId = matchId;

  white.color = "white";
  black.color = "black";

  send(white, {
    type: "MATCH_FOUND",
    matchId,
    color: "white"
  });

  send(black, {
    type: "MATCH_FOUND",
    matchId,
    color: "black"
  });

  send(white, {
    type: "MATCH_START",
    matchId,
    color: "white"
  });

  send(black, {
    type: "MATCH_START",
    matchId,
    color: "black"
  });

  console.log(`Match created: ${matchId}`);
}

function findOpponent(player) {
  if (waitingPlayers.length === 0) {
    waitingPlayers.push(player);

    send(player, {
      type: "FINDING_OPPONENT"
    });

    console.log("Player waiting for opponent.");
    return;
  }

  const opponent = waitingPlayers.shift();

  if (!opponent || opponent.readyState !== WebSocket.OPEN) {
    findOpponent(player);
    return;
  }

  createMatch(opponent, player);
}

server.on("connection", (socket) => {
  console.log("Player connected.");

  socket.matchId = null;
  socket.color = null;
  socket.gameName = null;

  send(socket, {
    type: "CONNECTED"
  });

  socket.on("message", (data) => {
    let message;

    try {
      message = JSON.parse(data.toString());
    } catch {
      send(socket, {
        type: "ERROR",
        message: "Invalid message format."
      });
      return;
    }

    if (message.type === "FIND_MATCH") {
      if (message.gameName) {
        socket.gameName = String(message.gameName).trim().slice(0, 50);
      }

      findOpponent(socket);
      return;
    }

    if (message.type === "PING") {
      send(socket, {
        type: "PONG"
      });
      return;
    }

    if (!socket.matchId) {
      send(socket, {
        type: "ERROR",
        message: "Player is not currently in a match."
      });
      return;
    }

    const match = matches.get(socket.matchId);

    if (!match) {
      send(socket, {
        type: "ERROR",
        message: "Match not found."
      });
      return;
    }

    let opponent;

    if (socket === match.white) {
      opponent = match.black;
    } else {
      opponent = match.white;
    }

    if (message.type === "MOVE_MADE") {
      send(opponent, {
        type: "MOVE_MADE",
        matchId: socket.matchId,
        move: message.move
      });
      return;
    }

    if (message.type === "GAME_EVENT") {
      send(opponent, {
        type: "GAME_EVENT",
        matchId: socket.matchId,
        event: message.event,
        data: message.data
      });
      return;
    }
  });

  socket.on("close", () => {
    console.log("Player disconnected.");

    const waitingIndex = waitingPlayers.indexOf(socket);

    if (waitingIndex !== -1) {
      waitingPlayers.splice(waitingIndex, 1);
    }

    if (!socket.matchId) {
      return;
    }

    const match = matches.get(socket.matchId);

    if (!match) {
      return;
    }

    const opponent =
      socket === match.white
        ? match.black
        : match.white;

    send(opponent, {
      type: "OPPONENT_DISCONNECTED",
      matchId: socket.matchId
    });

    matches.delete(socket.matchId);
  });
});
