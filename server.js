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

// =====================================================
// DATABASE INITIALIZATION
// =====================================================

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

// =====================================================
// PLAYER DATABASE FUNCTIONS
// =====================================================

async function getOrCreatePlayer(gameName) {
  if (!gameName) {
    return null;
  }

  const name = String(gameName)
    .trim()
    .slice(0, 50);

  if (!name) {
    return null;
  }

  try {
    const result = await db.query(
      `
      INSERT INTO players (game_name)
      VALUES ($1)
      ON CONFLICT (game_name)
      DO UPDATE SET game_name = EXCLUDED.game_name
      RETURNING *
      `,
      [name]
    );

    return result.rows[0];
  } catch (error) {
    console.error("Player database error:", error);
    return null;
  }
}

async function recordGameResult(winner, loser, draw = false) {
  try {
    if (!winner || !loser) {
      console.error(
        "Cannot record result: missing player name."
      );
      return;
    }

    // Make sure both players exist
    await Promise.all([
      getOrCreatePlayer(winner),
      getOrCreatePlayer(loser)
    ]);

    if (draw) {
      await Promise.all([
        db.query(
          `
          UPDATE players
          SET draws = draws + 1,
              games_played = games_played + 1
          WHERE game_name = $1
          `,
          [winner]
        ),

        db.query(
          `
          UPDATE players
          SET draws = draws + 1,
              games_played = games_played + 1
          WHERE game_name = $1
          `,
          [loser]
        )
      ]);

      console.log(
        `Draw recorded: ${winner} vs ${loser}`
      );

      return;
    }

    await Promise.all([
      db.query(
        `
        UPDATE players
        SET wins = wins + 1,
            games_played = games_played + 1
        WHERE game_name = $1
        `,
        [winner]
      ),

      db.query(
        `
        UPDATE players
        SET losses = losses + 1,
            games_played = games_played + 1
        WHERE game_name = $1
        `,
        [loser]
      )
    ]);

    console.log(
      `Result recorded: ${winner} won against ${loser}`
    );
  } catch (error) {
    console.error(
      "Failed to record game result:",
      error
    );
  }
}

// =====================================================
// WEBSOCKET HELPERS
// =====================================================

function send(player, message) {
  if (
    player &&
    player.readyState === WebSocket.OPEN
  ) {
    player.send(JSON.stringify(message));
  }
}

// =====================================================
// MATCH CREATION
// =====================================================

function createMatch(player1, player2) {
  const matchId =
    `match_${Date.now()}_${Math.random()
      .toString(36)
      .slice(2, 8)}`;

  const white =
    Math.random() < 0.5
      ? player1
      : player2;

  const black =
    white === player1
      ? player2
      : player1;

  const match = {
    id: matchId,
    white,
    black,
    state: "active",
    resultRecorded: false
  };

  matches.set(matchId, match);

  white.matchId = matchId;
  black.matchId = matchId;

  white.color = "white";
  black.color = "black";

  const whiteName =
    white.gameName || "White";

  const blackName =
    black.gameName || "Black";

  // Make sure both players exist in database
  getOrCreatePlayer(whiteName);
  getOrCreatePlayer(blackName);

  send(white, {
    type: "MATCH_FOUND",
    matchId,
    color: "white",
    playerName: whiteName,
    opponentName: blackName,
    whiteName,
    blackName
  });

  send(black, {
    type: "MATCH_FOUND",
    matchId,
    color: "black",
    playerName: blackName,
    opponentName: whiteName,
    whiteName,
    blackName
  });

  send(white, {
    type: "MATCH_START",
    matchId,
    color: "white",
    playerName: whiteName,
    opponentName: blackName,
    whiteName,
    blackName
  });

  send(black, {
    type: "MATCH_START",
    matchId,
    color: "black",
    playerName: blackName,
    opponentName: whiteName,
    whiteName,
    blackName
  });

  console.log(
    `Match created: ${matchId} | ${whiteName} vs ${blackName}`
  );
}

// =====================================================
// MATCHMAKING
// =====================================================

function findOpponent(player) {
  if (waitingPlayers.length === 0) {
    waitingPlayers.push(player);

    send(player, {
      type: "FINDING_OPPONENT"
    });

    console.log("Player waiting for opponent.");
    return;
  }

  const opponent =
    waitingPlayers.shift();

  if (
    !opponent ||
    opponent.readyState !== WebSocket.OPEN
  ) {
    findOpponent(player);
    return;
  }

  createMatch(opponent, player);
}

// =====================================================
// WEBSOCKET CONNECTION
// =====================================================

server.on("connection", (socket) => {
  console.log("Player connected.");

  socket.matchId = null;
  socket.color = null;
  socket.gameName = null;

  send(socket, {
    type: "CONNECTED"
  });

  // ===================================================
  // MESSAGE HANDLER
  // ===================================================

  socket.on("message", async (data) => {
    let message;

    try {
      message = JSON.parse(
        data.toString()
      );
    } catch {
      send(socket, {
        type: "ERROR",
        message: "Invalid message format."
      });

      return;
    }

    // =================================================
    // FIND MATCH
    // =================================================

    if (message.type === "FIND_MATCH") {
      if (message.gameName) {
        socket.gameName =
          String(message.gameName)
            .trim()
            .slice(0, 50);

        await getOrCreatePlayer(
          socket.gameName
        );
      }

      findOpponent(socket);
      return;
    }

    // =================================================
    // PING
    // =================================================

    if (message.type === "PING") {
      send(socket, {
        type: "PONG"
      });

      return;
    }

    // =================================================
    // LEADERBOARD
    // =================================================

    if (message.type === "GET_LEADERBOARD") {
      try {
        const result = await db.query(`
          SELECT
            game_name,
            wins,
            losses,
            draws,
            games_played
          FROM players
          ORDER BY wins DESC, games_played DESC
          LIMIT 50
        `);

        send(socket, {
          type: "LEADERBOARD",
          players: result.rows
        });
      } catch (error) {
        console.error(
          "Leaderboard database error:",
          error
        );

        send(socket, {
          type: "ERROR",
          message: "Failed to load leaderboard."
        });
      }

      return;
    }

    // =================================================
    // PLAYER STATS
    // =================================================

    if (message.type === "GET_PLAYER_STATS") {
      const requestedName =
        message.gameName ||
        socket.gameName;

      if (!requestedName) {
        send(socket, {
          type: "PLAYER_STATS",
          player: null
        });

        return;
      }

      try {
        const result = await db.query(
          `
          SELECT
            game_name,
            wins,
            losses,
            draws,
            games_played
          FROM players
          WHERE game_name = $1
          `,
          [
            String(requestedName)
              .trim()
              .slice(0, 50)
          ]
        );

        send(socket, {
          type: "PLAYER_STATS",
          player:
            result.rows[0] || null
        });
      } catch (error) {
        console.error(
          "Player stats database error:",
          error
        );

        send(socket, {
          type: "ERROR",
          message: "Failed to load player stats."
        });
      }

      return;
    }

    // =================================================
    // MATCH VALIDATION
    // =================================================

    if (!socket.matchId) {
      send(socket, {
        type: "ERROR",
        message:
          "Player is not currently in a match."
      });

      return;
    }

    const match =
      matches.get(socket.matchId);

    if (!match) {
      send(socket, {
        type: "ERROR",
        message: "Match not found."
      });

      return;
    }

    const opponent =
      socket === match.white
        ? match.black
        : match.white;

    // =================================================
    // MOVE MADE
    // =================================================

    if (message.type === "MOVE_MADE") {
      send(opponent, {
        type: "MOVE_MADE",
        matchId: socket.matchId,
        move: message.move
      });

      return;
    }

    // =================================================
    // GAME EVENT
    // =================================================

    if (message.type === "GAME_EVENT") {
      const event =
        String(message.event || "")
          .toUpperCase();

      const resultEvents = [
        "RESIGN",
        "RESIGNED",
        "CHECKMATE",
        "STALEMATE",
        "DRAW",
        "DRAW_AGREED"
      ];

      if (
        resultEvents.includes(event) &&
        !match.resultRecorded
      ) {
        match.resultRecorded = true;
        match.state = "finished";

        const playerName =
          socket.gameName;

        const opponentName =
          opponent.gameName;

        // ---------------------------------------------
        // RESIGN
        // The player who resigns loses.
        // ---------------------------------------------

        if (
          event === "RESIGN" ||
          event === "RESIGNED"
        ) {
          await recordGameResult(
            opponentName,
            playerName,
            false
          );
        }

        // ---------------------------------------------
        // DRAW / STALEMATE
        // ---------------------------------------------

        else if (
          event === "DRAW" ||
          event === "DRAW_AGREED" ||
          event === "STALEMATE"
        ) {
          await recordGameResult(
            playerName,
            opponentName,
            true
          );
        }

        // ---------------------------------------------
        // CHECKMATE
        //
        // If the client provides data.winner,
        // use that. Otherwise the reporting player
        // is treated as the winner.
        // ---------------------------------------------

        else if (event === "CHECKMATE") {
          const winnerColor =
            message.data &&
            typeof message.data === "object"
              ? message.data.winner
              : null;

          if (
            winnerColor === "white" &&
            match.white.gameName &&
            match.black.gameName
          ) {
            await recordGameResult(
              match.white.gameName,
              match.black.gameName,
              false
            );
          } else if (
            winnerColor === "black" &&
            match.white.gameName &&
            match.black.gameName
          ) {
            await recordGameResult(
              match.black.gameName,
              match.white.gameName,
              false
            );
          } else {
            await recordGameResult(
              playerName,
              opponentName,
              false
            );
          }
        }
      }

      // Always relay the event to opponent
      send(opponent, {
        type: "GAME_EVENT",
        matchId: socket.matchId,
        event: message.event,
        data: message.data
      });

      return;
    }
  });

  // ===================================================
  // DISCONNECT
  // ===================================================

  socket.on("close", () => {
    console.log("Player disconnected.");

    const waitingIndex =
      waitingPlayers.indexOf(socket);

    if (waitingIndex !== -1) {
      waitingPlayers.splice(
        waitingIndex,
        1
      );
    }

    if (!socket.matchId) {
      return;
    }

    const match =
      matches.get(socket.matchId);

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

// =====================================================
// SERVER ERROR HANDLING
// =====================================================

server.on("error", (error) => {
  console.error(
    "WebSocket server error:",
    error
  );
});

console.log("Chess multiplayer server started.");
