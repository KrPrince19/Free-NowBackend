console.log("🔥 RUNNING FILE:", __filename);

require("dotenv").config();

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { MongoClient, ObjectId } = require("mongodb");
const cors = require("cors");
const cron = require("node-cron");

const SERVER_VERSION = "6.0-ULTIMATE";
console.log("🛠️ SERVER VERSION:", SERVER_VERSION);

// Server-side deduplication cache (ClientId -> Timestamp)
const serverSideClientCache = new Map();
// Cleanup cache every 1 minute
setInterval(() => {
  const now = Date.now();
  for (const [id, time] of serverSideClientCache.entries()) {
    if (now - time > 60000) serverSideClientCache.delete(id);
  }
}, 60000);

/* =======================
   SAFETY: GLOBAL ERRORS
======================= */
process.on("uncaughtException", (err) => {
  console.error("🔥 Uncaught Exception:", err);
});

process.on("unhandledRejection", (reason) => {
  console.error("🔥 Unhandled Rejection:", reason);
});

/* =======================
   ENV CHECK
======================= */
if (!process.env.MONGO_URI) {
  console.error("❌ MONGO_URI is missing in environment variables");
  process.exit(1);
}

const PORT = process.env.PORT || 5000;

/* =======================
   APP + SERVER
======================= */
const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

/* =======================
   MONGODB
======================= */
const client = new MongoClient(process.env.MONGO_URI, {
  connectTimeoutMS: 10000,
  serverSelectionTimeoutMS: 10000,
  socketTimeoutMS: 45000,
  family: 4 // Force IPv4 to avoid some DNS SRV resolution issues
});
let db;

async function initDB(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`⏳ Connecting to MongoDB Atlas (Attempt ${i + 1}/${retries})...`);
      await client.connect();
      db = client.db("freeNow");
      console.log("✅ MongoDB Connected");
      return true;
    } catch (err) {
      console.error(`❌ MongoDB Connection Attempt ${i + 1} Failed:`, err.message);
      if (i < retries - 1) {
        console.log("🔄 Retrying in 5 seconds...");
        await new Promise(resolve => setTimeout(resolve, 5000));
      } else {
        return false;
      }
    }
  }
}

/* =======================
   CRON JOB (MIDNIGHT IST)
======================= */
cron.schedule(
  "0 0 * * *",
  async () => {
    try {
      console.log("🕛 Midnight Cron Running");
      if (!db) return;

      // 1. Cleanup old activity logs (30 days)
      const thirtyDaysAgo = new Date();
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
      await db.collection("activitylogs").updateMany(
        { timestamp: { $lt: thirtyDaysAgo }, isActive: true },
        { $set: { isActive: false } }
      );

      // 2. Perform rotation
      await rotateDailyStatsIfNeeded();

      console.log("✅ Midnight rotation and cleanup done");
    } catch (err) {
      console.error("❌ Cron Error:", err);
    }
  },
  { timezone: "Asia/Kolkata" }
);

cron.schedule(
  "0 0 1 * *",
  async () => {
    try {
      console.log("🗓️ Monthly Reset Cron Running");
      if (!db) return;

      await db.collection("globalstats").updateOne(
        { type: "monthly" },
        { $set: { count: 0 } },
        { upsert: true }
      );

      io.emit("month-reset", { count: 0 });
      console.log("✅ Monthly stats reset to zero");
    } catch (err) {
      console.error("❌ Monthly Cron Error:", err);
    }
  },
  { timezone: "Asia/Kolkata" }
);

/* =======================
   HELPERS
======================= */
async function rotateDailyStatsIfNeeded() {
  if (!db) return;
  try {
    const stats = await db.collection("globalstats").findOne({ type: "daily_peak" });
    const now = new Date();
    // Using IST date string for comparison
    const todayStr = now.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });

    const lastRotation = stats?.lastRotationDate;

    if (lastRotation !== todayStr) {
      console.log(`🔄 Rotating stats: Last rotation was ${lastRotation || 'never'}, today is ${todayStr}`);

      const yesterdayData = stats?.today || { names: "Waiting for peak vibe...", durationMs: 0 };

      await db.collection("globalstats").updateOne(
        { type: "daily_peak" },
        {
          $set: {
            yesterday: yesterdayData,
            today: { names: "Waiting for peak vibe...", durationMs: 0 },
            lastRotationDate: todayStr
          }
        },
        { upsert: true }
      );

      io.emit("midnight-update", { message: "Stats rotated" });
      console.log("✅ Rotation completed successfully");
    } else {
      console.log("📅 Stats are already up to date for today.");
    }
  } catch (err) {
    console.error("❌ Error in rotateDailyStatsIfNeeded:", err);
  }
}

async function updateDailyPeak(names, durationMs) {
  if (!db || durationMs < 1000) return; // Ignore very short glitches

  try {
    const stats = await db.collection("globalstats").findOne({ type: "daily_peak" });
    const today = stats?.today || { names: "Waiting...", durationMs: 0 };

    if (durationMs > today.durationMs) {
      await db.collection("globalstats").updateOne(
        { type: "daily_peak" },
        {
          $set: {
            "today.names": names,
            "today.durationMs": durationMs,
            "today.updatedAt": new Date()
          }
        },
        { upsert: true }
      );
      console.log(`📈 New today peak: ${names} (${Math.floor(durationMs / 60000)}m)`);
    }
  } catch (err) {
    console.error("❌ updateDailyPeak Error:", err);
  }
}

async function broadcastActiveUsers() {
  if (!db) return;
  const users = await db
    .collection("activeusers")
    .find()
    .sort({ createdAt: -1 })
    .toArray();

  io.emit(
    "users-update",
    users.map((u) => ({
      id: u.sessionId,
      name: u.name,
      status: u.status,
      createdAt: u.createdAt
    }))
  );
}

/* =======================
   API ROUTES
======================= */
app.get("/", (req, res) => {
  res.send("🚀 Backend running successfully");
});

app.get("/api/activeusers", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  const users = await db.collection("activeusers").find().toArray();
  res.json(users.map(u => ({
    id: u.sessionId,
    name: u.name,
    status: u.status,
    createdAt: u.createdAt
  })));
});

app.post("/api/sync-user", async (req, res) => {
  const { sessionId, email, name } = req.body;
  if (!db) return res.status(500).json({ error: "DB not ready" });

  try {
    await db.collection("users").updateOne(
      { email },
      {
        $set: { sessionId, name, lastSeen: new Date() },
        $setOnInsert: {
          totalRequests: 0,
          matchesMade: 0,
          isFree: false
        }
      },
      { upsert: true }
    );

    res.json({ success: true });
    broadcastActiveUsers(); // Trigger refresh for admin and dashboard
    io.emit("new-user-registered", { name, email, sessionId });
  } catch (err) {
    console.error("❌ Sync User Route Error:", err);
    res.status(500).json({ error: "Failed to sync user" });
  }
});

app.get("/api/global-stats/monthly", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  const stats = await db.collection("globalstats").findOne({ type: "monthly" });
  res.json({ count: stats?.count || 0 });
});

const activeRooms = new Map(); // roomId -> { participants: [], names: {}, startTime: Date }
const activeVibeGames = new Map(); // roomId -> { round: 1, selections: { sessionId: emoji }, turnId: sessionId }

app.get("/api/active-conversations", async (req, res) => {
  const rooms = Array.from(activeRooms.values()).map(r => ({
    roomId: r.roomId,
    names: Object.values(r.names).join(" & "),
    startTime: r.startTime
  }));
  res.json(rooms);
});

app.get("/api/stats/longest-yesterday", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });

  try {
    const stats = await db.collection("globalstats").findOne({ type: "daily_peak" });
    const record = stats?.yesterday;

    if (record && record.durationMs > 0) {
      const mins = Math.floor(record.durationMs / 60000);
      const displayDuration = mins < 60 ? `${mins}m` : `${Math.floor(mins / 60)}h ${mins % 60}m`;
      res.json({ names: record.names, duration: displayDuration });
    } else {
      res.json({ names: "Ready for new legends...", duration: "0m" });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/user-stats/:email", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  const user = await db.collection("users").findOne({ email: req.params.email });
  res.json({
    totalRequests: user?.totalRequests || 0,
    matchesMade: user?.matchesMade || 0,
    isSuspended: user?.isSuspended || false,
    systemWarning: user?.systemWarning || null,
    needsUnsuspendAcknowledge: user?.needsUnsuspendAcknowledge || false
  });
});

app.post("/api/admin/users/:email/suspend", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    const user = await db.collection("users").findOne({ email: req.params.email });
    const newState = !user?.isSuspended;

    const updateDoc = { isSuspended: newState };
    if (!newState) {
      updateDoc.needsUnsuspendAcknowledge = true;
    } else {
      updateDoc.needsUnsuspendAcknowledge = false;
    }

    await db.collection("users").updateOne(
      { email: req.params.email },
      { $set: updateDoc }
    );

    // Instant suspension via Socket
    io.emit("admin-suspension", {
      email: req.params.email,
      isSuspended: newState,
      needsUnsuspendAcknowledge: !newState
    });

    console.log(`🛡️ Admin toggled suspension for ${req.params.email} to ${newState}`);
    res.json({ success: true, isSuspended: newState });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/user-stats/:email/acknowledge-unsuspend", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    await db.collection("users").updateOne(
      { email: req.params.email },
      { $set: { needsUnsuspendAcknowledge: false } }
    );
    console.log(`✅ User ${req.params.email} acknowledged unsuspend warning`);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/admin/users/:email/warn", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  const { message } = req.body;
  try {
    await db.collection("users").updateOne(
      { email: req.params.email },
      { $set: { systemWarning: message } }
    );

    // Emit instant warning if user is online
    io.emit("admin-warning", {
      email: req.params.email,
      message
    });

    console.log(`⚠️ Admin sent warning to ${req.params.email}`);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/user-stats/:email/clear-warning", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    await db.collection("users").updateOne(
      { email: req.params.email },
      { $set: { systemWarning: null } }
    );
    console.log(`✅ User ${req.params.email} dismissed warning`);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/admin/users/:email/reset-stats", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    await db.collection("users").updateOne(
      { email: req.params.email },
      { $set: { totalRequests: 0, matchesMade: 0 } }
    );
    console.log(`📊 Admin reset stats for ${req.params.email}`);
    res.json({ success: true });
    io.emit("admin-stats-reset", { email: req.params.email });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/feedback", async (req, res) => {
  const { name, email, message } = req.body;
  if (!db) return res.status(500).json({ error: "DB not ready" });

  try {
    const feedbackDoc = {
      name,
      email,
      message,
      timestamp: new Date()
    };
    const result = await db.collection("feedback").insertOne(feedbackDoc);
    const feedbackWithId = { ...feedbackDoc, _id: result.insertedId.toString() };

    console.log(`📩 New feedback received from ${name} (${email})`);

    // Broadcast to Admin Dashboard
    const connectedClients = io.sockets.sockets.size;
    console.log(`📡 Broadcasting 'new-feedback' to ${connectedClients} connected clients`);

    io.emit("new-feedback", feedbackWithId);

    res.json({ success: true });
  } catch (err) {
    console.error("❌ Feedback Save Error:", err);
    res.status(500).json({ error: "Failed to save feedback" });
  }
});

app.get("/api/history/:email", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  const logs = await db.collection("activitylogs")
    .find({ userEmail: req.params.email })
    .sort({ timestamp: -1 })
    .limit(10)
    .toArray();
  res.json(logs);
});

/* =======================
   ADMIN API (SECURE)
======================= */
app.get("/api/admin/users", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    const users = await db.collection("users").find({}).sort({ totalRequests: -1 }).toArray();
    res.json(users);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/admin/users/:email", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    await db.collection("users").deleteOne({ email: req.params.email });
    // Also cleanup logs if necessary
    await db.collection("activitylogs").deleteMany({ userEmail: req.params.email });
    console.log(`🗑️ Admin deleted user: ${req.params.email}`);
    res.json({ success: true });
    io.emit("admin-user-deleted", { email: req.params.email });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/admin/feedback", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    const feedback = await db.collection("feedback").find({}).sort({ timestamp: -1 }).toArray();
    res.json(feedback);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/admin/feedback/:id", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    const { id } = req.params;
    await db.collection("feedback").deleteOne({ _id: new ObjectId(id) });
    console.log(`🗑️ Admin deleted feedback ID: ${id}`);

    // Broadcast deletion to all admin instances
    io.emit("feedback-deleted", { id: id.toString() });

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* =======================
   SOCKET.IO
======================= */
const requestTimers = new Map();
const userSockets = new Map(); // sessionId -> socketId
const userRooms = new Map(); // sessionId -> { roomId, partnerName }
const pendingDisconnects = new Map(); // sessionId -> Timeout

io.on("connection", (socket) => {
  console.log(`🔌 New Socket Connection: ${socket.id} | Origin: ${socket.handshake.headers.origin}`);
  console.log(`📡 Total Connected Clients: ${io.engine.clientsCount}`);

  // Track room joins for admin debugging
  socket.onAny((event, ...args) => {
    console.log(`📡 EVENT: ${event}`, args);
  });

  broadcastActiveUsers();

  socket.on("register-user", (sessionId) => {
    // If user already has a socket, disconnect the old one to prevent duplicates
    const oldSocketId = userSockets.get(sessionId);
    if (oldSocketId && oldSocketId !== socket.id) {
      console.log(`🔄 Session takeover for ${sessionId}: Disconnecting old socket ${oldSocketId}`);
      const oldSocket = io.sockets.sockets.get(oldSocketId);
      if (oldSocket) {
        oldSocket.emit("force-disconnect", "New session established elsewhere");
        oldSocket.disconnect(true);
      }
    }

    socket.sessionId = sessionId;
    userSockets.set(sessionId, socket.id);
    console.log(`✅ User registered: ${sessionId} -> ${socket.id}`);

    // Session Recovery & Grace Period Cleanup
    if (pendingDisconnects.has(sessionId)) {
      console.log(`♻️ Session recovered for ${sessionId}`);
      clearTimeout(pendingDisconnects.get(sessionId));
      pendingDisconnects.delete(sessionId);
    }

    if (userRooms.has(sessionId)) {
      const { roomId, partnerName } = userRooms.get(sessionId);
      socket.join(roomId);
      socket.currentRoom = roomId;
      socket.senderName = partnerName; // This is actually the user's name as saved on their socket
      console.log(`🔗 ${sessionId} rejoined room ${roomId}`);
    }
  });

  socket.on("send-chat-request", async ({ senderId, senderName, receiverId, receiverName, senderVibe }) => {
    const receiverSocketId = userSockets.get(receiverId);
    console.log(`📤 Chat request from ${senderName} (${senderId}) to ${receiverName || 'someone'} (${receiverId})`);

    // Track stats
    if (db) {
      try {
        const sender = await db.collection("users").findOneAndUpdate(
          { sessionId: senderId },
          { $inc: { totalRequests: 1 } }
        );

        const userDoc = sender?.value || (sender?.ok ? sender : null);
        if (userDoc && userDoc.email) {
          await db.collection("activitylogs").insertOne({
            userEmail: userDoc.email,
            type: 'REQUEST_SENT',
            detail: `Sent vibe check to ${receiverName || receiverId} (${senderVibe || 'free'})`,
            partnerName: receiverName,
            vibe: senderVibe,
            timestamp: new Date()
          });
        } else {
          console.log(`⚠️ Sender ${senderId} not found in users collection for logging`);
        }
      } catch (err) {
        console.error("❌ Error tracking stats for request:", err.message);
      }
    }

    if (receiverSocketId) {
      io.to(receiverSocketId).emit("receive-chat-request", { senderId, senderName });
      socket.emit("request-sent-success");
      console.log(`✅ Request delivered to ${receiverId}`);
    } else {
      console.log(`❌ Receiver ${receiverId} not found in active sockets`);
      socket.emit("request-failed", { message: "User is no longer online (stale session)" });
    }
  });

  socket.on("accept-chat", async ({ senderId, senderName, receiverId, receiverName, receiverVibe }) => {
    const senderSocketId = userSockets.get(senderId);
    if (senderSocketId) {
      const roomId = `room-${senderId}-${receiverId}`;
      const chatData = { roomId, members: [senderId, receiverId], startTime: new Date() };

      socket.join(roomId);
      const senderSocket = io.sockets.sockets.get(senderSocketId);
      if (senderSocket) senderSocket.join(roomId);

      // Track stats and activity
      if (db) {
        await db.collection("users").updateMany(
          { sessionId: { $in: [senderId, receiverId] } },
          { $inc: { matchesMade: 1 } }
        );
        await db.collection("globalstats").updateOne(
          { type: "monthly" },
          { $inc: { count: 1 } },
          { upsert: true }
        );

        const users = await db.collection("users").find({ sessionId: { $in: [senderId, receiverId] } }).toArray();
        for (const u of users) {
          const isSender = u.sessionId === senderId;
          const partnerName = isSender ? receiverName : senderName;
          const partnerVibe = isSender ? receiverVibe : (users.find(x => x.sessionId === senderId)?.status || "free");

          await db.collection("activitylogs").insertOne({
            userEmail: u.email,
            type: 'CONVERSATION',
            participants: [senderId, receiverId],
            participantNames: { [senderId]: senderName, [receiverId]: receiverName },
            partnerName: partnerName,
            vibe: partnerVibe,
            detail: `Connection with ${partnerName} (${partnerVibe})`,
            timestamp: new Date()
          });
        }
      }

      activeRooms.set(roomId, {
        roomId,
        names: { [senderId]: senderName, [receiverId]: receiverName },
        startTime: new Date()
      });

      // Persistent session data
      userRooms.set(senderId, { roomId, partnerName: senderName });
      userRooms.set(receiverId, { roomId, partnerName: receiverName });

      // Remove from active users once connected (they are no longer "available" or "online" in the general pool)
      if (db) {
        await db.collection("activeusers").deleteMany({ sessionId: { $in: [senderId, receiverId] } });
        await db.collection("users").updateMany(
          { sessionId: { $in: [senderId, receiverId] } },
          { $set: { isFree: false } }
        );
        broadcastActiveUsers();
      }

      // Attach data for lifecycle
      socket.currentRoom = roomId;
      socket.senderName = receiverName; // On receiver's socket, "senderName" is themselves
      if (senderSocket) {
        senderSocket.currentRoom = roomId;
        senderSocket.senderName = senderName;
      }

      try {
        io.to(senderSocketId).emit("chat-started", { ...chatData, partnerName: receiverName });
        socket.emit("chat-init-receiver", { ...chatData, partnerName: senderName });

        io.emit("conversation-started", { roomId });
        console.log(`🤝 Chat started between ${senderName} and ${receiverName} in room ${roomId}`);
      } catch (err) {
        console.error("❌ socket.emit error in accept-chat:", err);
      }
    }
  });

  socket.on("typing", ({ roomId, senderName }) => {
    socket.to(roomId).emit("partner-typing", { senderName });
  });

  socket.on("stop-typing", ({ roomId }) => {
    socket.to(roomId).emit("partner-stop-typing");
  });

  socket.on("send-private-message", ({ roomId, message, senderName, type, clientId }) => {
    // 1.5 ROOM VALIDATION
    if (!activeRooms.has(roomId)) {
      console.warn(`⚠️ [V5:BYPASS] Message attempted for non-existent room: ${roomId}`);
      return;
    }

    // 1. SERVER-SIDE DEDUPLICATION
    if (clientId) {
      if (serverSideClientCache.has(clientId)) {
        console.log(`⏭️ [V5:DUP] Server-side duplicate clientId ignored: ${clientId}`);
        return;
      }
      serverSideClientCache.set(clientId, Date.now());
    }

    const timestamp = new Date();
    const msg = {
      id: `${timestamp.getTime()}-${Math.random().toString(36).substr(2, 5)}`,
      roomId: roomId,
      clientId: clientId,
      text: message,
      sender: senderName,
      type: type || 'text',
      timestamp: timestamp.toISOString()
    };
    console.log(`💬 [V:${SERVER_VERSION}][Room ${roomId}] Msg from ${senderName}: ${type || 'text'} (ID: ${msg.id}, ClientId: ${clientId || 'UNDEFINED'})`);
    io.to(roomId).emit("new-message", msg);
  });

  socket.on("edit-message", ({ roomId, messageId, newText }) => {
    io.to(roomId).emit("message-updated", { messageId, newText });
  });

  socket.on("delete-message", ({ roomId, messageId }) => {
    io.to(roomId).emit("message-deleted", { messageId });
  });

  socket.on("end-chat", async ({ roomId, senderName }) => {
    const roomInfo = activeRooms.get(roomId);
    if (roomInfo) {
      const durationMs = new Date() - roomInfo.startTime;
      if (db) {
        const names = Object.values(roomInfo.names).join(" & ");
        await updateDailyPeak(names, durationMs);

        await db.collection("connections").insertOne({
          names,
          durationMs,
          endTime: new Date()
        });
      }
    }

    socket.to(roomId).emit("partner-left", { senderName });
    socket.leave(roomId);
    activeRooms.delete(roomId);

    // Cleanup persistent state
    for (const [sid, roomInfo] of userRooms.entries()) {
      if (roomInfo.roomId === roomId) userRooms.delete(sid);
    }

    io.emit("conversation-ended", { roomId });
    socket.currentRoom = null;
  });

  socket.on("reject-chat", ({ senderId, receiverId }) => {
    const senderSocketId = userSockets.get(senderId);
    if (senderSocketId) {
      io.to(senderSocketId).emit("request-rejected", { message: "Request declined" });
    }
  });

  socket.on("vibe-game-toggle", ({ roomId, isOpen }) => {
    io.to(roomId).emit("vibe-game-status", { isOpen });
    if (!isOpen) {
      activeVibeGames.delete(roomId);
      return;
    }

    if (isOpen && !activeVibeGames.has(roomId)) {
      // Initialize game
      const room = activeRooms.get(roomId);
      if (room) {
        const participantIds = Object.keys(room.names);
        activeVibeGames.set(roomId, {
          round: 1,
          selections: {},
          turnId: participantIds[0],
          participantIds
        });
      }
    }

    // Always emit current state to anyone who opens the dashboard
    if (activeVibeGames.has(roomId)) {
      io.to(roomId).emit("vibe-game-state", activeVibeGames.get(roomId));
    }
  });

  socket.on("vibe-emoji-select", ({ roomId, sessionId, emoji }) => {
    const game = activeVibeGames.get(roomId);
    if (!game) return;

    const sId = socket.sessionId || sessionId; // Use socket's own session if possible
    game.selections[sId] = emoji;

    // Broadcast that someone selected (but not what they selected)
    io.to(roomId).emit("vibe-partner-selected", { sessionId: sId });

    const keys = Object.keys(game.selections);
    if (keys.length === 2) {
      // Both selected!
      const isMatch = game.selections[keys[0]] === game.selections[keys[1]];
      io.to(roomId).emit("vibe-round-result", {
        selections: game.selections,
        isMatch,
        round: game.round
      });

      // Prepare next round
      game.round += 1;
      game.selections = {};
      // Flip turn
      game.turnId = game.turnId === game.participantIds[0] ? game.participantIds[1] : game.participantIds[0];

      setTimeout(() => {
        if (game.round > 5) {
          io.to(roomId).emit("vibe-game-status", { isOpen: false, finished: true });
          activeVibeGames.delete(roomId);
        } else {
          io.to(roomId).emit("vibe-game-state", game);
        }
      }, 3000); // 3 second delay to show result
    }
  });

  socket.on("vibe-game-reset", ({ roomId }) => {
    activeVibeGames.delete(roomId);
  });

  socket.on("draw-start", ({ roomId, x, y, color }) => {
    socket.to(roomId).emit("draw-partner-start", { x, y, color });
  });

  socket.on("draw-toggle", ({ roomId, isOpen }) => {
    io.to(roomId).emit("draw-room-toggle", { isOpen });
  });

  socket.on("draw-move", ({ roomId, x, y }) => {
    socket.to(roomId).emit("draw-partner-move", { x, y });
  });

  socket.on("draw-clear", ({ roomId }) => {
    io.to(roomId).emit("draw-room-clear");
  });

  socket.on("vibe-reaction", ({ roomId, messageId, emoji, x, y }) => {
    console.log(`💓 [SERVER:REACTION] Room ${roomId} | Msg ${messageId} | Icon ${emoji}`);
    io.to(roomId).emit("message-reaction-ribbon", { messageId, emoji, x, y });
  });

  socket.on("go-free", async ({ id, name, status }) => {
    if (!db) return;

    await db.collection("users").updateOne(
      { sessionId: id },
      { $set: { isFree: true, socketId: socket.id, status } }
    );

    await db.collection("activeusers").updateOne(
      { sessionId: id },
      {
        $set: {
          sessionId: id,
          name,
          status,
          socketId: socket.id,
          createdAt: new Date()
        }
      },
      { upsert: true }
    );

    broadcastActiveUsers();
  });

  socket.on("go-busy", async ({ id }) => {
    if (!db) return;

    await db.collection("users").updateOne(
      { sessionId: id },
      { $set: { isFree: false } }
    );

    await db.collection("activeusers").deleteOne({ sessionId: id });
    broadcastActiveUsers();
  });

  socket.on("disconnect", async () => {
    console.log("❌ Socket disconnected:", socket.id);
    const sessionId = socket.sessionId;

    if (!sessionId) return;

    // Grace Period for ALL Disconnections (5 seconds)
    // Prevents vanishing on page refresh
    console.log(`⏳ Starting 5s grace period for ${sessionId}`);

    const timeout = setTimeout(async () => {
      console.log(`🚨 Grace period expired for ${sessionId}`);

      // Handle room cleanup if they were in a chat
      if (socket.currentRoom) {
        const roomInfo = activeRooms.get(socket.currentRoom);
        if (roomInfo) {
          const durationMs = new Date() - roomInfo.startTime;
          if (db) {
            const names = Object.values(roomInfo.names).join(" & ");
            await updateDailyPeak(names, durationMs);
            await db.collection("connections").insertOne({
              names,
              durationMs,
              endTime: new Date()
            });
          }
        }
        socket.to(socket.currentRoom).emit("partner-left", { senderName: socket.senderName });
        activeRooms.delete(socket.currentRoom);
        io.emit("conversation-ended", { roomId: socket.currentRoom });
      }

      // Final cleanup
      userSockets.delete(sessionId);
      userRooms.delete(sessionId);
      pendingDisconnects.delete(sessionId);

      if (db) {
        await db.collection("users").updateOne({ sessionId }, { $set: { isFree: false } });
        await db.collection("activeusers").deleteOne({ sessionId });
        broadcastActiveUsers();
      }
    }, 5000);

    pendingDisconnects.set(sessionId, timeout);
  });
});

/* =======================
   START SERVER
======================= */
initDB().then(async (success) => {
  if (!success) {
    console.error("🛑 Server stopped (DB required)");
    process.exit(1);
  }

  // Perform startup checks/rotations
  await rotateDailyStatsIfNeeded();

  // 🧹 CLEAR STALE ACTIVE USERS ON STARTUP
  try {
    const result = await db.collection("activeusers").deleteMany({});
    console.log(`🧹 Cleaned up ${result.deletedCount} stale active user records`);
  } catch (err) {
    console.error("❌ Failed to cleanup activeusers on startup:", err);
  }

  server.listen(PORT, "0.0.0.0", () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
  });
});
