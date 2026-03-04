console.log("🔥 RUNNING FILE:", __filename);

const dotenvResult = require("dotenv").config();
if (dotenvResult.error) {
  console.error("❌ Dotenv Error:", dotenvResult.error);
} else {
  console.log("✅ Dotenv Loaded Successfully");
}

// Razorpay debug removed
console.log("📡 DEBUG - MONGO_URI exists:", !!process.env.MONGO_URI);

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { MongoClient, ObjectId } = require("mongodb");
const cors = require("cors");
const cron = require("node-cron");
const crypto = require("crypto");

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
let globalConfig = { pingLimit: 5, toggleLimit: 3 };

// DB initialization logic

async function initDB(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      console.log(`⏳ Connecting to MongoDB Atlas (Attempt ${i + 1}/${retries})...`);
      await client.connect();
      db = client.db("freeNow");
      console.log("✅ MongoDB Connected");

      // 🛡️ GLOBAL CONFIG: Initialize default settings if they don't exist
      const config = await db.collection("appConfig").findOne({ type: "global" });
      if (!config) {
        console.log("📝 Initializing global app configuration...");
        const defaultConfig = {
          type: "global",
          pingLimit: 5,
          toggleLimit: 3,
          updatedAt: new Date()
        };
        await db.collection("appConfig").insertOne(defaultConfig);
        globalConfig = { pingLimit: defaultConfig.pingLimit, toggleLimit: defaultConfig.toggleLimit };
      } else {
        globalConfig = { pingLimit: config.pingLimit, toggleLimit: config.toggleLimit };
      }
      console.log("📝 Global Config Loaded:", globalConfig);

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
);

// ♻️ MONETIZATION: Dedicated Midnight Reset for User Usage Counters
cron.schedule(
  "0 0 * * *",
  async () => {
    try {
      console.log("🕛 [CRON] Global Midnight Usage Reset Running...");
      if (!db) return;

      // Reset all user counts to 0.
      const result = await db.collection("users").updateMany(
        {},
        { $set: { requestsToday: 0, goFreeToday: 0 } }
      );

      console.log(`✅ [CRON] Reset counters for ${result.modifiedCount} users.`);

      // Update last reset date in config
      const now = new Date();
      const todayStr = now.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
      await db.collection("appConfig").updateOne(
        { type: "global" },
        { $set: { lastGlobalResetDate: todayStr, updatedAt: new Date() } }
      );

      // Notify all connected clients to trigger a fresh usage recovery
      io.emit("midnight-vibe-reset");

      console.log("✅ [CRON] Midnight Usage Reset Completed Successfully");
    } catch (err) {
      console.error("❌ [CRON] Midnight Reset Error:", err);
    }
  },
  { timezone: "Asia/Kolkata" }
);

/* =======================
   MONTHLY RESET CRON (1st of every Month IST)
======================= */
cron.schedule(
  "0 0 1 * *",
  async () => {
    try {
      console.log("📅 [CRON] Monthly Connection Reset Running...");
      if (!db) return;

      // 1. Reset the monthly connection counter in Global Stats
      await db.collection("globalstats").updateOne(
        { type: "monthly" },
        { $set: { count: 0, updatedAt: new Date() } },
        { upsert: true }
      );

      // 2. Update config to track that this month's reset was done
      const now = new Date();
      const currentMonth = now.getMonth(); // 0-11
      const currentYear = now.getFullYear();
      const monthKey = `${currentYear}-${currentMonth + 1}`; // e.g. "2024-3"

      await db.collection("appConfig").updateOne(
        { type: "global" },
        { $set: { lastMonthlyReset: monthKey, updatedAt: new Date() } }
      );

      // 3. Notify frontend to refresh counters (reset to 0 visually)
      io.emit("month-reset", { message: "New month started! Stats cleared." });

      console.log(`✅ [CRON] Monthly Reset Completed for ${monthKey}`);
    } catch (err) {
      console.error("❌ [CRON] Monthly Reset Error:", err);
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
      gender: u.gender || "none", // 🚻 GENDER: filtering
      createdAt: u.createdAt
    }))
  );
}

// Helper to send current usage stats to ALL sessions/tabs of a specific user
async function sendUsageUpdate(sessionId, socket) {
  if (!db) return;
  try {
    const user = await db.collection("users").findOne({ sessionId });
    if (user) {
      const today = new Date().toDateString();
      const requestsToday = user.lastRequestDate === today ? (user.requestsToday || 0) : 0;
      const goFreeToday = user.lastGoFreeDate === today ? (user.goFreeToday || 0) : 0;

      console.log(`📊 [USAGE-STATS] Session: ${sessionId} | Raw DB Requests: ${user.requestsToday} | Raw DB Toggles: ${user.goFreeToday} | LastPing: ${user.lastRequestDate} | LastFree: ${user.lastGoFreeDate} | Today: ${today}`);
      console.log(`📊 [USAGE-CALC] Session: ${sessionId} | RequestsLeft: ${5 - requestsToday} | TogglesLeft: ${3 - goFreeToday}`);

      const usageData = {
        requestsToday,
        goFreeToday,
        globalConfig: {
          pingLimit: globalConfig.pingLimit,
          toggleLimit: globalConfig.toggleLimit
        }
      };

      // Emit to specific socket if provided, OR to all sockets in the user's private room
      if (socket) {
        socket.emit("usage-update", usageData);
      }
      io.to(`user_${sessionId}`).emit("usage-update", usageData);

      console.log(`📊 [USAGE] Sent update to ${sessionId}:`, usageData);
    }
  } catch (err) {
    console.error("❌ sendUsageUpdate Error:", err);
  }
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
        $set: {
          sessionId,
          name,
          lastSeen: new Date()
        },
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
    gender: user?.gender || "none",
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

// API paths

// ♻️ ADMIN: Manually reset a user's daily usage counters
app.post("/api/admin/users/:email/reset-daily-limits", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    await db.collection("users").updateOne(
      { email: req.params.email },
      { $set: { requestsToday: 0, goFreeToday: 0 } }
    );

    console.log(`♻️ Admin reset daily usage for ${req.params.email}`);
    res.json({ success: true });

    io.emit("admin-usage-reset", { email: req.params.email });
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
app.get("/api/activeusers", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    const users = await db.collection("activeusers").find({}).toArray();
    res.json(users);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/admin/users", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    const users = await db.collection("users").find({}).sort({ totalRequests: -1 }).toArray();
    res.json(users);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// End of API routes

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

// 🛡️ ADMIN: Global App Configuration
app.get("/api/admin/config", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  try {
    const config = await db.collection("appConfig").findOne({ type: "global" });
    res.json(config);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/admin/config", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  const { pingLimit, toggleLimit } = req.body;

  try {
    const updateDoc = {};
    if (pingLimit !== undefined) updateDoc.pingLimit = parseInt(pingLimit);
    if (toggleLimit !== undefined) updateDoc.toggleLimit = parseInt(toggleLimit);

    await db.collection("appConfig").updateOne(
      { type: "global" },
      {
        $set: { ...updateDoc, updatedAt: new Date() }
      }
    );

    // Update local config
    if (pingLimit !== undefined) globalConfig.pingLimit = parseInt(pingLimit);
    if (toggleLimit !== undefined) globalConfig.toggleLimit = parseInt(toggleLimit);

    io.emit("config-update", globalConfig);

    console.log("🛡️ Admin updated global config:", globalConfig);
    res.json({ success: true, config: globalConfig });
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

    // Join a private room for this user session to sync multiple tabs
    socket.join(`user_${sessionId}`);

    socket.on("usage-refresh", (sid) => {
      console.log(`📊 [REFRESH] User ${sid} requested usage refresh`);
      sendUsageUpdate(sid, socket);
    });

    console.log(`✅ User registered: ${sessionId} -> ${socket.id} (Joined user_${sessionId})`);

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

    // Send initial usage stats on registration
    sendUsageUpdate(sessionId, socket);
  });

  socket.on("usage-refresh", (sessionId) => {
    sendUsageUpdate(sessionId, socket);
  });

  socket.on("send-chat-request", async ({ senderId, senderName, receiverId, receiverName, senderVibe }) => {
    const receiverSocketId = userSockets.get(receiverId);
    console.log(`📤 Chat request from ${senderName} (${senderId}) to ${receiverName || 'someone'} (${receiverId})`);

    // 🛡️ LIMIT CHECK
    if (db) {
      try {
        userCheck = await db.collection("users").findOne({ sessionId: senderId });

        const today = new Date().toDateString();
        if (userCheck && userCheck.lastRequestDate === today) {
          if ((userCheck.requestsToday || 0) >= globalConfig.pingLimit) {
            console.log(`🚫 [LIMIT] User ${senderId} hit ping limit (${globalConfig.pingLimit})`);
            socket.emit("request-failed", {
              message: `Daily limit reached (${globalConfig.pingLimit} requests).`,
              limitReached: true
            });
            return;
          }
        }
      } catch (err) {
        console.error("❌ Limit check error:", err);
      }
    }

    // 📊 USER METRICS: Increment total and daily request counters (Atomic & Tab-Safe)
    if (db) {
      try {
        const today = new Date().toDateString();

        // Attempt atomic increment for the SAME day
        let senderResult = await db.collection("users").findOneAndUpdate(
          { sessionId: senderId, lastRequestDate: today },
          { $inc: { totalRequests: 1, requestsToday: 1 } },
          { returnDocument: 'after' }
        );

        // If result is null, it's either a new day or a new user
        if (!senderResult || (!senderResult.value && !senderResult.sessionId)) {
          console.log(`🌅 [PING-NEW-DAY] Resetting daily counter for ${senderId}`);
          senderResult = await db.collection("users").findOneAndUpdate(
            { sessionId: senderId },
            {
              $set: { requestsToday: 1, lastRequestDate: today },
              $inc: { totalRequests: 1 }
            },
            { returnDocument: 'after', upsert: true }
          );
        }

        const updatedUser = senderResult?.value || senderResult;
        console.log(`📡 [PING-SUCCESS] User: ${senderId} | NewCount: ${updatedUser?.requestsToday || 1}`);

        // 📊 Emit updated counts to the sender
        sendUsageUpdate(senderId, socket);

        if (updatedUser && updatedUser.email) {
          await db.collection("activitylogs").insertOne({
            userEmail: updatedUser.email,
            type: 'REQUEST_SENT',
            detail: `Sent vibe check to ${receiverName || receiverId} (${senderVibe || 'free'})`,
            partnerName: receiverName,
            vibe: senderVibe,
            timestamp: new Date()
          });
        }
      } catch (err) {
        console.error("❌ Error tracking stats for request:", err.message);
      }
    }

    if (receiverSocketId) {
      io.to(receiverSocketId).emit("receive-chat-request", {
        senderId,
        senderName,
        isPriority: false
      });
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
      userRooms.set(senderId, { roomId, partnerName: receiverName });
      userRooms.set(receiverId, { roomId, partnerName: senderName });

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
      senderId: socket.sessionId, // Added senderId for robust frontend deduplication
      type: type || 'text',
      timestamp: timestamp.toISOString()
    };
    console.log(`💬 [V:${SERVER_VERSION}][Room ${roomId}] Msg from ${senderName} (${socket.sessionId}): ${type || 'text'} (ID: ${msg.id})`);
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
    io.to(roomId).emit("session-ended", { roomId }); // End the session for both
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

    const sId = socket.sessionId || sessionId;

    // 🛡️ TURN ENFORCEMENT: Only allow selection if it is this user's turn
    if (sId !== game.turnId) {
      console.log(`🚫 [VIBE] Blocked selection from ${sId} (Current Turn: ${game.turnId})`);
      return;
    }

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

      // Start next round with the user who picked LAST (alternate flow)
      // Turn is already flipped by the key check below if it was Step 1
      // but here we just ensure a consistent state for Round 2
      game.turnId = sId;

      setTimeout(() => {
        if (game.round > 5) {
          io.to(roomId).emit("vibe-game-status", { isOpen: false, finished: true });
          activeVibeGames.delete(roomId);
        } else {
          io.to(roomId).emit("vibe-game-state", game);
        }
      }, 3000); // 3 second delay to show result
    } else {
      // First person picked: switch turn to the other person for Step 2 of the logic
      game.turnId = game.participantIds.find(id => id !== sId);
      io.to(roomId).emit("vibe-game-state", game);
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

    // 🛡️ STATUS LIMIT CHECK
    try {
      userCheck = await db.collection("users").findOne({ sessionId: id });

      const today = new Date().toDateString();
      if (userCheck && userCheck.lastGoFreeDate === today) {
        if ((userCheck.goFreeToday || 0) >= globalConfig.toggleLimit) {
          socket.emit("limit-reached", {
            type: "STATUS_TOGGLE",
            message: `You've shared your vibe ${globalConfig.toggleLimit} times today!`
          });
          return;
        }
      }
    } catch (err) {
      console.error("❌ Status limit check error:", err);
    }

    const today = new Date().toDateString();

    // 🛡️ ATOMIC GO-FREE COUNTER: Multi-tab safe increment
    let toggleResult = await db.collection("users").findOneAndUpdate(
      { sessionId: id, lastGoFreeDate: today },
      { $inc: { goFreeToday: 1 }, $set: { isFree: true, socketId: socket.id, status } },
      { returnDocument: 'after' }
    );

    if (!toggleResult || (!toggleResult.value && !toggleResult.sessionId)) {
      console.log(`🌅 [GO-FREE-NEW-DAY] Resetting visibility counter for ${id}`);
      toggleResult = await db.collection("users").findOneAndUpdate(
        { sessionId: id },
        {
          $set: { goFreeToday: 1, lastGoFreeDate: today, isFree: true, socketId: socket.id, status }
        },
        { returnDocument: 'after', upsert: true }
      );
    }

    console.log(`🌐 [GO-FREE-SUCCESS] User: ${id} | Toggled Visibility`);

    // 📊 Update local count after toggle
    sendUsageUpdate(id, socket);

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

    // 📊 USER METRICS: Log visibility toggle to activity history
    try {
      const userDoc = await db.collection("users").findOne({ sessionId: id });
      if (userDoc && userDoc.email) {
        await db.collection("activitylogs").insertOne({
          userEmail: userDoc.email,
          type: 'STATUS_TOGGLE',
          detail: `Shared vibe visibility: "${status}"`,
          timestamp: new Date(),
          isActive: true
        });
      }
    } catch (err) {
      console.error("❌ Visibility log error:", err);
    }

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
        io.to(socket.currentRoom).emit("session-ended", { roomId: socket.currentRoom });
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

  // ♻️ STARTUP USAGE RESET: Check if we missed a reset while offline
  try {
    const now = new Date();
    const todayStr = now.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
    const config = await db.collection("appConfig").findOne({ type: "global" });

    if (config?.lastGlobalResetDate !== todayStr) {
      console.log(`🔄 [STARTUP] Monthly/Daily reset needed. Last reset: ${config?.lastGlobalResetDate || 'Never'}`);

      const result = await db.collection("users").updateMany(
        {},
        { $set: { requestsToday: 0, goFreeToday: 0 } }
      );

      await db.collection("appConfig").updateOne(
        { type: "global" },
        { $set: { lastGlobalResetDate: todayStr, updatedAt: new Date() } }
      );

      console.log(`✅ [STARTUP] Missed reset completed for ${result.modifiedCount} users.`);
    }

    // ♻️ STARTUP MONTHLY RESET: Check if we missed a monthly reset while offline
    const currentMonth = now.getMonth();
    const currentYear = now.getFullYear();
    const monthKey = `${currentYear}-${currentMonth + 1}`;

    if (config?.lastMonthlyReset !== monthKey) {
      console.log(`🔄 [STARTUP] Monthly Connection Reset needed. Last reset: ${config?.lastMonthlyReset || 'Never'}`);

      await db.collection("globalstats").updateOne(
        { type: "monthly" },
        { $set: { count: 0, updatedAt: new Date() } },
        { upsert: true }
      );

      await db.collection("appConfig").updateOne(
        { type: "global" },
        { $set: { lastMonthlyReset: monthKey, updatedAt: new Date() } }
      );

      console.log(`✅ [STARTUP] Missed Monthly reset completed for ${monthKey}`);
    }
  } catch (err) {
    console.error("❌ Failed startup usage reset check:", err);
  }

  // 🧹 CLEAR STALE ACTIVE USERS ON STARTUP
  try {
    const result = await db.collection("activeusers").deleteMany({});
    console.log(`🧹 Cleaned up ${result.deletedCount} stale active user records`);
  } catch (err) {
    console.error("❌ Failed to cleanup activeusers on startup:", err);
  }

  server.listen(PORT, "0.0.0.0", () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
    console.log("✅ [SYNC] Global Stats & Dashboard Theme Fixes Active");
  });
});
