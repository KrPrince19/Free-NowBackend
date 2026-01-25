console.log("🔥 RUNNING FILE:", __filename);

require("dotenv").config();

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { MongoClient } = require("mongodb");
const cors = require("cors");
const cron = require("node-cron");

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
const client = new MongoClient(process.env.MONGO_URI);
let db;

async function initDB() {
  try {
    console.log("⏳ Connecting to MongoDB Atlas...");
    await client.connect();
    db = client.db("freeNow"); // ✅ MATCH URI DB NAME
    console.log("✅ MongoDB Connected");
    return true;
  } catch (err) {
    console.error("❌ MongoDB Connection Error:", err.message);
    return false;
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

      const thirtyDaysAgo = new Date();
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

      await db.collection("activitylogs").updateMany(
        { timestamp: { $lt: thirtyDaysAgo }, isActive: true },
        { $set: { isActive: false } }
      );

      io.emit("midnight-update", { message: "New day started" });
      console.log("✅ Midnight cleanup done");
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
});

app.get("/api/global-stats/monthly", async (req, res) => {
  if (!db) return res.status(500).json({ error: "DB not ready" });
  const stats = await db.collection("globalstats").findOne({ type: "monthly" });
  res.json({ count: stats?.count || 0 });
});

const activeRooms = new Map(); // roomId -> { participants: [], names: {}, startTime: Date }

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
    // Current time in IST (UTC+5.5)
    const nowIST = new Date(new Date().getTime() + (5.5 * 60 * 60 * 1000));

    // Yesterday in IST
    const yesterday = new Date(nowIST);
    yesterday.setDate(yesterday.getDate() - 1);
    yesterday.setHours(0, 0, 0, 0);

    const today = new Date(nowIST);
    today.setHours(0, 0, 0, 0);

    // Convert back to UTC for DB query (since DB stores in UTC)
    const startUTC = new Date(yesterday.getTime() - (5.5 * 60 * 60 * 1000));
    const endUTC = new Date(today.getTime() - (5.5 * 60 * 60 * 1000));

    let record = await db.collection("connections")
      .find({ endTime: { $gte: startUTC, $lt: endUTC } })
      .sort({ durationMs: -1 })
      .limit(1)
      .toArray();

    // Fallback: If no record for specifically yesterday, show the peak from the last 7 days
    if (record.length === 0) {
      const weekAgo = new Date(nowIST);
      weekAgo.setDate(weekAgo.getDate() - 7);
      const weekAgoUTC = new Date(weekAgo.getTime() - (5.5 * 60 * 60 * 1000));

      record = await db.collection("connections")
        .find({ endTime: { $gte: weekAgoUTC } })
        .sort({ durationMs: -1 })
        .limit(1)
        .toArray();
    }

    if (record.length > 0) {
      const r = record[0];
      const mins = Math.floor(r.durationMs / 60000);
      const displayDuration = mins < 60 ? `${mins}m` : `${Math.floor(mins / 60)}h ${mins % 60}m`;
      res.json({ names: r.names, duration: displayDuration });
    } else {
      res.json({ names: "Waiting for peak vibe...", duration: "0m" });
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
    matchesMade: user?.matchesMade || 0
  });
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
   SOCKET.IO
======================= */
const requestTimers = new Map();
const userSockets = new Map(); // sessionId -> socketId
const userRooms = new Map(); // sessionId -> { roomId, partnerName }
const pendingDisconnects = new Map(); // sessionId -> Timeout

io.on("connection", (socket) => {
  console.log("🔌 Socket connected:", socket.id);
  broadcastActiveUsers();

  socket.on("register-user", (sessionId) => {
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
    console.log(`📤 Chat request from ${senderName} (${senderId}) to ${receiverId}`);

    // Track stats
    if (db) {
      const sender = await db.collection("users").findOneAndUpdate(
        { sessionId: senderId },
        { $inc: { totalRequests: 1 } }
      );

      const userDoc = sender.value || sender;
      if (userDoc && userDoc.email) {
        await db.collection("activitylogs").insertOne({
          userEmail: userDoc.email,
          type: 'REQUEST_SENT',
          detail: `Sent vibe check to ${receiverName || receiverId} (${senderVibe || 'free'})`,
          partnerName: receiverName,
          vibe: senderVibe,
          timestamp: new Date()
        });
      }
    }

    if (receiverSocketId) {
      io.to(receiverSocketId).emit("receive-chat-request", { senderId, senderName });
      socket.emit("request-sent-success");
    } else {
      console.log(`❌ Receiver ${receiverId} not found`);
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

      // Attach data for lifecycle
      socket.currentRoom = roomId;
      socket.senderName = receiverName; // On receiver's socket, "senderName" is themselves
      if (senderSocket) {
        senderSocket.currentRoom = roomId;
        senderSocket.senderName = senderName;
      }

      io.to(senderSocketId).emit("chat-started", { ...chatData, partnerName: receiverName });
      socket.emit("chat-init-receiver", { ...chatData, partnerName: senderName });

      io.emit("conversation-started", { roomId });
      console.log(`🤝 Chat started between ${senderName} and ${receiverName} in room ${roomId}`);
    }
  });

  socket.on("typing", ({ roomId, senderName }) => {
    socket.to(roomId).emit("partner-typing", { senderName });
  });

  socket.on("stop-typing", ({ roomId }) => {
    socket.to(roomId).emit("partner-stop-typing");
  });

  socket.on("send-private-message", ({ roomId, message, senderName }) => {
    const msg = {
      id: Date.now().toString(),
      text: message,
      sender: senderName,
      time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      timestamp: new Date()
    };
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
        await db.collection("connections").insertOne({
          names: Object.values(roomInfo.names).join(" & "),
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

    // Grace Period for Disconnection (5 seconds)
    if (socket.currentRoom && sessionId) {
      console.log(`⏳ Starting 5s grace period for ${sessionId}`);
      const timeout = setTimeout(async () => {
        console.log(`🚨 Grace period expired for ${sessionId}`);

        const roomInfo = activeRooms.get(socket.currentRoom);
        if (roomInfo) {
          const durationMs = new Date() - roomInfo.startTime;
          if (db) {
            await db.collection("connections").insertOne({
              names: Object.values(roomInfo.names).join(" & "),
              durationMs,
              endTime: new Date()
            });
          }
        }

        socket.to(socket.currentRoom).emit("partner-left", { senderName: socket.senderName });
        activeRooms.delete(socket.currentRoom);
        userRooms.delete(sessionId);
        io.emit("conversation-ended", { roomId: socket.currentRoom });
        pendingDisconnects.delete(sessionId);

        // Perform final cleanup
        if (db) {
          await db.collection("users").updateOne({ sessionId }, { $set: { isFree: false } });
          await db.collection("activeusers").deleteOne({ sessionId });
          broadcastActiveUsers();
        }
      }, 5000);

      pendingDisconnects.set(sessionId, timeout);
    } else if (sessionId) {
      // Immediate cleanup for non-chatting users
      userSockets.delete(sessionId);
      if (db) {
        await db.collection("users").updateOne({ sessionId }, { $set: { isFree: false } });
        await db.collection("activeusers").deleteOne({ sessionId });
        broadcastActiveUsers();
      }
    }
  });
});

/* =======================
   START SERVER
======================= */
initDB().then((success) => {
  if (!success) {
    console.error("🛑 Server stopped (DB required)");
    process.exit(1);
  }

  server.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
  });
});
