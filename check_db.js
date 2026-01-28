const { MongoClient } = require("mongodb");
require("dotenv").config();

async function checkStats() {
    const client = new MongoClient(process.env.MONGO_URI);
    try {
        await client.connect();
        const db = client.db("freeNow");
        const stats = await db.collection("globalstats").findOne({ type: "daily_peak" });
        console.log("--- Global Stats (daily_peak) ---");
        if (!stats) {
            console.log("No daily_peak stats found.");
        } else {
            console.log("Last Rotation:", stats.lastRotationDate);
            console.log("Yesterday:", JSON.stringify(stats.yesterday, null, 2));
            console.log("Today:", JSON.stringify(stats.today, null, 2));
        }
    } catch (err) {
        console.error("Error:", err);
    } finally {
        await client.close();
    }
}

checkStats();
