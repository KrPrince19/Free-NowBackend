const { MongoClient } = require("mongodb");
require("dotenv").config();

async function checkStats() {
    const client = new MongoClient(process.env.MONGO_URI);
    try {
        await client.connect();
        const db = client.db("freeNow");
        const stats = await db.collection("globalstats").findOne({ type: "daily_peak" });
        if (stats) {
            console.log("ROTATION_LOG: " + JSON.stringify({
                lastRotationDate: stats.lastRotationDate,
                yesterdayNames: stats.yesterday?.names,
                yesterdayDuration: stats.yesterday?.durationMs,
                todayNames: stats.today?.names
            }));
        } else {
            console.log("ROTATION_LOG: NULL");
        }
    } catch (err) {
        console.error(err);
    } finally {
        await client.close();
    }
}

checkStats();
