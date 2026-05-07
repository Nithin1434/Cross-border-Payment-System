const mongoose = require('mongoose');

const mongoUri = 'mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/payments?replicaSet=rs0';

async function check() {
  try {
    console.log("Connecting to:", mongoUri);
    await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 2000 });
    console.log("Connected! ✅");
    const admin = mongoose.connection.db.admin();
    const status = await admin.command({ replSetGetStatus: 1 });
    console.log("Replica Set Status:", status.ok ? "OK" : "FAILED");
    console.log("Members:", status.members.map(m => `${m.name} is ${m.stateStr}`));
    process.exit(0);
  } catch (err) {
    console.error("Connection failed! ❌");
    console.error(err.message);
    process.exit(1);
  }
}

check();
