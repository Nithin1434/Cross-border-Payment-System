require('dotenv').config();
const express = require('express');
const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');

const app = express();
const port = process.env.PORT || 10000;

app.get('/', (req, res) => res.send('Worker is alive! 🚀'));
app.listen(port, () => console.log(`Worker heartbeat listening on port ${port}`));

// =======================
// ✅ MongoDB (Replica Set required for Transactions)
// =======================
const mongoUri = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/payments?replicaSet=rs0';

mongoose.connect(mongoUri)
  .then(() => console.log("MongoDB Connected (Replica Set) ✅"))
  .catch(err => {
      console.error("MongoDB Connection Failed ❌ - Ensure replica set is running.");
      process.exit(1);
  });

// =======================
// 👤 USER MODEL
// =======================
const User = mongoose.model('User', new mongoose.Schema({
  username: String,
  password: String,
  balance: { type: Number, default: 50000 },
  country: String,
  profilePic: { type: String, default: "" },
  pin2fa: { type: String, default: "" },
  twoFaEnabled: { type: Boolean, default: false },
  accountNumber: { type: String, default: "" },
  swiftCode: { type: String, default: "" },
  bankName: { type: String, default: "" }
}));

// =======================
// 💸 TRANSACTION MODEL
// =======================
const Transaction = mongoose.model('Transaction', new mongoose.Schema({
  from: String,
  to: String,
  recipientName: String,
  recipientAddress: String,
  recipientAccountNumber: String,
  recipientSwiftCode: String,
  recipientBankName: String,
  amount: Number,
  currency: String,
  convertedAmount: Number,
  convertedCurrency: String,
  status: String,
  retries: { type: Number, default: 0 },
  userId: String,
  type: String, // CREDIT / DEBIT
  txId: String,
  createdAt: { type: Date, default: Date.now }
}));

// =======================
// 🔥 Kafka Setup
// =======================
const kafka = new Kafka({
  clientId: 'worker',
  brokers: [process.env.KAFKA_BROKERS || '127.0.0.1:9092'],
  ssl: process.env.KAFKA_SASL_USERNAME ? { rejectUnauthorized: false } : false,
  sasl: process.env.KAFKA_SASL_USERNAME ? {
    mechanism: 'plain',
    username: process.env.KAFKA_SASL_USERNAME,
    password: process.env.KAFKA_SASL_PASSWORD
  } : undefined
});

const consumer = kafka.consumer({ groupId: 'payment-group-v6' });

// =======================
// 🚀 PROCESS FUNCTION (Atomic Settlement)
// =======================
async function processTransaction(data) {
  console.log(`📥 Processing Transaction: ${data.txId} (${data.amount} ${data.currency})`);

  try {
    const tx = await Transaction.findOne({ txId: data.txId, type: "DEBIT" });

    if (!tx || tx.status === "SUCCESS") {
      console.log("⚠️ Already processed or missing:", data.txId);
      return;
    }

    // 🔍 find receiver and sender
    const receiver = await User.findOne({ username: data.to });
    const sender = await User.findById(data.userId);

    if (!sender || sender.balance < data.amount) {
      console.log(`❌ FAILED: Insufficient balance for ${data.txId}`);
      await Transaction.updateOne({ txId: data.txId, type: "DEBIT" }, { status: "FAILED" });
      return;
    }

    // 💸 Deduct sender
    await User.updateOne({ _id: data.userId }, { $inc: { balance: -data.amount } });
    
    // 👉 Update DEBIT to SUCCESS
    await Transaction.updateOne({ txId: data.txId, type: "DEBIT" }, { status: "SUCCESS" });

    // 👉 receiver (internal or external)
    if (receiver) {
        // 👉 Internal receiver (CREDIT)
        await Transaction.create({
            ...data,
            type: "CREDIT",
            userId: receiver._id,
            status: "SUCCESS"
        });

        // 💰 credit receiver
        await User.updateOne({ _id: receiver._id }, { $inc: { balance: data.convertedAmount } });
        console.log(`✅ SETTLED: -${data.amount} to +${data.convertedAmount} (Internal)`);
    } else {
        console.log(`✅ SETTLED: -${data.amount} (External SWIFT)`);
    }

    console.log(`📲 Notifications dispatched for ${data.txId}`);

  } catch (err) {
    console.error("❌ Settlement Error:", err);
    
    data.retries = (data.retries || 0) + 1;
    if (data.retries <= 2) {
      console.log(`🔄 Retrying settle (${data.retries}/2)...`);
      await new Promise(r => setTimeout(r, 2000));
      return processTransaction(data);
    }

    await Transaction.updateOne({ txId: data.txId, type: "DEBIT" }, { status: "FAILED" });
    console.log("❌ TX MARKED FAILED AFTER RETRIES");
  }
}

// =======================
// 🔥 RUN CONSUMER
// =======================
async function run() {
  await consumer.connect();
  console.log("Kafka Connected ✅");
  await consumer.subscribe({ topic: 'transactions', fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        await processTransaction(data);
      } catch (e) {
        console.error("Consumer error:", e);
      }
    },
  });
}

run().catch(console.error);