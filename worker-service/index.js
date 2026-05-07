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
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const tx = await Transaction.findOne({ txId: data.txId, type: "DEBIT" }).session(session);

    if (!tx || tx.status === "SUCCESS") {
      console.log("⚠️ Already processed or missing:", data.txId);
      await session.abortTransaction();
      session.endSession();
      return;
    }

    // Simulate Processing Delay for Realism 🕒
    await new Promise(res => setTimeout(res, 2000));

    // 🔍 find receiver and sender inside session
    const receiver = await User.findOne({ username: data.to }).session(session);
    const sender = await User.findById(data.userId).session(session);

    if (!sender || sender.balance < data.amount) {
      console.log(`⚠️ Insufficient balance for transaction: ${data.txId}`);
      await Transaction.updateOne({ txId: data.txId, type: "DEBIT" }, { status: "FAILED" }).session(session);
      await session.commitTransaction(); // Commit the FAILED status
      session.endSession();
      return;
    }

    // 💸 Deduct sender
    await User.updateOne({ _id: data.userId }, { $inc: { balance: -data.amount } }).session(session);
    
    // 👉 Update DEBIT to SUCCESS
    tx.status = "SUCCESS";
    await tx.save({ session });

    // 👉 receiver (internal or external)
    if (receiver) {
        // 👉 Internal receiver (CREDIT)
        await Transaction.create([{
            ...data,
            type: "CREDIT",
            userId: receiver._id,
            status: "SUCCESS"
        }], { session });

        // 💰 credit receiver
        await User.updateOne({ _id: receiver._id }, { $inc: { balance: data.convertedAmount } }).session(session);
        console.log(`✅ INTERNAL SETTLED: -${data.amount} ${data.currency} from ${sender.username}, +${data.convertedAmount} ${data.convertedCurrency} to ${receiver.username}`);
    } else {
        // SWIFT External
        console.log(`✅ SWIFT EXTERNAL SETTLED: -${data.amount} ${data.currency} from ${sender.username}`);
    }

    await session.commitTransaction();
    session.endSession();
    
    // Notifications (Outside Transaction for performance, if they fail it's okay)
    console.log(`[NotificationEngine] 📲 Notifications dispatched for ${data.txId}`);

  } catch (err) {
    console.error("❌ Settlement Error:", err);
    await session.abortTransaction();
    session.endSession();

    data.retries = (data.retries || 0) + 1;
    if (data.retries <= 3) {
      console.log(`🔄 Retrying settle (${data.retries}/3)...`);
      await new Promise(r => setTimeout(r, 3000));
      return processTransaction(data);
    }

    // Hard fail after retries
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