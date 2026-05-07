require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const axios = require('axios');
const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { v4: uuidv4 } = require('uuid');

const app = express();

// =======================
// 🛡️ SECURITY MIDDLEWARE
// =======================
app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Rate limiting disabled per user request
// const generalLimiter = rateLimit({
//   windowMs: 15 * 60 * 1000,
//   max: 100,
//   message: "Too many requests. Please try again later.",
//   standardHeaders: true,
//   legacyHeaders: false
// });
// app.use(generalLimiter);


const SECRET = process.env.JWT_SECRET || "secretkey";
const DAILY_TRANSFER_LIMIT = Number(process.env.DAILY_TRANSFER_LIMIT) || 500000;
const PORT = process.env.PORT || 3001;

// =======================
// ✅ MongoDB
// =======================
const mongoUri = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/payments?replicaSet=rs0';
mongoose.connect(mongoUri)
  .then(() => console.log("MongoDB Connected ✅"))
  .catch(err => console.error(err));

// =======================
// 💚 HEALTH CHECK
// =======================
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    service: 'transaction-service',
    uptime: Math.floor(process.uptime()) + 's',
    db: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
    timestamp: new Date().toISOString()
  });
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
// 🤝 BENEFICIARY MODEL
// =======================
const Beneficiary = mongoose.model('Beneficiary', new mongoose.Schema({
  userId: String,
  fullName: String,
  bankName: String,
  accountNumber: String,
  swiftCode: String,
  address: String,
  country: String,
  addedAt: { type: Date, default: Date.now }
}));

// =======================
// 💸 TRANSACTION MODEL
// =======================
const Transaction = mongoose.model('Transaction', new mongoose.Schema({
  from: String,
  to: String, // Keep internal username if available
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
  userId: String,
  type: String,
  txId: String,
  purpose: { type: String, default: "General Transfer" },
  createdAt: { type: Date, default: Date.now }
}));

// =======================
// 🔐 AUTH
// =======================
function auth(req, res, next) {
  const authHeader = req.headers.authorization;

  if (!authHeader) return res.status(401).send("No token ❌");

  const token = authHeader.split(" ")[1];

  try {
    const decoded = jwt.verify(token, SECRET);
    req.userId = decoded.id;
    next();
  } catch {
    res.status(401).send("Invalid token ❌");
  }
}

// =======================
// 🔥 Kafka Setup
// =======================
const kafka = new Kafka({
  clientId: 'worker',
  brokers: [process.env.KAFKA_BROKERS || '127.0.0.1:9092'],
  ssl: { rejectUnauthorized: false },
  sasl: process.env.KAFKA_SASL_USERNAME ? {
    mechanism: 'plain',
    username: process.env.KAFKA_SASL_USERNAME,
    password: process.env.KAFKA_SASL_PASSWORD
  } : undefined
});

const producer = kafka.producer();

async function connectProducer() {
  await producer.connect();
  console.log("Kafka Producer Connected ✅");
}
connectProducer();

// =======================
// 🔐 REGISTER
// =======================
app.post('/register', async (req, res) => {
  try {
    const { username, password, country } = req.body;

    if (!username || !password || !country) {
      return res.status(400).send("All fields required ❌");
    }

    const exists = await User.findOne({ username });
    if (exists) {
      return res.status(400).send("User already exists ❌");
    }

    const hashed = await bcrypt.hash(password, 10);

    // Auto-generate SWIFT details based on country
    const countryToBank = {
      INDIA: { bankName: "State Bank of India", swiftCode: "SBININBB" },
      USA: { bankName: "JP Morgan Chase", swiftCode: "CHASUS33" },
      UK: { bankName: "Barclays Bank", swiftCode: "BARCGB22" },
      CANADA: { bankName: "Royal Bank of Canada", swiftCode: "ROYCCA22" },
      AUSTRALIA: { bankName: "Commonwealth Bank", swiftCode: "CTBAAU2S" },
      GERMANY: { bankName: "Deutsche Bank", swiftCode: "DEUTDEFF" },
      JAPAN: { bankName: "Mizuho Bank", swiftCode: "MHBKJPJT" },
      UAE: { bankName: "Emirates NBD", swiftCode: "EBILAEAD" }
    };
    const bankDetails = countryToBank[country.toUpperCase()] || { bankName: "Global Reserve Bank", swiftCode: "GLOBUS11" };
    const randomAcc = Math.floor(Math.random() * 9000000000) + 1000000000;

    await User.create({
      username,
      password: hashed,
      balance: 50000,
      country,
      accountNumber: "PAYFLOW-" + randomAcc,
      swiftCode: bankDetails.swiftCode,
      bankName: bankDetails.bankName
    });

    res.send("Registered ✅");

  } catch (err) {
    console.error("Registration Error:", err);
    res.status(500).send("Error ❌");
  }
});

// =======================
// 🔐 LOGIN
// =======================
app.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body;

    const user = await User.findOne({ username });

    if (!user) return res.status(401).send("User not found ❌");

    const isMatch = await bcrypt.compare(password, user.password);

    if (!isMatch) return res.status(401).send("Wrong password ❌");

    const token = jwt.sign(
      { id: user._id },
      SECRET,
      { expiresIn: "1h" }
    );

    res.json({ token });

  } catch (err) {
    console.error(err);
    res.status(500).send("Login error ❌");
  }
});

// =======================
// 💰 WALLET DEPOSIT
// =======================
app.post('/deposit', auth, async (req, res) => {
  try {
    const { amount } = req.body;
    if (!amount || amount <= 0) return res.status(400).send("Invalid amount");

    await User.updateOne({ _id: req.userId }, { $inc: { balance: amount } });
    res.send("Deposit successful ✅");
  } catch (err) {
    res.status(500).send("Deposit failed ❌");
  }
});

// =======================
// 🛡️ FEES & QUOTE
// =======================
app.post('/quote', auth, async (req, res) => {
  try {
    const { to, amount, targetCountry } = req.body;
    if (!amount) return res.status(400).send("Invalid amount");

    const sender = await User.findById(req.userId);
    const currencyMap = { 
        INDIA: "INR", USA: "USD", UK: "GBP", 
        CANADA: "CAD", AUSTRALIA: "AUD", GERMANY: "EUR", JAPAN: "JPY", UAE: "AED" 
    };
    const currency = currencyMap[(sender.country || "").toUpperCase().trim()] || "USD";

    // For target currency, check if 'to' is a username or use the provided targetCountry
    let destCountry = targetCountry;
    if (to) {
        const receiver = await User.findOne({ username: to });
        if (receiver) destCountry = receiver.country;
    }
    
    const targetCurrency = currencyMap[(destCountry || "").toUpperCase().trim()] || "USD";

    const response = await axios.get(
      `${process.env.CURRENCY_SERVICE_URL || 'http://127.0.0.1:3002'}/convert?amount=${amount}&from=${currency}&to=${targetCurrency}`
    );

    const convertedAmount = response.data.converted;
    const rate = response.data.rate;
    if (!convertedAmount) return res.status(500).send("Currency conversion failed");

    const platformFee = amount * 0.015; // 1.5% fee
    const totalAmountDeducted = amount + platformFee;

    res.json({
      baseAmount: amount,
      platformFee,
      totalAmountDeducted,
      rate,
      receivedAmount: convertedAmount,
      currency,
      targetCurrency
    });
  } catch (err) {
    res.status(500).send("Quote error");
  }
});

// =======================
// 🤝 BENEFICIARIES
// =======================
app.get('/beneficiaries', auth, async (req, res) => {
  try {
    const data = await Beneficiary.find({ userId: req.userId });
    res.json(data);
  } catch {
    res.status(500).send("Error");
  }
});

app.post('/beneficiaries', auth, async (req, res) => {
  console.log(`👤 Beneficiary Save Attempt by User ID: ${req.userId}`);
  console.log("📦 DATA received:", JSON.stringify(req.body));

  try {
    const { fullName, bankName, accountNumber, swiftCode, address, country } = req.body;
    
    // Check if user is trying to add an internal person by username
    let internalUser = await User.findOne({ username: fullName });
    if (!internalUser && accountNumber) {
        internalUser = await User.findOne({  $or: [{ username: accountNumber }, { accountNumber: accountNumber }] });
    }

    if (!fullName && !internalUser) {
        console.warn("❌ FAILED: No name provided");
        return res.status(400).send("Please provide a name or valid PayFlow username.");
    }

    // Force SWIFT details if NOT an internal user
    if (!internalUser && (!accountNumber || !swiftCode)) {
        console.warn("❌ FAILED: External bank missing SWIFT details");
        return res.status(400).send("For external banks, you must provide BOTH an Account Number and a SWIFT/BIC Code.");
    }

    const bData = {
      userId: req.userId,
      fullName: internalUser ? internalUser.username : fullName,
      bankName: internalUser ? (internalUser.bankName || "PayFlow Hub") : (bankName || "External Bank"),
      accountNumber: internalUser ? (internalUser.accountNumber || internalUser.username) : accountNumber,
      swiftCode: internalUser ? (internalUser.swiftCode || "PAYFLOW-INTERNAL") : swiftCode,
      address: address || "Global",
      country: internalUser ? (internalUser.country || "GLOBAL") : (country || "GLOBAL")
    };

    const exists = await Beneficiary.findOne({ userId: req.userId, accountNumber: bData.accountNumber });
    if (exists) {
        console.warn("❌ FAILED: Duplicate contact", bData.accountNumber);
        return res.status(400).send("This beneficiary is already in your contacts.");
    }

    await Beneficiary.create(bData);
    console.log("✅ SUCCESS: Beneficiary added:", bData.fullName);
    res.send("Beneficiary Added! ✅");
  } catch (err) {
    console.error("❌ ERROR adding beneficiary:", err.message);
    res.status(500).send("Server error while saving beneficiary.");
  }
});

// =======================
// 💸 SEND MONEY (FIXED)
// =======================
app.post('/transfer', auth, async (req, res) => {
  try {
    const { to, amount, pin, reason, recipientName, recipientAddress, recipientAccountNumber, recipientSwiftCode, recipientBankName } = req.body;

    if (!amount || amount <= 0) {
      return res.status(400).send("Please provide a valid transfer amount.");
    }

    if (!to && (!recipientAccountNumber || !recipientSwiftCode)) {
      return res.status(400).send("To complete a SWIFT transfer, please provide the recipient's Account Number and SWIFT/BIC Code.");
    }

    const sender = await User.findById(req.userId);
    
    // 🔐 2FA SECURITY CHECK
    if (sender.twoFaEnabled) {
        if (!pin) return res.status(400).send("2FA is enabled. Please provide your PIN.");
        const isMatch = await bcrypt.compare(String(pin), sender.pin2fa);
        if (!isMatch) return res.status(401).send("Incorrect 2FA PIN.");
    }
    
    // Find receiver if username 'to' is provided, or try to find by account name or account number
    let receiver = null;
    if (to) {
        receiver = await User.findOne({ username: to });
    } else {
        // Search by recipientName (as username) or recipientAccountNumber
        receiver = await User.findOne({ 
            $or: [
                { username: recipientName },
                { accountNumber: recipientAccountNumber }
            ] 
        });
    }

    const targetTo = receiver ? receiver.username : (to || "SWIFT_EXTERNAL");

    const senderCountry = (sender.country || "").toUpperCase().trim();
    const currencyMap = { 
        INDIA: "INR", USA: "USD", UK: "GBP", 
        CANADA: "CAD", AUSTRALIA: "AUD", GERMANY: "EUR", JAPAN: "JPY", UAE: "AED" 
    };
    const currency = currencyMap[senderCountry] || "USD";

    // Determine target currency
    let targetCurrency = "USD";
    if (receiver) {
        const receiverCountry = (receiver.country || "").toUpperCase().trim();
        targetCurrency = currencyMap[receiverCountry] || "USD";
    } else {
        // If external SWIFT, we could potentially get country from bank details
        // For now default to USD or based on some mapping
        targetCurrency = "USD"; 
    }

    const response = await axios.get(
      `${process.env.CURRENCY_SERVICE_URL || 'http://127.0.0.1:3002'}/convert?amount=${amount}&from=${currency}&to=${targetCurrency}`
    );

    const convertedAmount = response.data.converted;

    if (!convertedAmount) {
      return res.status(500).send("Currency conversion failed ❌");
    }

    const platformFee = amount * 0.015;
    const totalAmountDeducted = amount + platformFee;

    if (sender.balance < totalAmountDeducted) {
      return res.status(400).send("Insufficient balance (including 1.5% fee)");
    }

    // 🛡️ DAILY TRANSFER LIMIT CHECK
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const dailySpent = await Transaction.aggregate([
      { $match: { userId: req.userId, type: "DEBIT", createdAt: { $gte: todayStart } } },
      { $group: { _id: null, total: { $sum: "$amount" } } }
    ]);
    const spentToday = (dailySpent[0]?.total || 0) + totalAmountDeducted;
    if (spentToday > DAILY_TRANSFER_LIMIT) {
      return res.status(400).send(`Daily transfer limit of ${DAILY_TRANSFER_LIMIT.toLocaleString()} exceeded. You have already sent ${(dailySpent[0]?.total || 0).toLocaleString()} today.`);
    }

    const txId = uuidv4();
    
    await Transaction.create({
      from: sender.username,
      to: targetTo,
      recipientName,
      recipientAddress,
      recipientAccountNumber,
      recipientSwiftCode,
      recipientBankName,
      amount: totalAmountDeducted,
      currency,
      convertedAmount,
      convertedCurrency: targetCurrency,
      status: "PROCESSING",
      userId: req.userId,
      type: "DEBIT",
      txId,
      purpose: reason || "General Transfer"
    });

    const newData = {
      from: sender.username,
      to: targetTo,
      recipientName,
      recipientAddress,
      recipientAccountNumber,
      recipientSwiftCode,
      recipientBankName,
      amount: totalAmountDeducted,
      currency,
      userId: req.userId,
      convertedAmount,
      convertedCurrency: targetCurrency,
      txId
    };

    await producer.send({
      topic: 'transactions',
      messages: [{ value: JSON.stringify(newData) }]
    });

    res.send("Sent to Kafka 🚀");

  } catch (err) {
    console.error("❌ ERROR:", err.message);
    res.status(500).send("Error ❌");
  }
});

// =======================
// 👤 USER PROFILE (/me)
// =======================
app.get('/me', auth, async (req, res) => {
  try {
    const user = await User.findById(req.userId).select('-password');
    if (!user) return res.status(404).send("User not found");
    
    // Back-fill details for existing users if missing
    if (!user.accountNumber) {
        const countryToBank = {
          INDIA: { bankName: "State Bank of India", swiftCode: "SBININBB" },
          USA: { bankName: "JP Morgan Chase", swiftCode: "CHASUS33" },
          UK: { bankName: "Barclays Bank", swiftCode: "BARCGB22" },
          CANADA: { bankName: "Royal Bank of Canada", swiftCode: "ROYCCA22" },
          AUSTRALIA: { bankName: "Commonwealth Bank", swiftCode: "CTBAAU2S" },
          GERMANY: { bankName: "Deutsche Bank", swiftCode: "DEUTDEFF" },
          JAPAN: { bankName: "Mizuho Bank", swiftCode: "MHBKJPJT" },
          UAE: { bankName: "Emirates NBD", swiftCode: "EBILAEAD" }
        };
        const bankDetails = countryToBank[(user.country || "").toUpperCase()] || { bankName: "Global Reserve Bank", swiftCode: "GLOBUS11" };
        const randomAcc = Math.floor(Math.random() * 9000000000) + 1000000000;
        
        user.accountNumber = "PAYFLOW-" + randomAcc;
        user.swiftCode = bankDetails.swiftCode;
        user.bankName = bankDetails.bankName;
        await user.save();
    }

    const currencyMap = { 
        INDIA: "INR", USA: "USD", UK: "GBP", 
        CANADA: "CAD", AUSTRALIA: "AUD", GERMANY: "EUR", JAPAN: "JPY", UAE: "AED" 
    };
    const currency = currencyMap[(user.country || "").toUpperCase().trim()] || "USD";
    
    // Convert to plain object to add currency field
    const userData = user.toObject();
    userData.currency = currency;
    res.json(userData);
  } catch (err) {
    res.status(500).send("Error");
  }
});

// =======================
// 📸 UPLOAD PROFILE PIC
// =======================
app.post('/profile/upload', auth, async (req, res) => {
  try {
    const { profilePic } = req.body;
    if (!profilePic) return res.status(400).send("No image provided");
    await User.findByIdAndUpdate(req.userId, { profilePic });
    res.send("Profile picture updated ✅");
  } catch (err) {
    res.status(500).send("Failed to upload picture");
  }
});

// =======================
// 🔑 CHANGE PASSWORD
// =======================
app.post('/change-password', auth, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;
    if (!currentPassword || !newPassword) return res.status(400).send("All fields required");
    if (newPassword.length < 6) return res.status(400).send("Password must be at least 6 characters");

    const user = await User.findById(req.userId);
    const isMatch = await bcrypt.compare(currentPassword, user.password);
    if (!isMatch) return res.status(401).send("Current password is incorrect");

    const hashed = await bcrypt.hash(newPassword, 10);
    await User.findByIdAndUpdate(req.userId, { password: hashed });
    res.send("Password changed successfully ✅");
  } catch (err) {
    res.status(500).send("Failed to change password");
  }
});

// =======================
// 🔐 2FA SETUP (Set PIN)
// =======================
app.post('/2fa/setup', auth, async (req, res) => {
  try {
    const { pin } = req.body;
    if (!pin || pin.length !== 4 || isNaN(pin)) return res.status(400).send("PIN must be exactly 4 digits");
    const hashed = await bcrypt.hash(String(pin), 10);
    await User.findByIdAndUpdate(req.userId, { pin2fa: hashed, twoFaEnabled: true });
    res.send("2FA enabled ✅");
  } catch (err) {
    res.status(500).send("Failed to setup 2FA");
  }
});

// =======================
// 🔓 2FA DISABLE
// =======================
app.post('/2fa/disable', auth, async (req, res) => {
  try {
    const { pin } = req.body;
    const user = await User.findById(req.userId);
    if (!user.twoFaEnabled) return res.status(400).send("2FA is not enabled");
    const isMatch = await bcrypt.compare(String(pin), user.pin2fa);
    if (!isMatch) return res.status(401).send("Incorrect PIN");
    await User.findByIdAndUpdate(req.userId, { pin2fa: "", twoFaEnabled: false });
    res.send("2FA disabled ✅");
  } catch (err) {
    res.status(500).send("Failed to disable 2FA");
  }
});

// =======================
// 📈 PROFILE STATS (/profile/stats)
// =======================
app.get('/profile/stats', auth, async (req, res) => {
  try {
    // Determine overall money sent and overall money received
    const data = await Transaction.find({ userId: req.userId, status: "SUCCESS" });
    let totalSent = 0;
    let totalReceived = 0;

    data.forEach(tx => {
      if(tx.type === "DEBIT") totalSent += tx.amount || 0;
      if(tx.type === "CREDIT") totalReceived += tx.convertedAmount || 0;
    });

    res.json({ totalSent, totalReceived });
  } catch {
    res.status(500).send("Error");
  }
});

// =======================
// 📄 GET TRANSACTIONS
// =======================
app.get('/transactions', auth, async (req, res) => {
  try {
    const { search, status } = req.query;
    const filter = { userId: req.userId };

    // Status filter
    if (status && status !== 'ALL') {
      filter.status = status.toUpperCase();
    }

    // Search filter (recipient name, txId, or 'to' field)
    if (search) {
      filter.$or = [
        { to: { $regex: search, $options: 'i' } },
        { recipientName: { $regex: search, $options: 'i' } },
        { txId: { $regex: search, $options: 'i' } }
      ];
    }

    const data = await Transaction.find(filter).sort({ createdAt: -1 });
    res.json(data);
  } catch (err) {
    res.status(500).send("Error ❌");
  }
});

// =======================
// ❌ FAILED
// =======================
app.get('/failed', auth, async (req, res) => {
  try {
    const data = await Transaction.find({
      userId: req.userId,
      status: "FAILED"
    });

    res.json(data);
  } catch {
    res.status(500).send("Error ❌");
  }
});

// =======================
// 📥 EXPORT TRANSACTIONS (CSV)
// =======================
app.get('/transactions/export', auth, async (req, res) => {
  try {
    const data = await Transaction.find({ userId: req.userId }).sort({ createdAt: -1 });
    
    let csv = "Date,Type,To,Amount,Currency,Status,Purpose,TXID\n";
    data.forEach(tx => {
      const date = tx.createdAt.toISOString().split('T')[0];
      csv += `${date},${tx.type},${tx.to},${tx.amount},${tx.currency},${tx.status},"${tx.purpose}",${tx.txId}\n`;
    });

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=transactions.csv');
    res.status(200).send(csv);
  } catch (err) {
    res.status(500).send("Export failed ❌");
  }
});

// =======================
// 🚀 START
// =======================
const server = app.listen(PORT, () => {
  console.log(`Transaction Service running on ${PORT} 🚀`);
  console.log(`🛡️ Security: Helmet ON | Rate Limiting ON | Daily Limit: ${DAILY_TRANSFER_LIMIT.toLocaleString()}`);
});

// =======================
// 🔌 GRACEFUL SHUTDOWN
// =======================
async function gracefulShutdown(signal) {
  console.log(`\n⚠️ ${signal} received. Shutting down gracefully...`);
  try {
    server.close(() => console.log('  ✅ HTTP server closed'));
    await producer.disconnect();
    console.log('  ✅ Kafka producer disconnected');
    await mongoose.connection.close();
    console.log('  ✅ MongoDB connection closed');
  } catch (err) {
    console.error('  ❌ Error during shutdown:', err.message);
  }
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));