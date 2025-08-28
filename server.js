// server.js - DynamoDB Version for Production (Fixed for PK/SK structure)
import "dotenv/config";
import express from "express";
import cors from "cors";
import helmet from "helmet";
import rateLimit from "express-rate-limit";
import { createClient } from "redis";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, GetCommand, QueryCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { Queue, Worker, QueueEvents } from "bullmq";
import jwt from "jsonwebtoken";
import multer from "multer";
import fetch from "node-fetch";
import { v4 as uuidv4 } from "uuid";
import winston from "winston";
import { promisify } from "util";

// ===================== CONFIGURATION =====================
const {
  NODE_ENV = "production",
  PORT = 3000,
  
  // AWS Configuration
  AWS_REGION,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  DYNAMODB_TABLE_NAME,
  
  // Redis Configuration
  REDIS_URL,
  JWT_SECRET,
  
  // RunPod Configuration
  RUNPOD_MODE = "proxy",
  RUNPOD_ENDPOINT_ID,
  RUNPOD_TOKEN, 
  RUNPOD_PROXY_BASE,
  RUNPOD_PROXY_RUN_PATH = "/run",
  RUNPOD_PROXY_STATUS_PATH = "/status",
  
  // Rate Limiting
  RATE_LIMIT_WINDOW_MS = "900000",
  RATE_LIMIT_MAX = "100",
  
  // Queue Configuration
  QUEUE_CONCURRENCY = "50",
  QUEUE_MAX_RETRIES = "3",
  
  // Security
  CORS_ORIGINS = "",
  TRUSTED_PROXIES = "1",
  
} = process.env;

// Validation
if (!AWS_REGION || !AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY || !DYNAMODB_TABLE_NAME) {
  console.error("[FATAL] AWS credentials and table name are required");
  process.exit(1);
}
if (!REDIS_URL) {
  console.error("[FATAL] REDIS_URL is required");
  process.exit(1);
}
if (!JWT_SECRET) {
  console.error("[FATAL] JWT_SECRET is required");
  process.exit(1);
}
if (RUNPOD_MODE === "proxy" && !RUNPOD_PROXY_BASE) {
  console.error("[FATAL] RUNPOD_PROXY_BASE is required for proxy mode");
  process.exit(1);
}

// ===================== LOGGING =====================
const logger = winston.createLogger({
  level: NODE_ENV === "production" ? "info" : "debug",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.simple()
    })
  ]
});

// ===================== DATABASE SETUP =====================
const dynamoClient = new DynamoDBClient({
  region: AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
});

const docClient = DynamoDBDocumentClient.from(dynamoClient);
const TABLE_NAME = DYNAMODB_TABLE_NAME || "outfitz-jobs";

// Database helper functions - Fixed for PK/SK structure
const db = {
  async createJob(jobData) {
    const timestamp = new Date().toISOString();
    const params = {
      TableName: TABLE_NAME,
      Item: {
        PK: `USER#${jobData.userId}`,
        SK: `JOB#${timestamp}#${jobData.id}`,
        // Store all job data as attributes
        jobId: jobData.id,
        userId: jobData.userId,
        workflow: jobData.workflow,
        status: jobData.status,
        priority: jobData.priority,
        input: jobData.input,
        createdAt: timestamp,
      }
    };
    await docClient.send(new PutCommand(params));
    return jobData;
  },

  async getJob(jobId, userId = null) {
    // Since we can't directly query by jobId with this structure,
    // we need to query all user jobs and find the specific one
    if (!userId) {
      throw new Error('userId is required to get job with current table structure');
    }

    const params = {
      TableName: TABLE_NAME,
      KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
      ExpressionAttributeValues: {
        ':pk': `USER#${userId}`,
        ':sk': 'JOB#'
      }
    };
    
    const result = await docClient.send(new QueryCommand(params));
    const job = result.Items?.find(item => item.jobId === jobId);
    
    return job || null;
  },

  async updateJobWithUser(jobId, userId, updates) {
    // First find the job to get its SK
    const job = await this.getJob(jobId, userId);
    if (!job) {
      throw new Error('Job not found');
    }

    const updateExpression = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};
    
    Object.keys(updates).forEach((key, index) => {
      const attrName = `#attr${index}`;
      const attrValue = `:val${index}`;
      
      updateExpression.push(`${attrName} = ${attrValue}`);
      expressionAttributeNames[attrName] = key;
      expressionAttributeValues[attrValue] = updates[key];
    });

    const params = {
      TableName: TABLE_NAME,
      Key: {
        PK: job.PK,
        SK: job.SK
      },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
    };

    await docClient.send(new UpdateCommand(params));
  },

  async getUserJobs(userId, limit = 20, lastKey = null) {
    const params = {
      TableName: TABLE_NAME,
      KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
      ExpressionAttributeValues: {
        ':pk': `USER#${userId}`,
        ':sk': 'JOB#'
      },
      ScanIndexForward: false, // Sort by SK descending (newest first)
      Limit: limit
    };

    if (lastKey) {
      params.ExclusiveStartKey = lastKey;
    }

    const result = await docClient.send(new QueryCommand(params));
    return {
      jobs: result.Items || [],
      lastKey: result.LastEvaluatedKey
    };
  },

  async createUser(userData) {
    const params = {
      TableName: TABLE_NAME,
      Item: {
        PK: `USER#${userData.id}`,
        SK: 'PROFILE',
        userId: userData.id,
        ...userData,
        createdAt: new Date().toISOString(),
      }
    };
    await docClient.send(new PutCommand(params));
    return userData;
  },

  async getUser(userId) {
    const params = {
      TableName: TABLE_NAME,
      Key: {
        PK: `USER#${userId}`,
        SK: 'PROFILE'
      }
    };
    
    const result = await docClient.send(new GetCommand(params));
    return result.Item || null;
  }
};

// ===================== REDIS & QUEUE =====================
const redis = createClient({
  url: REDIS_URL,
  socket: {
    connectTimeout: 60000,
    commandTimeout: 5000,
  },
  retry_strategy: (times) => Math.min(times * 50, 2000)
});

redis.on('error', (err) => logger.error('Redis Client Error', err));
redis.on('connect', () => logger.info('Redis Client Connected'));

await redis.connect();

// Job Queue Setup
const jobQueue = new Queue('runpod-jobs', {
  connection: redis,
  defaultJobOptions: {
    attempts: parseInt(QUEUE_MAX_RETRIES),
    backoff: {
      type: 'exponential',
      delay: 2000,
    },
    removeOnComplete: 100,
    removeOnFail: 50,
  },
});

const queueEvents = new QueueEvents('runpod-jobs', { connection: redis });

// Queue event handlers
queueEvents.on('completed', async ({ jobId, returnvalue, data }) => {
  logger.info(`Job ${jobId} completed`);
  try {
    // Extract userId from job data since we need it for the update
    const userId = data?.userId;
    if (userId) {
      await db.updateJobWithUser(jobId, userId, {
        status: 'COMPLETED',
        result: returnvalue,
        completedAt: new Date().toISOString(),
      });
    } else {
      logger.error(`No userId found for completed job ${jobId}`);
    }
  } catch (error) {
    logger.error(`Failed to update completed job ${jobId}:`, error);
  }
});

queueEvents.on('failed', async ({ jobId, failedReason, data }) => {
  logger.error(`Job ${jobId} failed:`, failedReason);
  try {
    // Extract userId from job data since we need it for the update
    const userId = data?.userId;
    if (userId) {
      await db.updateJobWithUser(jobId, userId, {
        status: 'FAILED',
        error: failedReason,
        completedAt: new Date().toISOString(),
      });
    } else {
      logger.error(`No userId found for failed job ${jobId}`);
    }
  } catch (error) {
    logger.error(`Failed to update failed job ${jobId}:`, error);
  }
});

// ===================== JOB PROCESSOR =====================
async function buildRunpodPayload(input) {
  const { workflow, files = [], prompt, ...otherInputs } = input;

  if (workflow === 'API_AVATAR_WF') {
    // Skip file upload, use existing files on RunPod
    return {
      input: {
        workflow_path: "/workspace/OUTFITZ/01-Workflows/API_AVATAR_WF.json",
        set_nodes: {
          "12.image_path": "/workspace/memoSelf.jpg",  // Use existing file
          "120.image_path": "/workspace/pose-input-man.png",
          "63.text": prompt || "",
          "72.text": "close-up, close shot, close up shot, (worst quality, low quality, normal quality, lowres, low details, oversaturated, undersaturated, overexposed, underexposed, grayscale, bw, bad photo, bad photography, bad art:1.4), (watermark, signature, text font, username, error, logo, words, letters, digits, autograph, trademark, name:1.2), (blur, blurry, grainy), morbid, ugly, asymmetrical, mutated malformed, mutilated, poorly lit, bad shadow, draft, cropped, out of frame, cut off, censored, jpeg artifacts, out of focus, glitch, duplicate, (airbrushed, cartoon, anime, semi-realistic, cgi, render, blender, digital art, manga, amateur:1.3), (3D ,3D Game, 3D Game Scene, 3D Character:1.1), (bad hands, bad anatomy, bad body, bad face, bad teeth, bad arms, bad legs, deformities:1.3), anime, cartoon, graphic, (blur, blurry, bokeh), text, painting, crayon, graphite, abstract, glitch, deformed, mutated, ugly, disfigured"
        },
        completion_nodes: ["132", "133"],
        complete_when_any: false
      }
    };
  }

  return {
    input: {
      workflow_path: input.workflow_path,
      set_nodes: input.set_nodes || {},
      completion_nodes: input.completion_nodes || ["132", "133"],
      complete_when_any: input.complete_when_any || false,
      ...otherInputs
    }
  };
}

async function uploadFileToRunpod(file) {
  const formData = new FormData();
  const buffer = Buffer.from(file.base64, 'base64');
  const blob = new Blob([buffer], { type: file.mime });
  
  formData.append('file', blob, file.name);

  const response = await fetch(`${RUNPOD_PROXY_BASE}/upload`, {
    method: 'POST',
    body: formData,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`File upload failed: ${response.status} - ${errorText}`);
  }

  const result = await response.json();
  return result.filename;
}

async function pollRunpodForCompletion(runpodJobId, maxAttempts = 180) {
  const delay = promisify(setTimeout);
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await fetch(`${RUNPOD_PROXY_BASE}${RUNPOD_PROXY_STATUS_PATH}/${runpodJobId}`);
      
      if (!response.ok) {
        throw new Error(`Status check failed: ${response.status}`);
      }

      const status = await response.json();
      
      logger.debug(`Job ${runpodJobId} status check ${attempt}: ${status.status}`);

      if (status.status === 'COMPLETED') {
        return status.output || status;
      }
      
      if (status.status === 'FAILED') {
        throw new Error(`RunPod job failed: ${JSON.stringify(status.error || status)}`);
      }

      const waitTime = Math.min(2000 + (attempt * 100), 5000);
      await delay(waitTime);
      
    } catch (error) {
      logger.error(`Status check attempt ${attempt} failed:`, error);
      
      if (attempt === maxAttempts) {
        throw new Error(`Job polling failed after ${maxAttempts} attempts: ${error.message}`);
      }
      
      await delay(5000);
    }
  }
  
  throw new Error(`Job did not complete within timeout (${maxAttempts} attempts)`);
}

// Worker
const worker = new Worker('runpod-jobs', async (job) => {
  const { jobId, userId, input } = job.data;
  
  logger.info(`Processing job ${jobId} for user ${userId}`);
  
  try {
    await db.updateJobWithUser(jobId, userId, { 
      status: 'PROCESSING',
      processingStartedAt: new Date().toISOString()
    });

    const runpodBody = await buildRunpodPayload(input);
    
    const response = await fetch(`${RUNPOD_PROXY_BASE}${RUNPOD_PROXY_RUN_PATH}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(runpodBody),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`RunPod submission failed: ${response.status} - ${errorText}`);
    }

    const runpodResult = await response.json();
    const runpodJobId = runpodResult.id;

    if (!runpodJobId) {
      throw new Error('RunPod did not return job ID');
    }

    await db.updateJobWithUser(jobId, userId, { 
      runpodJobId,
      status: 'SUBMITTED_TO_RUNPOD'
    });

    const result = await pollRunpodForCompletion(runpodJobId);
    
    logger.info(`Job ${jobId} completed successfully`);
    return result;

  } catch (error) {
    logger.error(`Job ${jobId} processing failed:`, error);
    throw error;
  }
}, {
  connection: redis,
  concurrency: parseInt(QUEUE_CONCURRENCY),
});

// ===================== EXPRESS APP =====================
const app = express();

app.set('trust proxy', parseInt(TRUSTED_PROXIES));
app.use(helmet());

const allowedOrigins = CORS_ORIGINS.split(',').map(s => s.trim()).filter(Boolean);
app.use(cors({
  origin: function (origin, callback) {
    if (!origin) return callback(null, true);
    if (allowedOrigins.length === 0 || allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    callback(new Error('Not allowed by CORS'));
  },
  credentials: true,
}));

const limiter = rateLimit({
  windowMs: parseInt(RATE_LIMIT_WINDOW_MS),
  max: parseInt(RATE_LIMIT_MAX),
  message: { error: 'Too many requests, please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});
app.use('/api/', limiter);

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

app.use((req, res, next) => {
  logger.info(`${req.method} ${req.path}`, {
    ip: req.ip,
    userAgent: req.get('User-Agent'),
  });
  next();
});

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024, files: 5 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype.startsWith('image/')) {
      cb(null, true);
    } else {
      cb(new Error('Only image files are allowed'), false);
    }
  },
});

// ===================== AUTH MIDDLEWARE =====================
function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid token' });
    }
    req.user = user;
    next();
  });
}

const userLimiter = rateLimit({
  windowMs: parseInt(RATE_LIMIT_WINDOW_MS),
  max: parseInt(RATE_LIMIT_MAX),
  keyGenerator: (req) => req.user?.id || req.ip,
  message: { error: 'Too many requests from this user, please try again later.' },
});

// ===================== ROUTES =====================

// Health check
app.get('/health', async (req, res) => {
  try {
    await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { PK: 'HEALTH_CHECK', SK: 'TEST' }
    }));
    
    await redis.ping();
    
    const waiting = await jobQueue.getWaiting();
    const active = await jobQueue.getActive();
    
    res.json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      database: 'connected',
      redis: 'connected',
      queue: {
        waiting: waiting.length,
        active: active.length,
      },
    });
  } catch (error) {
    logger.error('Health check failed:', error);
    res.status(503).json({
      status: 'unhealthy',
      error: error.message,
    });
  }
});

// Authentication
app.post('/api/auth/token', async (req, res) => {
  try {
    const { userId } = req.body;
    
    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    // Create user if doesn't exist
    let user = await db.getUser(userId);
    if (!user) {
      user = await db.createUser({
        id: userId,
        totalJobs: 0,
        monthlyJobsUsed: 0,
        monthlyJobsLimit: 100,
        subscriptionTier: 'free',
      });
    }

    const token = jwt.sign(
      { id: userId, type: 'user' },
      JWT_SECRET,
      { expiresIn: '7d' }
    );

    res.json({ token, expiresIn: '7d' });
  } catch (error) {
    logger.error('Token generation failed:', error);
    res.status(500).json({ error: 'Token generation failed' });
  }
});

// Submit job
app.post('/api/jobs', authenticateToken, userLimiter, upload.any(), async (req, res) => {
  try {
    const userId = req.user.id;
    const isMultipart = req.is('multipart/form-data');
    
    let input = {};
    if (isMultipart) {
      input = req.body.input ? JSON.parse(req.body.input) : req.body;
      
      if (req.files && req.files.length > 0) {
        input.files = req.files.map(file => ({
          field: file.fieldname,
          name: file.originalname,
          mime: file.mimetype,
          size: file.size,
          base64: file.buffer.toString('base64'),
        }));
      }
    } else {
      input = req.body.input || req.body;
    }

    const { workflow, priority = 'basic' } = input;

    if (!workflow) {
      return res.status(400).json({ error: 'Workflow is required' });
    }

    const jobId = uuidv4();
    
    const job = await db.createJob({
      id: jobId,
      userId,
      workflow,
      status: 'PENDING',
      priority,
      input,
    });

    await jobQueue.add('process-runpod-job', {
      jobId,
      userId,
      input,
    }, {
      priority: priority === 'premium' ? 10 : 5,
      jobId,
    });

    logger.info(`Job ${jobId} submitted for user ${userId}`);

    res.json({
      jobId,
      status: 'PENDING',
    });

  } catch (error) {
    logger.error('Job submission failed:', error);
    res.status(500).json({ 
      error: 'Job submission failed',
      details: NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Get job status
app.get('/api/jobs/:jobId', authenticateToken, async (req, res) => {
  try {
    const { jobId } = req.params;
    const userId = req.user.id;

    const job = await db.getJob(jobId, userId);

    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    let queuePosition = null;
    if (job.status === 'PENDING') {
      try {
        const queueJob = await jobQueue.getJob(jobId);
        if (queueJob) {
          const waiting = await jobQueue.getWaiting();
          queuePosition = waiting.findIndex(j => j.id === jobId) + 1;
        }
      } catch (err) {
        logger.warn(`Could not get queue position for job ${jobId}:`, err);
      }
    }

    res.json({
      jobId: job.jobId,
      status: job.status,
      workflow: job.workflow,
      priority: job.priority,
      createdAt: job.createdAt,
      processingStartedAt: job.processingStartedAt,
      completedAt: job.completedAt,
      queuePosition,
      result: job.status === 'COMPLETED' ? job.result : null,
      error: job.status === 'FAILED' ? job.error : null,
    });

  } catch (error) {
    logger.error('Job status fetch failed:', error);
    res.status(500).json({ error: 'Failed to fetch job status' });
  }
});

// Get job result
app.get('/api/jobs/:jobId/result', authenticateToken, async (req, res) => {
  try {
    const { jobId } = req.params;
    const userId = req.user.id;

    const job = await db.getJob(jobId, userId);

    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    if (job.status !== 'COMPLETED') {
      return res.json({
        ready: false,
        status: job.status,
        error: job.error,
      });
    }

    const result = job.result || {};
    let response = { ready: true, raw: result };

    if (job.workflow === 'API_AVATAR_WF') {
      const outputPerNode = result.outputPerNode || {};
      
      response.avatarUrl = outputPerNode['132']?.[0]?.url || null;
      response.avatarNoBgUrl = outputPerNode['133']?.[0]?.url || null;
      
      if (!response.avatarUrl || !response.avatarNoBgUrl) {
        const extractUrl = (nodeId) => {
          return outputPerNode[nodeId]?.[0]?.url || 
                 outputPerNode[String(nodeId)]?.[0]?.url || 
                 null;
        };
        
        response.avatarUrl = response.avatarUrl || extractUrl(132);
        response.avatarNoBgUrl = response.avatarNoBgUrl || extractUrl(133);
      }
    }

    res.json(response);

  } catch (error) {
    logger.error('Job result fetch failed:', error);
    res.status(500).json({ error: 'Failed to fetch job result' });
  }
});

// List user jobs
app.get('/api/jobs', authenticateToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const { limit = 20 } = req.query;
    
    const result = await db.getUserJobs(userId, parseInt(limit));

    res.json({
      jobs: result.jobs.map(job => ({
        jobId: job.jobId,
        workflow: job.workflow,
        status: job.status,
        priority: job.priority,
        createdAt: job.createdAt,
        completedAt: job.completedAt,
        error: job.error,
      })),
      hasMore: !!result.lastKey
    });

  } catch (error) {
    logger.error('Jobs list fetch failed:', error);
    res.status(500).json({ error: 'Failed to fetch jobs' });
  }
});

// Error handling
app.use((error, req, res, next) => {
  logger.error('Unhandled error:', error);
  
  if (error instanceof multer.MulterError) {
    if (error.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'File too large (max 15MB)' });
    }
    if (error.code === 'LIMIT_FILE_COUNT') {
      return res.status(400).json({ error: 'Too many files (max 5)' });
    }
  }
  
  res.status(500).json({ 
    error: 'Internal server error',
    details: NODE_ENV === 'development' ? error.message : undefined
  });
});

app.use('*', (req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down gracefully...');
  
  try {
    await worker.close();
    await redis.quit();
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown:', error);
    process.exit(1);
  }
});

// Start server
const server = app.listen(PORT, () => {
  logger.info(`Server running on port ${PORT}`, {
    environment: NODE_ENV,
    queueConcurrency: QUEUE_CONCURRENCY,
    runpodBase: RUNPOD_PROXY_BASE,
    dynamoTable: TABLE_NAME,
  });
});

export default app;

