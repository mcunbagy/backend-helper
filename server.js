// server.js
import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
import fetch from "node-fetch";
import { v4 as uuidv4 } from "uuid";

// ——— ENV ———
const {
  RUNPOD_MODE = "proxy",                 // "serverless" | "proxy"
  RUNPOD_ENDPOINT_ID = "",               // serverless ise zorunlu
  RUNPOD_TOKEN = "",                     // serverless ise zorunlu
  RUNPOD_PROXY_BASE = "",                // proxy ise zorunlu, ör: https://<pod>-3000.proxy.runpod.net
  RUNPOD_PROXY_RUN_PATH = "/run",
  RUNPOD_PROXY_STATUS_PATH = "/status",

  PORT = 3000,
  CORS_ORIGINS = "",
  HELPER_APP_TOKEN = "",
} = process.env;

// Modlara göre basit doğrulama
if (RUNPOD_MODE === "serverless") {
  if (!RUNPOD_ENDPOINT_ID || !RUNPOD_TOKEN) {
    console.error("[FATAL] serverless mode needs RUNPOD_ENDPOINT_ID + RUNPOD_TOKEN");
    process.exit(1);
  }
} else if (RUNPOD_MODE === "proxy") {
  if (!RUNPOD_PROXY_BASE) {
    console.error("[FATAL] proxy mode needs RUNPOD_PROXY_BASE");
    process.exit(1);
  }
} else {
  console.error("[FATAL] RUNPOD_MODE must be 'serverless' or 'proxy'");
  process.exit(1);
}

// Modlara göre URL’ler
const RUNPOD_RUN = RUNPOD_MODE === "proxy"
  ? `${RUNPOD_PROXY_BASE}${RUNPOD_PROXY_RUN_PATH}`
  : `https://api.runpod.ai/v2/${RUNPOD_ENDPOINT_ID}/run`;

const RUNPOD_STATUS = RUNPOD_MODE === "proxy"
  ? `${RUNPOD_PROXY_BASE}${RUNPOD_PROXY_STATUS_PATH}`
  : `https://api.runpod.ai/v2/${RUNPOD_ENDPOINT_ID}/status`;

// ——— App ———
const app = express();
app.use(express.json({ limit: "25mb" }));

// CORS
const allowList = CORS_ORIGINS.split(",").map(s => s.trim()).filter(Boolean);
app.use(cors({
  origin(origin, cb) {
    if (!origin) return cb(null, true);
    if (allowList.length === 0 || allowList.includes(origin)) return cb(null, true);
    return cb(new Error("Not allowed by CORS"));
  }
}));
app.options("*", cors());

// Token koruması
app.use((req, res, next) => {
  if (!HELPER_APP_TOKEN) return next();
  const token =
    req.header("x-helper-token") ||
    req.header("authorization")?.replace(/^Bearer\s+/i, "");
  if (token && token === HELPER_APP_TOKEN) return next();
  return res.status(401).json({ ok: false, error: "unauthorized" });
});

// Upload (form-data) – memory
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 },
});

// ——— In‑Memory Job Store + Queues ———
const JOBS = new Map();   // id -> { state, input, runpodJobId, result, error, createdAt, priority }
const premiumQ = [];
const basicQ = [];
const CONCURRENCY = 1;
let running = 0;

function enqueue(job) {
  (job.priority === "premium" ? premiumQ : basicQ).push(job);
}
function nextJob() {
  return premiumQ.shift() || basicQ.shift() || null;
}

// ——— Helper: proxy’ye dosya yükle (yerleşik FormData/Blob ile) ———
async function uploadToProxyAsFilename(proxyBase, fieldName, buf, filename, mime) {
  const fd = new FormData(); // Node 18+ global
  const blob = new Blob([buf], { type: mime || "application/octet-stream" });
  fd.append("file", blob, filename);    // backend /upload 'file' alanını bekliyor
  // Content-Type başlığını ELLE ekleme; boundary’yi fetch kendi koyar.
  const r = await fetch(`${proxyBase}/upload`, { method: "POST", body: fd });
  const t = await r.text();
  if (!r.ok) throw new Error(`proxy upload failed ${r.status} ${t}`);
  const j = JSON.parse(t);
  if (!j?.ok || !j?.filename) throw new Error(`proxy upload bad payload ${t}`);
  return j.filename; // Comfy input klasörüne yazılmış nihai isim (örn. in_*.jpg)
}

function pickFile(files, ...names) {
  return (files || []).find(f => names.includes(f.field));
}

/**
 * Avatar sözleşmesi:
 *  - workflow_path: /workspace/OUTFITZ/01-Workflows/API_AVATAR_WF.json
 *  - set_nodes:
 *      "63.text"        => prompt
 *      "12.image"       => selfie (PROXY’ye upload edilip dosya adı verilir)
 *      "120.image_path" => sabit pose dosyası
 *  - return_nodes: [71, 130]
 */
async function buildRunpodBodyForAvatar(jobInput = {}) {
  const files = jobInput.files || [];
  const selfie = pickFile(files, "selfie", "fullshot");
  if (!selfie) throw new Error("selfie/fullshot image required");

  // 1) selfie’yi proxy’ye yükle -> Comfy input’a düşsün, bize dosya adı dönsün
  const uploadedName = await uploadToProxyAsFilename(
    RUNPOD_PROXY_BASE,
    "file",
    Buffer.from(selfie.base64, "base64"),
    selfie.name || "selfie.jpg",
    selfie.mime || "image/jpeg"
  );

  const prompt =
    jobInput.prompt ||
    jobInput.PROMPT_MAIN ||
    jobInput?.input?.prompt ||
    "";

  return {
    input: {
      workflow_path: "/workspace/OUTFITZ/01-Workflows/API_AVATAR_WF.json",
      set_nodes: {
        "63.text": String(prompt || ""),
        "12.image": uploadedName, // Load Image node filename bekliyor
        "120.image_path": "/workspace/OUTFITZ/01-Workflows/pose-input.png",
      },
      return_nodes: [71, 130],
    },
    stream: false,
  };
}

async function dispatchToRunpod(jobId) {
  const job = JOBS.get(jobId);
  if (!job) return;

  try {
    JOBS.set(jobId, { ...job, state: "QUEUED" });

    // body hazırlama
    let body;
    const wf = (job.input?.workflow || "").toUpperCase();
    if (wf === "API_AVATAR_WF") {
      body = await buildRunpodBodyForAvatar(job.input);
    } else {
      body = { input: job.input, stream: false };
    }

    console.log("[runpod body keys]", {
      mode: RUNPOD_MODE,
      run_url: RUNPOD_RUN,
      workflow_path: body?.input?.workflow_path,
      has_set_nodes: !!body?.input?.set_nodes,
      return_nodes: body?.input?.return_nodes,
      set_nodes_keys: body?.input?.set_nodes ? Object.keys(body.input.set_nodes) : [],
    });

    // header (proxy’de Authorization yok)
    const headers = { "Content-Type": "application/json" };
    if (RUNPOD_MODE !== "proxy") {
      headers.Authorization = `Bearer ${RUNPOD_TOKEN}`;
    }

    const rpRes = await fetch(RUNPOD_RUN, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    if (!rpRes.ok) {
      const txt = await rpRes.text();
      throw new Error(`RunPod /run failed: ${rpRes.status} ${txt}`);
    }

    const rpJson = await rpRes.json(); // proxy: { id } döndürüyoruz bridge.py'de
    const remoteId = rpJson?.id || rpJson?.jobId || rpJson?.runId || null;
    if (!remoteId) {
      throw new Error("RunPod /run: missing remote job id");
    }

    JOBS.set(jobId, {
      ...JOBS.get(jobId),
      state: "SUBMITTED",
      runpodJobId: remoteId,
    });
  } catch (err) {
    console.error("[dispatch error]", err);
    JOBS.set(jobId, {
      ...JOBS.get(jobId),
      state: "FAILED",
      error: String(err?.message || err),
    });
  }
}

async function workerTick() {
  if (running >= CONCURRENCY) return;
  const job = nextJob();
  if (!job) return;
  running++;
  try {
    await dispatchToRunpod(job.id);
  } finally {
    running--;
  }
}
setInterval(workerTick, 300);

// ——— Health ———
app.get("/health", (_req, res) => res.json({ ok: true, up: true }));

// ——— Start Job ———
app.post("/jobs/start", upload.any(), async (req, res) => {
  try {
    const isForm = req.is("multipart/form-data");
    const workflow = isForm ? req.body?.workflow : req.body?.workflow;
    const priority = (isForm ? req.body?.priority : req.body?.priority) === "premium" ? "premium" : "basic";

    if (!workflow) {
      return res.status(400).json({ ok: false, error: "workflow required" });
    }

    let input;
    if (isForm) {
      const raw = req.body?.input;
      input = raw ? JSON.parse(raw) : {};
    } else {
      input = req.body?.input || {};
    }

    // dosyaları base64’e koyup input.files’e ekle
    const files = (req.files || []).map(f => ({
      field: f.fieldname,
      name: f.originalname,
      mime: f.mimetype,
      base64: f.buffer.toString("base64"),
    }));
    if (files.length) input.files = files;

    input.workflow = workflow;

    const id = uuidv4();
    JOBS.set(id, {
      id,
      createdAt: Date.now(),
      state: "PENDING",
      priority,
      input,
    });

    enqueue({ id, priority });

    res.json({ ok: true, jobId: id });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// ——— Status ———
app.get("/jobs/:id/status", async (req, res) => {
  try {
    const job = JOBS.get(req.params.id);
    if (!job) return res.status(404).json({ ok: false, error: "not found" });

    // RunPod'a gönderilmeden önceki haller
    if (!job.runpodJobId) {
      return res.json({
        ok: true,
        state: job.state,
        local: true,
        error: job.error || null,
        workflow: job.input?.workflow || null,
        hasFiles: !!job.input?.files?.length,
      });
    }

    // RunPod status
    const statusUrl = `${RUNPOD_STATUS}/${job.runpodJobId}`;
    const statusHeaders = {};
    if (RUNPOD_MODE !== "proxy") {
      statusHeaders.Authorization = `Bearer ${RUNPOD_TOKEN}`;
    }

    const rp = await fetch(statusUrl, { headers: statusHeaders });
    if (!rp.ok) {
      const txt = await rp.text();
      throw new Error(`RunPod /status failed: ${rp.status} ${txt}`);
    }

    const js = await rp.json(); // { status: IN_QUEUE|IN_PROGRESS|COMPLETED|FAILED, output?... }
    const state = js.status;
    if (state === "COMPLETED") {
      JOBS.set(job.id, { ...job, state: "COMPLETED", result: js.output || js });
    } else if (state === "FAILED") {
      JOBS.set(job.id, { ...job, state: "FAILED", error: js });
    }
    res.json({ ok: true, state, raw: js });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// ——— Result ———
app.get("/jobs/:id/result", async (req, res) => {
  try {
    const job = JOBS.get(req.params.id);
    if (!job) return res.status(404).json({ ok: false, error: "not found" });
    if (job.state !== "COMPLETED") {
      return res.json({ ok: true, done: false });
    }

    const out = job.result || {};
    let avatarUrl = null;
    let cutoutUrl = null;

    if (out.outputPerNode) {
      avatarUrl = out.outputPerNode?.["71"]?.[0]?.url || out.outputPerNode?.[71]?.[0]?.url || null;
      cutoutUrl = out.outputPerNode?.["130"]?.[0]?.url || out.outputPerNode?.[130]?.[0]?.url || null;
    }

    if ((!avatarUrl || !cutoutUrl) && Array.isArray(out.images)) {
      const byNode = (id) => out.images.find(img => String(img.node_id || img.nodeId) === String(id))?.url || null;
      avatarUrl = avatarUrl || byNode(71);
      cutoutUrl = cutoutUrl || byNode(130);
    }

    avatarUrl = avatarUrl || out.AVATAR_URL || out.outputs?.avatarUrl || out.image_url || out.url || out.outputs?.[0]?.url || null;
    cutoutUrl = cutoutUrl || out.AVATAR_CUTOUT_URL || out.outputs?.avatarCutoutUrl || null;

    res.json({ ok: true, done: true, avatarUrl, avatarCutoutUrl: cutoutUrl, raw: out });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// ——— Start ———
app.listen(PORT, () => {
  console.log(`[helper] listening on :${PORT}`);
  console.log(`[helper] mode=${RUNPOD_MODE} run=${RUNPOD_RUN}`);
});
