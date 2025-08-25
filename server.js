// server.js
import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
import fetch from "node-fetch";
import { v4 as uuidv4 } from "uuid";

// ——— ENV ———
const {
  RUNPOD_ENDPOINT_ID,
  RUNPOD_TOKEN,
  PORT = 3000,
  CORS_ORIGINS = "",
  HELPER_APP_TOKEN = "",
} = process.env;

if (!RUNPOD_ENDPOINT_ID || !RUNPOD_TOKEN) {
  console.error("[FATAL] Please set RUNPOD_ENDPOINT_ID and RUNPOD_TOKEN in .env");
  process.exit(1);
}

const RUNPOD_RUN    = `https://api.runpod.ai/v2/${RUNPOD_ENDPOINT_ID}/run`;
const RUNPOD_STATUS = `https://api.runpod.ai/v2/${RUNPOD_ENDPOINT_ID}/status`;

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
// (isteğe bağlı) preflight:
app.options("*", cors());

// Basit token koruması (istemci "x-helper-token" header'ı ile gelir)
app.use((req, res, next) => {
  if (!HELPER_APP_TOKEN) return next(); // token tanımlı değilse korumayı pas geç
  const token = req.header("x-helper-token") || req.header("authorization")?.replace(/^Bearer\s+/i, "");
  if (token && token === HELPER_APP_TOKEN) return next();
  return res.status(401).json({ ok: false, error: "unauthorized" });
});

// Upload (form-data) – files -> memory
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 15 * 1024 * 1024 } });

// ——— In‑Memory Job Store + Priority Queues ———
const JOBS = new Map();   // id -> { state, input, runpodJobId, result, error, createdAt, priority }
const premiumQ = [];
const basicQ   = [];
const CONCURRENCY = 1;    // MVP: tek worker. İstersen 2-3 yaparsın.
let running = 0;

function enqueue(job) {
  (job.priority === "premium" ? premiumQ : basicQ).push(job);
}
function nextJob() {
  return premiumQ.shift() || basicQ.shift() || null;
}

// ——— Helpers (API_AVATAR_WF için özel body) ———
function pickFile(files, ...names) {
  return (files || []).find(f => names.includes(f.field));
}

/**
 * Senin sözleşmene göre RunPod body’si:
 * - workflow_path: /workspace/OUTFITZ/01-Workflows/API_AVATAR_WF.json
 * - set_nodes:
 *    "63.text"           => prompt
 *    "12.image_base64"   => selfie (data URL)
 *    "120.image_base64"  => pose (opsiyonel, data URL)
 * - return_nodes: [71, 130]
 */
async function buildRunpodBodyForAvatar(jobInput = {}) {
  const files = jobInput.files || [];
  const selfie = (files || []).find(f => ["selfie", "fullshot"].includes(f.field));
  if (!selfie) throw new Error("selfie/fullshot image required");

  const prompt = jobInput.prompt || jobInput.PROMPT_MAIN || jobInput?.input?.prompt || "";

  return {
    input: {
      workflow_path: "/workspace/OUTFITZ/01-Workflows/API_AVATAR_WF.json",
      set_nodes: {
        // #63 CLIP Text Encode (Prompt).text
        "63.text": String(prompt || ""),

        // #12 Load Image  → selfie’yi data URL’den yazdıracak worker’a bırakıyoruz
        "12.image_base64": `data:${selfie.mime};base64,${selfie.base64}`,

        // #120 Load Image → sabit dosya (sen pod’da bu dosyayı yönetiyorsun)
        "120.image_path": "/workspace/OUTFITZ/01-Workflows/pose-input.png"
      },
      // Çıkışlar: #71 avatar (BG’li), #130 cutout (BG’siz)
      return_nodes: [71, 130]
    },
    stream: false
  };
}

async function dispatchToRunpod(jobId) {
  const job = JOBS.get(jobId);
  if (!job) return;

  try {
    JOBS.set(jobId, { ...job, state: "QUEUED" });

    // ——— RunPod run body ———
    let body;
    const wf = (job.input?.workflow || "").toUpperCase();
    if (wf === "API_AVATAR_WF") {
      body = await buildRunpodBodyForAvatar(job.input);
    } else {
      // Diğer iş akışları için genel geçer fallback (şimdilik)
      body = { input: job.input, stream: false };
    }

   console.log("[runpod body keys]", {
     workflow_path: body?.input?.workflow_path,
     has_set_nodes: !!body?.input?.set_nodes,
     return_nodes: body?.input?.return_nodes,
     set_nodes_keys: body?.input?.set_nodes ? Object.keys(body.input.set_nodes) : []
   });

    const rpRes = await fetch(RUNPOD_RUN, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${RUNPOD_TOKEN}`
      },
      body: JSON.stringify(body)
    });

    if (!rpRes.ok) {
      const txt = await rpRes.text();
      throw new Error(`RunPod /run failed: ${rpRes.status} ${txt}`); // 400/413 gibi
    }
    const rpJson = await rpRes.json(); // { id: "<runpodJobId>" }
    JOBS.set(jobId, { ...JOBS.get(jobId), state: "SUBMITTED", runpodJobId: rpJson?.id });
  } catch (err) {
   console.error("[dispatch error]", err);
   JOBS.set(jobId, { ...JOBS.get(jobId), state: "FAILED", error: String(err?.message || err) });
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
// JSON veya form-data (dosyalı) kabul ediyoruz:
// - JSON: { workflow, input, priority }
// - form: workflow, input(JSON string), priority, files...
app.post("/jobs/start", upload.any(), async (req, res) => {
  try {
    const isForm = req.is("multipart/form-data");
    const workflow = isForm ? req.body?.workflow : req.body?.workflow;
    const priority = (isForm ? req.body?.priority : req.body?.priority) === "premium" ? "premium" : "basic";

    if (!workflow) return res.status(400).json({ ok: false, error: "workflow required" });

    // input toparla
    let input;
    if (isForm) {
      const raw = req.body?.input;
      input = raw ? JSON.parse(raw) : {};
    } else {
      input = req.body?.input || {};
    }

    // dosyaları base64’e koyup input.files’e ekliyoruz
    const files = (req.files || []).map(f => ({
      field: f.fieldname,
      name: f.originalname,
      mime: f.mimetype,
      base64: f.buffer.toString("base64")
    }));
    if (files.length) {
      input.files = files; // RunPod worker set_nodes.*.image_base64 için data URL oluşturacağız
    }

    // ——— Workflow seçimi (adıyla) ———
    input.workflow = workflow;

    const id = uuidv4();
    JOBS.set(id, {
      id,
      createdAt: Date.now(),
      state: "PENDING",
      priority,
      input
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

    // RunPod'a gönderilmeden önceki haller: PENDING | QUEUED | SUBMITTED | FAILED
    if (!job.runpodJobId) {
     return res.json({
       ok: true,
       state: job.state,
       local: true,
       error: job.error || null,           // <— HATA METNİ
       workflow: job.input?.workflow || null,
       hasFiles: !!job.input?.files?.length
     });
    }

    // RunPod status
    const rp = await fetch(`${RUNPOD_STATUS}/${job.runpodJobId}`, {
      headers: { Authorization: `Bearer ${RUNPOD_TOKEN}` }
    });
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

    // Çıkış normalize – Comfy/worker çıkışına göre alanları deneriz
    const out = job.result || {};

    let avatarUrl = null;
    let cutoutUrl = null;

    // 1) Önerdiğimiz sözleşme: node-id bazlı output
    if (out.outputPerNode) {
      avatarUrl = out.outputPerNode?.["71"]?.[0]?.url || out.outputPerNode?.[71]?.[0]?.url || null;
      cutoutUrl = out.outputPerNode?.["130"]?.[0]?.url || out.outputPerNode?.[130]?.[0]?.url || null;
    }

    // 2) Alternatif: images listesinde node_id ile tutulan url’ler
    if ((!avatarUrl || !cutoutUrl) && Array.isArray(out.images)) {
      const byNode = (id) => out.images.find(img => String(img.node_id || img.nodeId) === String(id))?.url || null;
      avatarUrl = avatarUrl || byNode(71);
      cutoutUrl = cutoutUrl || byNode(130);
    }

    // 3) Son çare: farklı alan adları
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
  console.log(`[helper] runpod endpoint: ${RUNPOD_ENDPOINT_ID}`);
});
