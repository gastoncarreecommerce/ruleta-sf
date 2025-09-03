import crypto from "crypto";
import { put } from "@vercel/blob";

export default async function handler(req, res) {
  // --- CORS ---
  const allowed = process.env.ALLOWED_ORIGIN;
  const origin = req.headers.origin || "";
  if (req.method === "OPTIONS") {
    res.setHeader("Access-Control-Allow-Origin", allowed || "*");
    res.setHeader("Access-Control-Allow-Methods", "POST,GET,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    return res.status(204).end();
  }
  if (allowed) {
    if (origin === allowed) res.setHeader("Access-Control-Allow-Origin", allowed);
    else if (!origin) res.setHeader("Access-Control-Allow-Origin", "*");
    else return res.status(403).json({ ok: false, error: "forbidden_origin" });
  } else {
    res.setHeader("Access-Control-Allow-Origin", "*");
  }
  res.setHeader("Access-Control-Allow-Methods", "POST,GET,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    // Seguridad opcional
    const secret = url.searchParams.get("secret") || "";
    if (process.env.SHARED_SECRET && secret !== process.env.SHARED_SECRET) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    // Leer params (GET/POST x-www-form-urlencoded/JSON)
    let email = url.searchParams.get("email") || null;
    let discount = url.searchParams.get("discount");
    let result = (url.searchParams.get("result") || "").toLowerCase();
    let variationId = url.searchParams.get("variationId") || url.searchParams.get("variationid") || "";
    let campaign = url.searchParams.get("campaign") || "default";
    let source = url.searchParams.get("source") || "DY_Ruleta";

    if (req.method === "POST") {
      const ctype = (req.headers["content-type"] || "").toLowerCase();
      const bodyStr = await readBody(req);
      if (ctype.includes("application/x-www-form-urlencoded")) {
        const params = new URLSearchParams(bodyStr);
        email = params.get("email") || email;
        if (params.has("discount")) discount = params.get("discount");
        if (params.get("result")) result = String(params.get("result")).toLowerCase();
        variationId = params.get("variationId") || variationId;
        campaign = params.get("campaign") || campaign;
      } else if (ctype.includes("application/json")) {
        const body = bodyStr ? JSON.parse(bodyStr) : {};
        email = body.email || email;
        if (body.discount != null) discount = body.discount;
        if (body.result) result = String(body.result).toLowerCase();
        if (body.variationId) variationId = body.variationId;
        if (body.campaign) campaign = body.campaign;
      }
    }

    // Validaciones mínimas
    if (!email || !isValidEmail(email)) {
      return res.status(400).json({ ok: false, error: "invalid_email" });
    }
    if (discount != null && discount !== "") {
      const n = Number(discount);
      if (Number.isNaN(n) || n < 0 || n > 100) {
        return res.status(400).json({ ok: false, error: "invalid_discount" });
      }
      discount = String(n);
    } else {
      discount = "";
    }
    if (result && result !== "won" && result !== "lost") result = "";

    // Armar registro
    const timestamp = new Date().toISOString();
    const hashedEmail = sha256Lower(email);
    const ua = req.headers["user-agent"] || "";
    const referer = req.headers["referer"] || "";
    const id = crypto.randomUUID();

    const campaignSafe = String(campaign).toLowerCase().replace(/[^a-z0-9_-]/gi, "-");
    const day = timestamp.slice(0, 10); // YYYY-MM-DD
    const key = `ruleta/${campaignSafe}/${day}/${id}.json`;

    const record = {
      timestamp,
      email,
      campaign,
      discount,
      result,
      variationId,
      hashedEmail,
      source,
      userAgent: ua,
      referer
    };

    // Guardar en Blob (público por simplicidad de exportación)
    const { url: blobUrl } = await put(
      key,
      JSON.stringify(record) + "\n",
      {
        access: "public", // si prefieres privado, cambia a "private" y ajustamos export
        contentType: "application/json",
        addRandomSuffix: false,
        token: process.env.BLOB_READ_WRITE_TOKEN
      }
    );

    return res.status(200).json({ ok: true, saved: record, blobKey: key, url: blobUrl });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "server_error", detail: String(err?.message || err) });
  }
}

// Helpers
function isValidEmail(e) {
  return /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-z\-0-9]+\.)+[a-z]{2,}))$/i.test(e);
}
async function readBody(req) {
  const bufs = []; for await (const c of req) bufs.push(c); return Buffer.concat(bufs).toString("utf8");
}
function sha256Lower(email) {
  return crypto.createHash("sha256").update(String(email).toLowerCase()).digest("hex");
}
