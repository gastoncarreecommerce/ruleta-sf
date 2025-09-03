import { list } from "@vercel/blob";

// Exporta CSV de todos los registros guardados por /api/ruleta-blob
export default async function handler(req, res) {
  try {
    // CORS simple para poder bajar desde el navegador
    res.setHeader("Access-Control-Allow-Origin", "*");

    // Validar pre-requisitos
    const token = process.env.BLOB_READ_WRITE_TOKEN;
    if (!token) {
      return res.status(500).json({
        ok: false,
        error: "missing_env",
        detail: "Falta BLOB_READ_WRITE_TOKEN en Vercel → Project → Settings → Environment Variables",
      });
    }

    const url = new URL(req.url, `http://${req.headers.host}`);
    const campaign = (url.searchParams.get("campaign") || "").toLowerCase().replace(/[^a-z0-9_-]/gi, "-");
    const from = url.searchParams.get("from"); // YYYY-MM-DD (opcional)
    const to = url.searchParams.get("to");     // YYYY-MM-DD (opcional)

    const prefix = campaign ? `ruleta/${campaign}/` : "ruleta/";

    const rows = [];
    let cursor;

    do {
      const page = await list({ prefix, token, cursor });
      // Si no hay blobs, devolvemos CSV vacío con headers
      const blobs = page?.blobs || [];
      for (const b of blobs) {
        if (!b.pathname.endsWith(".json")) continue;

        // Filtrar por fecha (la fecha viene en el path: ruleta/<campaign>/<YYYY-MM-DD>/<uuid>.json)
        if (from || to) {
          const m = b.pathname.match(/\/(\d{4}-\d{2}-\d{2})\//);
          if (m) {
            const d = m[1];
            if (from && d < from) continue;
            if (to && d > to) continue;
          }
        }

        // Los blobs son públicos: b.url debería existir.
        // Fallback a b.downloadUrl por seguridad.
        const fetchUrl = b.url || b.downloadUrl;
        if (!fetchUrl) continue;

        const rec = await fetch(fetchUrl).then(r => {
          if (!r.ok) throw new Error(`fetch ${b.pathname} -> ${r.status}`);
          return r.json();
        }).catch(() => null);

        if (rec) {
          rows.push([
            rec.timestamp || "",
            rec.email || "",
            rec.campaign || "",
            rec.discount || "",
            rec.result || "",
            rec.variationId || "",
            rec.hashedEmail || "",
            rec.source || "",
            rec.userAgent || "",
            rec.referer || ""
          ]);
        }
      }
      cursor = page.cursor;
    } while (cursor);

    // Armar CSV
    const header = ["Timestamp","Email","Campaign","Discount","Result","VariationId","HashedEmail","Source","UserAgent","Referer"];
    const csv = [header, ...rows].map(cols => cols.map(csvEscape).join(",")).join("\n");

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="ruleta_${campaign || "all"}.csv"`);
    return res.status(200).send(csv);

  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      error: "server_error",
      detail: String(err?.message || err)
    });
  }
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
