import { list } from "@vercel/blob";

export default async function handler(req, res) {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const campaign = (url.searchParams.get("campaign") || "").toLowerCase().replace(/[^a-z0-9_-]/gi, "-");
    const from = url.searchParams.get("from"); // YYYY-MM-DD (opcional)
    const to   = url.searchParams.get("to");   // YYYY-MM-DD (opcional)

    // CORS simple (descarga desde browser si querés)
    res.setHeader("Access-Control-Allow-Origin", "*");

    const prefix = campaign ? `ruleta/${campaign}/` : "ruleta/";
    const token = process.env.BLOB_READ_WRITE_TOKEN;

    const items = [];
    let cursor;

    // Paginamos todo el prefijo y convertimos cada JSON en fila
    do {
      const page = await list({ prefix, token, cursor });
      for (const b of page.blobs || []) {
        if (!b.pathname.endsWith(".json")) continue;

        // Filtrar por fecha según path .../<YYYY-MM-DD>/...
        if (from || to) {
          const match = b.pathname.match(/\/(\d{4}-\d{2}-\d{2})\//);
          if (match) {
            const d = match[1];
            if (from && d < from) continue;
            if (to && d > to) continue;
          }
        }

        // Descargar JSON (blobs públicos)
        const fetchUrl = b.url || b.downloadUrl;
        const rec = await fetch(fetchUrl).then(r => r.json()).catch(() => null);
        if (rec) items.push(rec);
      }
      cursor = page.cursor;
    } while (cursor);

    // Armar CSV
    const header = ["Timestamp","Email","Campaign","Discount","Result","VariationId","HashedEmail","Source","UserAgent","Referer"];
    const csvLines = [header.join(",")];
    for (const r of items) {
      const row = [
        r.timestamp || "",
        r.email || "",
        r.campaign || "",
        r.discount || "",
        r.result || "",
        r.variationId || "",
        r.hashedEmail || "",
        r.source || "",
        r.userAgent || "",
        r.referer || ""
      ].map(csvEscape);
      csvLines.push(row.join(","));
    }
    const csv = csvLines.join("\n");

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="ruleta_${campaign || "all"}.csv"`);
    return res.status(200).send(csv);

  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok:false, error:"server_error", detail:String(err?.message || err) });
  }
}

function csvEscape(v) {
  const s = String(v ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
