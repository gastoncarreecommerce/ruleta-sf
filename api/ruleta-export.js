export default async function handler(req, res) {
  try {
    res.setHeader("Access-Control-Allow-Origin", "*");

    // Import dinámico del SDK sin token (proyecto vinculado al store)
    const { list } = await import("@vercel/blob");

    const url = new URL(req.url, `http://${req.headers.host}`);
    const campaign = (url.searchParams.get("campaign") || "").toLowerCase().replace(/[^a-z0-9_-]/gi, "-");
    const from = url.searchParams.get("from"); // YYYY-MM-DD
    const to   = url.searchParams.get("to");   // YYYY-MM-DD

    const prefix = campaign ? `ruleta/${campaign}/` : "ruleta/";

    const rows = [];
    let cursor;

    do {
      const page = await list({ prefix, cursor }); // sin token
      const blobs = page?.blobs || [];
      for (const b of blobs) {
        if (!b.pathname.endsWith(".json")) continue;

        if (from || to) {
          const m = b.pathname.match(/\/(\d{4}-\d{2}-\d{2})\//);
          if (m) {
            const d = m[1];
            if (from && d < from) continue;
            if (to && d > to) continue;
          }
        }

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

    const header = ["Timestamp","Email","Campaign","Discount","Result","VariationId","HashedEmail","Source","UserAgent","Referer"];
    const csv = [header, ...rows].map(cols => cols.map(csvEscape).join(",")).join("\n");

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
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
