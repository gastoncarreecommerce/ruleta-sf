// api/ruleta-sfmc.js
import crypto from 'crypto';

export default async function handler(req, res) {
  // CORS (en prod podés limitar a tu dominio)
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,GET,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();

  try {
    // 1) Leer parámetros
    const url = new URL(req.url, `http://${req.headers.host}`);
    const q = Object.fromEntries(url.searchParams.entries());

    // Defaults
    let email = null;
    let discount = q.discount != null ? Number(q.discount) : null;
    const campaign = q.campaign || 'default';
    let variationId = q.variationId || null;
    let result = (q.result || '').toLowerCase(); // 'won' | 'lost' (si viniera por query)

    if (req.method === 'POST') {
      const ctype = (req.headers['content-type'] || '').toLowerCase();

      if (ctype.includes('application/x-www-form-urlencoded')) {
        const bodyStr = await readBody(req);
        const params = new URLSearchParams(bodyStr);

        // Lo que ya mandaba DY + NUEVOS campos
        email = params.get('email') || email;
        if (params.has('discount') && (discount == null))
          discount = Number(params.get('discount'));
        if (params.has('variationId') && !variationId)
          variationId = params.get('variationId') || variationId;
        if (params.has('result') && !result)
          result = String(params.get('result') || '').toLowerCase();
      } else if (ctype.includes('application/json')) {
        const body = await parseJSON(req);
        email = body.email || email;
        if (body.discount != null && discount == null) discount = Number(body.discount);
        if (body.variationId && !variationId) variationId = body.variationId;
        if (body.result && !result) result = String(body.result).toLowerCase();
      }
    } else if (req.method === 'GET') {
      email = q.email || email;
    }

    // 2) Validaciones
    if (!email || !isValidEmail(email)) {
      return res.status(400).json({ ok: false, error: 'invalid_email' });
    }
    if (discount != null && (isNaN(discount) || discount < 0 || discount > 100)) {
      return res.status(400).json({ ok: false, error: 'invalid_discount' });
    }
    if (result && result !== 'won' && result !== 'lost') {
      // normalizamos cualquier valor raro
      result = '';
    }

    // Opcional: sólo guardar ganadores (activar con env var)
    if (process.env.SAVE_ONLY_WINNERS === '1' && result !== 'won') {
      return res.status(200).json({ ok: true, skipped: 'not_winner' });
    }

    // 3) Token
    const token = await getSfmcToken();

    // 4) Upsert en la DE (SOAP)
    const nowIso = new Date().toISOString();
    const hashed = sha256Lower(email);

    const RESULT_FIELD = process.env.SFMC_RESULT_FIELD || 'Result';
    const VAR_FIELD = process.env.SFMC_VARIATION_FIELD || 'VariationId';
    const values = {
      Email: email,
      Campaign: campaign,
      Discount: discount != null ? String(discount) : '',
      [RESULT_FIELD]: result || '',
      HashedEmail: hashed,
      Source: process.env.SFMC_SOURCE_VALUE || 'DY_Ruleta',
      Timestamp: nowIso,
      [VAR_FIELD]: variationId || ''
    };

    const soapResp = await soapUpsertRow({
      token,
      deKey: process.env.SFMC_DE_KEY,
      values
    });

    return res.status(200).json({
      ok: true,
      saved: {
        email,
        campaign,
        discount: values.Discount,
        result: values[RESULT_FIELD],
        variationId: values[VAR_FIELD]
      },
      soap: { status: soapResp.status }
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: 'server_error', detail: String(err?.message || err) });
  }
}

/* ========== helpers ========== */

function isValidEmail(email) {
  return /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-z\-0-9]+\.)+[a-z]{2,}))$/i.test(email);
}

async function readBody(req) {
  const buffers = [];
  for await (const chunk of req) buffers.push(chunk);
  return Buffer.concat(buffers).toString('utf8');
}

async function parseJSON(req) {
  const str = await readBody(req);
  return str ? JSON.parse(str) : {};
}

async function getSfmcToken() {
  const authBase = (process.env.SFMC_AUTH_BASE || '').replace(/\/+$/, '');
  const body = {
    grant_type: 'client_credentials',
    client_id: process.env.SFMC_CLIENT_ID,
    client_secret: process.env.SFMC_CLIENT_SECRET
  };
  if (process.env.SFMC_ACCOUNT_ID) body.account_id = process.env.SFMC_ACCOUNT_ID;

  const r = await fetch(`${authBase}/v2/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(`Token error: ${r.status} ${await r.text()}`);
  const j = await r.json();
  if (!j.access_token) throw new Error('No access_token');
  return j.access_token;
}

async function soapUpsertRow({ token, deKey, values }) {
  if (!deKey) throw new Error('Missing SFMC_DE_KEY');
  const soapBase =
    process.env.SFMC_SOAP_BASE ||
    process.env.SFMC_REST_BASE.replace('rest.', 'soap.').replace(/\/+$/, '') + '/Service.asmx';

  const propsXML = Object.keys(values)
    .map(
      (k) => `
    <Property>
      <Name>${escapeXml(k)}</Name>
      <Value>${escapeXml(values[k] == null ? '' : String(values[k]))}</Value>
    </Property>`
    )
    .join('');

  const envelope = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:tns="http://exacttarget.com/wsdl/partnerAPI">
  <soapenv:Header>
    <fueloauth xmlns="http://exacttarget.com">${escapeXml(token)}</fueloauth>
  </soapenv:Header>
  <soapenv:Body>
    <tns:CreateRequest>
      <tns:Options>
        <tns:SaveOptions>
          <tns:SaveOption>
            <tns:PropertyName>*</tns:PropertyName>
            <tns:SaveAction>UpdateAdd</tns:SaveAction>
          </tns:SaveOption>
        </tns:SaveOptions>
      </tns:Options>
      <tns:Objects xsi:type="tns:DataExtensionObject">
        <tns:CustomerKey>${escapeXml(deKey)}</tns:CustomerKey>
        <tns:Properties>${propsXML}
        </tns:Properties>
      </tns:Objects>
    </tns:CreateRequest>
  </soapenv:Body>
</soapenv:Envelope>`;

  const r = await fetch(soapBase, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml; charset=utf-8',
      'SOAPAction': 'Create'
    },
    body: envelope
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`SOAP error ${r.status}: ${text}`);
  return { status: r.status, body: text };
}

function escapeXml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function sha256Lower(email) {
  return crypto.createHash('sha256').update(String(email).toLowerCase()).digest('hex');
}
