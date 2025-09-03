// api/ruleta-sfmc.js
import crypto from 'crypto';

/**
 * Endpoint que recibe:
 *  - query:  campaign
 *  - body x-www-form-urlencoded | json: email, result (won|lost), discount (número), variationId
 * Guarda/actualiza fila en la Data Extension vía SOAP (UpdateAdd).
 */
export default async function handler(req, res) {
  // CORS (endurecer en prod si querés)
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,GET,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();

  try {
    // -------- 1) INPUTS --------
    const url = new URL(req.url, `http://${req.headers.host}`);
    const q = Object.fromEntries(url.searchParams.entries());

    let email = null;
    let discount = q.discount != null ? Number(q.discount) : null;
    const campaign = q.campaign || 'default';
    let variationId = q.variationId || null;
    let result = (q.result || '').toLowerCase(); // 'won' | 'lost'

    if (req.method === 'POST') {
      const ctype = (req.headers['content-type'] || '').toLowerCase();

      if (ctype.includes('application/x-www-form-urlencoded')) {
        const bodyStr = await readBody(req);
        const params = new URLSearchParams(bodyStr);

        email = params.get('email') || email;
        if (params.has('discount') && discount == null) discount = Number(params.get('discount'));
        if (params.has('variationId') && !variationId) variationId = params.get('variationId');
        if (params.has('result') && !result) result = String(params.get('result') || '').toLowerCase();
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

    // -------- 2) VALIDACIONES --------
    if (!email || !isValidEmail(email)) {
      return res.status(400).json({ ok: false, error: 'invalid_email' });
    }
    if (discount != null && (isNaN(discount) || discount < 0 || discount > 100)) {
      return res.status(400).json({ ok: false, error: 'invalid_discount' });
    }
    if (result && result !== 'won' && result !== 'lost') result = '';

    // Guardar solo ganadores (opcional)
    if (process.env.SAVE_ONLY_WINNERS === '1' && result !== 'won') {
      return res.status(200).json({ ok: true, skipped: 'not_winner' });
    }

    // -------- 3) TOKEN --------
    const token = await getSfmcToken();

    // -------- 4) UP SERT EN DE (SOAP) --------
    const nowIso = new Date().toISOString();
    const hashed = sha256Lower(email);

    const RESULT_FIELD = process.env.SFMC_RESULT_FIELD || 'Result';
    const VAR_FIELD    = process.env.SFMC_VARIATION_FIELD || 'VariationId';

    // Nunca mandamos Discount vacío para no pisar un valor previo en un "lost"
    const values = {
      Email: email,
      Campaign: campaign,
      HashedEmail: hashed,
      Source: process.env.SFMC_SOURCE_VALUE || 'DY_Ruleta',
      Timestamp: nowIso
    };
    if (!isNaN(discount) && discount !== null) values.Discount = String(discount);
    if (process.env.SFMC_SEND_RESULT !== '0')    values[RESULT_FIELD] = result || '';
    if (process.env.SFMC_SEND_VARIATION !== '0') values[VAR_FIELD]    = variationId || '';

    const soapResp = await soapUpsertRow({
      token,
      deKey: process.env.SFMC_DE_KEY,
      values
    });

    // Parseamos respuesta SOAP para detectar errores lógicos (aunque HTTP sea 200)
    const parsed = parseSoapCreateResponse(soapResp.body);
    if (!parsed.ok) {
      return res.status(502).json({ ok: false, error: 'sfmc_soap', detail: parsed });
    }

    return res.status(200).json({
      ok: true,
      saved: {
        email,
        campaign,
        discount: values.Discount ?? '',
        result: values[RESULT_FIELD] ?? '',
        variationId: values[VAR_FIELD] ?? ''
      },
      soap: {
        status: soapResp.status,
        requestId: parsed.requestId,
        overall: parsed.overall,
        statusMessage: parsed.status
      }
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: 'server_error', detail: String(err?.message || err) });
  }
}

/* ========== HELPERS ========== */

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

// Garantiza que el SOAP base termine en /Service.asmx, o lo deriva desde REST si faltara
function normalizeSoapBase(soapBaseEnv, restBaseEnv) {
  if (soapBaseEnv && soapBaseEnv.trim()) {
    const base = soapBaseEnv.replace(/\/+$/, '');
    return /\/Service\.asmx$/i.test(base) ? base : base + '/Service.asmx';
  }
  const rest = (restBaseEnv || '').replace(/\/+$/, '');
  if (!rest) throw new Error('Missing SFMC_SOAP_BASE and SFMC_REST_BASE');
  return rest.replace('rest.', 'soap.') + '/Service.asmx';
}

async function soapUpsertRow({ token, deKey, values }) {
  if (!deKey) throw new Error('Missing SFMC_DE_KEY');
  const soapBase = normalizeSoapBase(process.env.SFMC_SOAP_BASE, process.env.SFMC_REST_BASE);

  const propsXML = Object.keys(values)
    .map((k) => `
      <Property>
        <Name>${escapeXml(k)}</Name>
        <Value>${escapeXml(values[k] == null ? '' : String(values[k]))}</Value>
      </Property>`)
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

  const text = await r.text(); // SOAP devuelve 200 incluso con errores lógicos
  if (!r.ok) throw new Error(`SOAP error ${r.status}: ${text}`);
  return { status: r.status, body: text };
}

// Devuelve ok=false si OverallStatus/StatusMessage no son OK; trae RequestID y ErrorCode(s)
function parseSoapCreateResponse(xml) {
  const overall = (xml.match(/<OverallStatus>([^<]+)<\/OverallStatus>/i) || [])[1] || '';
  const status  = (xml.match(/<StatusMessage>([^<]+)<\/StatusMessage>/i) || [])[1] || '';
  const reqId   = (xml.match(/<RequestID>([^<]+)<\/RequestID>/i) || [])[1] || '';
  const codes   = Array.from(xml.matchAll(/<ErrorCode>([^<]+)<\/ErrorCode>/ig)).map(m => m[1]);
  const ok = /OK/i.test(overall) || /OK/i.test(status);
  return { ok, overall, status, requestId: reqId, errors: codes };
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
