// api/ruleta-sfmc.js
import crypto from 'crypto';

/** ENV requeridas:
 * SFMC_CLIENT_ID, SFMC_CLIENT_SECRET, SFMC_AUTH_BASE, SFMC_REST_BASE,
 * SFMC_SOAP_BASE (con /Service.asmx), SFMC_DE_KEY
 * Opcionales:
 * SFMC_ACCOUNT_ID (MID), SFMC_RESULT_FIELD, SFMC_VARIATION_FIELD, SFMC_SOURCE_VALUE,
 * SAVE_ONLY_WINNERS=1, SFMC_SEND_RESULT=0, SFMC_SEND_VARIATION=0,
 * SFMC_KEY_FIELDS="Email,Campaign"
 */

export default async function handler(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,GET,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();

  try {
    /* -------- 1) INPUTS -------- */
    const url = new URL(req.url, `http://${req.headers.host}`);
    const q = Object.fromEntries(url.searchParams.entries());

    let email = null;
    let discount = q.discount != null ? Number(q.discount) : null;
    const campaign = q.campaign || 'default';
    let variationId = q.variationId || null;
    let result = (q.result || '').toLowerCase(); // won|lost

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

    /* -------- 2) VALIDACIONES -------- */
    if (!email || !isValidEmail(email)) {
      return res.status(400).json({ ok: false, error: 'invalid_email' });
    }
    if (discount != null && (isNaN(discount) || discount < 0 || discount > 100)) {
      return res.status(400).json({ ok: false, error: 'invalid_discount' });
    }
    if (result && result !== 'won' && result !== 'lost') result = '';

    // Guardar solo ganadores (si se pide)
    if (process.env.SAVE_ONLY_WINNERS === '1' && result !== 'won') {
      return res.status(200).json({ ok: true, skipped: 'not_winner' });
    }

    /* -------- 3) TOKEN -------- */
    const token = await getSfmcToken();

    /* -------- 4) ARMAR VALORES -------- */
    const nowIso = new Date().toISOString();
    const hashed = sha256Lower(email);

    const RESULT_FIELD = process.env.SFMC_RESULT_FIELD || 'Result';
    const VAR_FIELD    = process.env.SFMC_VARIATION_FIELD || 'VariationId';

    // Campos a guardar (no incluimos Discount vacío)
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

    // Claves (por defecto Email + Campaign)
    const keyFields = (process.env.SFMC_KEY_FIELDS || 'Email,Campaign')
      .split(',')
      .map(s => s.trim())
      .filter(Boolean);
    const keyValues = {};
    for (const k of keyFields) keyValues[k] = values[k];

    const deKey = process.env.SFMC_DE_KEY;
    if (!deKey) throw new Error('Missing SFMC_DE_KEY');

    /* -------- 5) Create (UpdateAdd) con <Keys> + fallback a Update -------- */
    let soapResp = await soapCreateUpdateAdd({ token, deKey, values, keyValues });
    let parsed = parseSoapCreateResponse(soapResp.body);

    // Si hay 68001 (duplicado/violación), hacemos Update con <Keys>
    if (!parsed.ok && /UpdateAdd violation/i.test(parsed.status)) {
      soapResp = await soapUpdate({ token, deKey, values, keyValues });
      parsed = parseSoapUpdateResponse(soapResp.body);
    }

    if (!parsed.ok) {
      return res.status(502).json({ ok: false, error: 'sfmc_soap', detail: parsed });
    }

    return res.status(200).json({
      ok: true,
      saved: {
        email: values.Email,
        campaign: values.Campaign,
        discount: values.Discount ?? '',
        result: values[RESULT_FIELD] ?? '',
        variationId: values[VAR_FIELD] ?? ''
      },
      soap: {
        status: soapResp.status,
        requestId: parsed.requestId || '',
        overall: parsed.overall || '',
        statusMessage: parsed.status || ''
      }
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: 'server_error', detail: String(err?.message || err) });
  }
}

/* ================= HELPERS ================= */

function isValidEmail(email) {
  return /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-z\-0-9]+\.)+[a-z]{2,}))$/i.test(email);
}
async function readBody(req) { const bufs = []; for await (const c of req) bufs.push(c); return Buffer.concat(bufs).toString('utf8'); }
async function parseJSON(req) { const s = await readBody(req); return s ? JSON.parse(s) : {}; }

async function getSfmcToken() {
  const authBase = (process.env.SFMC_AUTH_BASE || '').replace(/\/+$/, '');
  const body = {
    grant_type: 'client_credentials',
    client_id: process.env.SFMC_CLIENT_ID,
    client_secret: process.env.SFMC_CLIENT_SECRET
  };
  if (process.env.SFMC_ACCOUNT_ID) body.account_id = process.env.SFMC_ACCOUNT_ID;

  const r = await fetch(`${authBase}/v2/token`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(`Token error: ${r.status} ${await r.text()}`);
  const j = await r.json();
  if (!j.access_token) throw new Error('No access_token');
  return j.access_token;
}

function normalizeSoapBase(soapBaseEnv, restBaseEnv) {
  if (soapBaseEnv && soapBaseEnv.trim()) {
    const base = soapBaseEnv.replace(/\/+$/, '');
    return /\/Service\.asmx$/i.test(base) ? base : base + '/Service.asmx';
  }
  const rest = (restBaseEnv || '').replace(/\/+$/, '');
  if (!rest) throw new Error('Missing SFMC_SOAP_BASE and SFMC_REST_BASE');
  return rest.replace('rest.', 'soap.') + '/Service.asmx';
}

function escapeXml(s) {
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&apos;');
}
function sha256Lower(email) {
  return crypto.createHash('sha256').update(String(email).toLowerCase()).digest('hex');
}

/* ===== SOAP builders ===== */

function buildPropertiesXML(values) {
  const keys = Object.keys(values);
  if (!keys.length) return ''; // por las dudas
  return keys.map(k => `
    <Property>
      <Name>${escapeXml(k)}</Name>
      <Value>${escapeXml(values[k] == null ? '' : String(values[k]))}</Value>
    </Property>
  `).join('');
}
function buildKeysXML(keyValues) {
  return Object.keys(keyValues).map(k => `
    <tns:Key>
      <tns:Name>${escapeXml(k)}</tns:Name>
      <tns:Value>${escapeXml(keyValues[k] == null ? '' : String(keyValues[k]))}</tns:Value>
    </tns:Key>
  `).join('');
}

/* Create (UpdateAdd) con <Keys> */
async function soapCreateUpdateAdd({ token, deKey, values, keyValues }) {
  const soapBase = normalizeSoapBase(process.env.SFMC_SOAP_BASE, process.env.SFMC_REST_BASE);
  const propsXML = buildPropertiesXML(values);
  const keysXML  = buildKeysXML(keyValues);

  const envelope = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:tns="http://exacttarget.com/wsdl/partnerAPI">
  <soapenv:Header><fueloauth xmlns="http://exacttarget.com">${escapeXml(token)}</fueloauth></soapenv:Header>
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
        <tns:Keys>${keysXML}</tns:Keys>
        <tns:Properties>${propsXML}</tns:Properties>
      </tns:Objects>
    </tns:CreateRequest>
  </soapenv:Body>
</soapenv:Envelope>`;

  const r = await fetch(soapBase, {
    method: 'POST',
    headers: { 'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'Create' },
    body: envelope
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`SOAP error ${r.status}: ${text}`);
  return { status: r.status, body: text };
}

/* Update con <Keys> (si hay violación de UpdateAdd) */
async function soapUpdate({ token, deKey, values, keyValues }) {
  const soapBase = normalizeSoapBase(process.env.SFMC_SOAP_BASE, process.env.SFMC_REST_BASE);

  // En Update mandamos solo campos NO clave; si quedara vacío, forzamos Timestamp
  const v2 = { ...values };
  for (const k of Object.keys(keyValues)) delete v2[k];
  if (!Object.keys(v2).length) {
    v2.Timestamp = new Date().toISOString(); // asegura que siempre haya al menos 1 Property
  }

  const propsXML = buildPropertiesXML(v2);
  const keysXML  = buildKeysXML(keyValues);

  const envelope = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:tns="http://exacttarget.com/wsdl/partnerAPI">
  <soapenv:Header><fueloauth xmlns="http://exacttarget.com">${escapeXml(token)}</fueloauth></soapenv:Header>
  <soapenv:Body>
    <tns:UpdateRequest>
      <tns:Objects xsi:type="tns:DataExtensionObject">
        <tns:CustomerKey>${escapeXml(deKey)}</tns:CustomerKey>
        <tns:Keys>${keysXML}</tns:Keys>
        <tns:Properties>${propsXML}</tns:Properties>
      </tns:Objects>
    </tns:UpdateRequest>
  </soapenv:Body>
</soapenv:Envelope>`;

  const r = await fetch(soapBase, {
    method: 'POST',
    headers: { 'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'Update' },
    body: envelope
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`SOAP error ${r.status}: ${text}`);
  return { status: r.status, body: text };
}

/* ===== SOAP parsers ===== */
function parseSoapCreateResponse(xml) {
  const overall = (xml.match(/<OverallStatus>([^<]+)<\/OverallStatus>/i) || [])[1] || '';
  const status  = (xml.match(/<StatusMessage>([^<]+)<\/StatusMessage>/i) || [])[1] || '';
  const reqId   = (xml.match(/<RequestID>([^<]+)<\/RequestID>/i) || [])[1] || '';
  const codes   = Array.from(xml.matchAll(/<ErrorCode>([^<]+)<\/ErrorCode>/ig)).map(m => m[1]);
  const ok = /OK/i.test(overall) || /OK/i.test(status);
  return { ok, overall, status, requestId: reqId, errors: codes };
}
function parseSoapUpdateResponse(xml) {
  const overall = (xml.match(/<OverallStatus>([^<]+)<\/OverallStatus>/i) || [])[1] || '';
  const status  = (xml.match(/<StatusMessage>([^<]+)<\/StatusMessage>/i) || [])[1] || '';
  const reqId   = (xml.match(/<RequestID>([^<]+)<\/RequestID>/i) || [])[1] || '';
  const ok = /OK/i.test(overall) || /OK/i.test(status);
  return { ok, overall, status, requestId: reqId, errors: [] };
}
