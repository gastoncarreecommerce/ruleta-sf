import crypto from 'crypto';

// === Config fija basada en TU DE ===
// Si más adelante agregás/quitas campos en la DE, ajustá esta lista o
// usa la env var SFMC_FIELDS_ALLOWLIST para sobreescribir.
function getFieldList() {
  const envList = (process.env.SFMC_FIELDS_ALLOWLIST || '').trim();
  if (envList) return envList.split(',').map(s => s.trim()).filter(Boolean);
  // Tu DE (según captura): Email, Campaign (PK), Result, VariationId, HashedEmail, Source, Timestamp
  // + Discount si lo agregaste (me dijiste que sí).
  return ['Email','Campaign','Result','VariationId','HashedEmail','Source','Timestamp','Discount'];
}

export default async function handler(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,GET,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();

  const debugOn = (process.env.DEBUG_SOAP === '1') || /(?:[?&]debug=1)(?:$|&)/.test(req.url || '');

  try {
    // -------- 1) INPUT --------
    const url = new URL(req.url, `http://${req.headers.host}`);
    const q = Object.fromEntries(url.searchParams.entries());

    let email = q.email || null;
    let discount = q.discount != null ? Number(q.discount) : null;
    const campaign = q.campaign || 'default';
    let variationId = q.variationId || null;
    let result = (q.result || '').toLowerCase();

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
    }

    // -------- 2) VALIDACIONES --------
    if (!email || !isValidEmail(email)) {
      return res.status(400).json({ ok:false, error:'invalid_email' });
    }
    if (discount != null && (isNaN(discount) || discount < 0 || discount > 100)) {
      return res.status(400).json({ ok:false, error:'invalid_discount' });
    }
    if (result && result !== 'won' && result !== 'lost') result = '';

    if (process.env.SAVE_ONLY_WINNERS === '1' && result !== 'won') {
      return res.status(200).json({ ok:true, skipped:'not_winner' });
    }

    // -------- 3) TOKEN --------
    const token = await getSfmcToken();

    // -------- 4) ARMAR PROPS EN BASE A TU DE --------
    const nowIso = new Date().toISOString();
    const hashed = sha256Lower(email);
    const RESULT_FIELD = process.env.SFMC_RESULT_FIELD || 'Result';
    const VAR_FIELD    = process.env.SFMC_VARIATION_FIELD || 'VariationId';

    const allValues = {
      Email: email,
      Campaign: campaign,
      [RESULT_FIELD]: result || '',
      [VAR_FIELD]: variationId || '',
      HashedEmail: hashed,
      Source: process.env.SFMC_SOURCE_VALUE || 'DY_Ruleta',
      Timestamp: nowIso
    };
    // Solo enviar Discount si viene número (para no borrar)
    if (!isNaN(discount) && discount !== null) allValues.Discount = String(discount);

    // Tomar SOLO los campos de la lista (no dependemos de SOAP discover)
    const allowed = new Set(getFieldList());
    const propsToSend = {};
    for (const k of Object.keys(allValues)) {
      if (allowed.has(k)) propsToSend[k] = allValues[k];
    }

    // Keys (por defecto Email+Campaign)
    const keyFields = (process.env.SFMC_KEY_FIELDS || 'Email,Campaign')
      .split(',').map(s => s.trim()).filter(Boolean);

    for (const k of keyFields) {
      if (!(k in propsToSend)) {
        return res.status(400).json({ ok:false, error:'missing_key_field', detail:`Key '${k}' no está en propsToSend` });
      }
    }

    const deKey = (process.env.SFMC_DE_KEY || '').trim();
    if (!deKey) return res.status(500).json({ ok:false, error:'missing_env', detail:'SFMC_DE_KEY' });

    // -------- 5) CREATE + UpdateAdd (keys en Properties) --------
    let path = 'create';
    let soapResp = await soapCreateUpdateAdd({ token, deKey, values: propsToSend });
    let parsed = parseSoapCreateResponse(soapResp.body);

    // -------- 6) Fallback: Update (Keys + Properties no-clave) --------
    if (!parsed.ok && /UpdateAdd violation/i.test(parsed.status)) {
      path = 'update';
      const keyValues = {};
      keyFields.forEach(k => keyValues[k] = propsToSend[k]);

      const nonKeyValues = { ...propsToSend };
      keyFields.forEach(k => delete nonKeyValues[k]);

      // Si no quedó nada para actualizar, mandamos al menos Timestamp si existe
      if (Object.keys(nonKeyValues).length === 0 && allowed.has('Timestamp')) {
        nonKeyValues.Timestamp = nowIso;
      }

      if (Object.keys(nonKeyValues).length === 0) {
        return res.status(400).json({ ok:false, error:'no_updatable_fields',
          detail:'No hay campos no-clave para Update' });
      }

      soapResp = await soapUpdate({ token, deKey, keyValues, values: nonKeyValues });
      parsed = parseSoapUpdateResponse(soapResp.body);
    }

    if (!parsed.ok) {
      const payload = { ok:false, error:'sfmc_soap', detail: parsed };
      if (debugOn) payload.debug = {
        path, propsSent: Object.keys(propsToSend), keyFields
      };
      return res.status(502).json(payload);
    }

    const response = {
      ok: true,
      saved: {
        email: propsToSend.Email,
        campaign: propsToSend.Campaign,
        discount: propsToSend.Discount ?? '',
        result: propsToSend[RESULT_FIELD] ?? '',
        variationId: propsToSend[VAR_FIELD] ?? ''
      },
      soap: {
        status: soapResp.status,
        requestId: parsed.requestId || '',
        overall: parsed.overall || '',
        statusMessage: parsed.status || ''
      }
    };
    if (debugOn) response.debug = { path, propsSent: Object.keys(propsToSend), keyFields };

    return res.status(200).json(response);

  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok:false, error:'server_error', detail:String(err?.message || err) });
  }
}

/* ===== Helpers ===== */
function isValidEmail(e){
  return /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-z\-0-9]+\.)+[a-z]{2,}))$/i.test(e);
}
async function readBody(req){ const bufs=[]; for await (const c of req) bufs.push(c); return Buffer.concat(bufs).toString('utf8'); }
async function parseJSON(req){ const s = await readBody(req); return s ? JSON.parse(s) : {}; }
function sha256Lower(email){ return crypto.createHash('sha256').update(String(email).toLowerCase()).digest('hex'); }

async function getSfmcToken() {
  const authBase = (process.env.SFMC_AUTH_BASE || '').replace(/\/+$/, '');
  const body = {
    grant_type: 'client_credentials',
    client_id: process.env.SFMC_CLIENT_ID,
    client_secret: process.env.SFMC_CLIENT_SECRET
  };
  if (process.env.SFMC_ACCOUNT_ID) body.account_id = process.env.SFMC_ACCOUNT_ID;

  const r = await fetch(`${authBase}/v2/token`, {
    method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)
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

function escapeXml(s){
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&apos;');
}

function buildPropertiesXML(values){
  return Object.keys(values).map(k => `
    <Property><Name>${escapeXml(k)}</Name><Value>${escapeXml(values[k] == null ? '' : String(values[k]))}</Value></Property>
  `).join('');
}
function buildKeysXML(keyValues){
  return Object.keys(keyValues).map(k => `
    <tns:Key><tns:Name>${escapeXml(k)}</tns:Name><tns:Value>${escapeXml(keyValues[k] == null ? '' : String(keyValues[k]))}</tns:Value></tns:Key>
  `).join('');
}

async function soapCreateUpdateAdd({ token, deKey, values }) {
  const soapBase = normalizeSoapBase(process.env.SFMC_SOAP_BASE, process.env.SFMC_REST_BASE);
  const propsXML = buildPropertiesXML(values);
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
        <tns:Properties>${propsXML}</tns:Properties>
      </tns:Objects>
    </tns:CreateRequest>
  </soapenv:Body>
</soapenv:Envelope>`;
  const r = await fetch(soapBase, { method:'POST', headers:{ 'Content-Type':'text/xml; charset=utf-8', 'SOAPAction':'Create' }, body: envelope });
  const text = await r.text();
  if (!r.ok) throw new Error(`SOAP error ${r.status}: ${text}`);
  return { status: r.status, body: text };
}

async function soapUpdate({ token, deKey, keyValues, values }) {
  const soapBase = normalizeSoapBase(process.env.SFMC_SOAP_BASE, process.env.SFMC_REST_BASE);
  const propsXML = buildPropertiesXML(values);
  const keysXML  = buildKeysXML(keyValues);

  const envelope = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:tns="http://exacttarget.com/wsdl/partnerAPI">
  <soapenv:Header>
    <fueloauth xmlns="http://exacttarget.com">${escapeXml(token)}</fueloauth>
  </soapenv:Header>
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
  const r = await fetch(soapBase, { method:'POST', headers:{ 'Content-Type':'text/xml; charset=utf-8', 'SOAPAction':'Update' }, body: envelope });
  const text = await r.text();
  if (!r.ok) throw new Error(`SOAP error ${r.status}: ${text}`);
  return { status: r.status, body: text };
}

function parseSoapCreateResponse(xml){
  const overall = (xml.match(/<OverallStatus>([^<]+)<\/OverallStatus>/i) || [])[1] || '';
  const status  = (xml.match(/<StatusMessage>([^<]+)<\/StatusMessage>/i) || [])[1] || '';
  const reqId   = (xml.match(/<RequestID>([^<]+)<\/RequestID>/i) || [])[1] || '';
  const codes   = Array.from(xml.matchAll(/<ErrorCode>([^<]+)<\/ErrorCode>/ig)).map(m => m[1]);
  const ok = /OK/i.test(overall) || /OK/i.test(status);
  return { ok, overall, status, requestId: reqId, errors: codes };
}
function parseSoapUpdateResponse(xml){
  const overall = (xml.match(/<OverallStatus>([^<]+)<\/OverallStatus>/i) || [])[1] || '';
  const status  = (xml.match(/<StatusMessage>([^<]+)<\/StatusMessage>/i) || [])[1] || '';
  const reqId   = (xml.match(/<RequestID>([^<]+)<\/RequestID>/i) || [])[1] || '';
  const ok = /OK/i.test(overall) || /OK/i.test(status);
  return { ok, overall, status, requestId: reqId, errors: [] };
}
