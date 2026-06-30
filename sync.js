// ============================================================
// Spyne Lead Journey - Daily Sync Script
// Pulls Factors.ai + HubSpot data into Railway PostgreSQL
//
// Set these environment variables in Railway:
//   FACTORS_API_KEY=your_factors_api_key
//   HUBSPOT_API_KEY=your_hubspot_private_app_token
//   DATABASE_URL=your_railway_postgres_connection_string
//   FACTORS_DOMAIN=spyne.ai
// ============================================================

const { Pool } = require('pg');

const db = new Pool({ connectionString: process.env.DATABASE_URL });
const FACTORS_API_KEY = process.env.FACTORS_API_KEY;
const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;
const FACTORS_DOMAIN = process.env.FACTORS_DOMAIN || 'spyne.ai';

// ---- Step 0: Auto-create tables on first run ----
async function ensureSchema() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS contacts (
      id TEXT PRIMARY KEY,
      email TEXT UNIQUE,
      name TEXT,
      company TEXT,
      lead_source TEXT,
      lifecyclestage TEXT,
      lead_status TEXT,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS journeys (
      id SERIAL PRIMARY KEY,
      contact_id TEXT REFERENCES contacts(id),
      page_url TEXT,
      page_title TEXT,
      event_name TEXT,
      visited_at TIMESTAMP,
      utm_source TEXT,
      utm_campaign TEXT,
      utm_medium TEXT
    );
    CREATE TABLE IF NOT EXISTS deals (
      id TEXT PRIMARY KEY,
      contact_id TEXT REFERENCES contacts(id),
      deal_name TEXT,
      deal_stage TEXT,
      amount DECIMAL,
      close_date TIMESTAMP,
      created_at TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS sync_log (
      id SERIAL PRIMARY KEY,
      source TEXT,
      last_sync TIMESTAMP,
      records_pulled INTEGER,
      status TEXT
    );
  `);
  console.log('Schema ready.');
}

// ---- Helper ----
async function apiFetch(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(`API error ${res.status}: ${url}`);
  return res.json();
}

// ---- Step 1: Pull contacts from HubSpot ----
async function syncHubSpotContacts() {
  console.log('Pulling HubSpot contacts...');
  const properties = [
    'email','firstname','lastname','company',
    'lead_source','lifecyclestage','hs_lead_status',
    'createdate','lastmodifieddate'
  ].join(',');

  let after = null;
  let total = 0;

  do {
    const url = `https://api.hubapi.com/crm/v3/objects/contacts?limit=100&properties=${properties}${after ? `&after=${after}` : ''}`;
    const data = await apiFetch(url, {
      headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}` }
    });

    for (const c of data.results) {
      const p = c.properties;
      await db.query(`
        INSERT INTO contacts (id, email, name, company, lead_source, lifecyclestage, lead_status, created_at, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (id) DO UPDATE SET
          email=EXCLUDED.email, name=EXCLUDED.name, company=EXCLUDED.company,
          lead_source=EXCLUDED.lead_source, lifecyclestage=EXCLUDED.lifecyclestage,
          lead_status=EXCLUDED.lead_status, updated_at=EXCLUDED.updated_at
      `, [
        c.id,
        p.email,
        `${p.firstname || ''} ${p.lastname || ''}`.trim(),
        p.company,
        p.lead_source,
        p.lifecyclestage,
        p.hs_lead_status,
        p.createdate ? new Date(p.createdate) : null,
        p.lastmodifieddate ? new Date(p.lastmodifieddate) : null
      ]);
    }

    total += data.results.length;
    after = data.paging?.next?.after || null;
  } while (after);

  console.log(`HubSpot contacts synced: ${total}`);
  return total;
}

// ---- Step 2: Pull deals from HubSpot ----
async function syncHubSpotDeals() {
  console.log('Pulling HubSpot deals...');
  const properties = ['dealname','dealstage','amount','closedate','createdate'].join(',');

  let after = null;
  let total = 0;

  do {
    const url = `https://api.hubapi.com/crm/v3/objects/deals?limit=100&properties=${properties}&associations=contacts${after ? `&after=${after}` : ''}`;
    const data = await apiFetch(url, {
      headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}` }
    });

    for (const deal of data.results) {
      const p = deal.properties;
      const contactId = deal.associations?.contacts?.results?.[0]?.id || null;

      await db.query(`
        INSERT INTO deals (id, contact_id, deal_name, deal_stage, amount, close_date, created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (id) DO UPDATE SET
          deal_stage=EXCLUDED.deal_stage, amount=EXCLUDED.amount, close_date=EXCLUDED.close_date
      `, [
        deal.id, contactId, p.dealname, p.dealstage,
        p.amount ? parseFloat(p.amount) : null,
        p.closedate ? new Date(p.closedate) : null,
        p.createdate ? new Date(p.createdate) : null
      ]);
    }

    total += data.results.length;
    after = data.paging?.next?.after || null;
  } while (after);

  console.log(`HubSpot deals synced: ${total}`);
  return total;
}

// ---- Step 3: Pull journeys from Factors.ai ----
async function syncFactorsJourneys() {
  console.log('Pulling Factors.ai journeys...');

  const to = new Date().toISOString().split('T')[0];
  const from = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  const url = `https://api.factors.ai/open/v1/account/${FACTORS_DOMAIN}/journey?from=${from}&to=${to}`;
  const data = await apiFetch(url, {
    headers: { Authorization: `Bearer ${FACTORS_API_KEY}` }
  });

  let total = 0;

  for (const user of (data.users || [])) {
    const email = user.user_properties?.email;
    if (!email) continue;

    const result = await db.query('SELECT id FROM contacts WHERE email=$1', [email]);
    const contactId = result.rows[0]?.id;
    if (!contactId) continue;

    for (const activity of (user.user_activities || [])) {
      await db.query(`
        INSERT INTO journeys (contact_id, page_url, page_title, event_name, visited_at, utm_source, utm_campaign, utm_medium)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      `, [
        contactId,
        activity.properties?.$page_url || activity.properties?.page_url,
        activity.properties?.$page_title || activity.properties?.page_title,
        activity.event_name,
        activity.timestamp ? new Date(activity.timestamp) : null,
        activity.properties?.utm_source,
        activity.properties?.utm_campaign,
        activity.properties?.utm_medium
      ]);
      total++;
    }
  }

  console.log(`Factors.ai journey events synced: ${total}`);
  return total;
}

// ---- Main ----
async function run() {
  try {
    console.log('=== Starting Spyne Lead Journey Sync ===');
    await ensureSchema();

    const contacts = await syncHubSpotContacts();
    const deals = await syncHubSpotDeals();
    const journeys = await syncFactorsJourneys();

    await db.query(`
      INSERT INTO sync_log (source, last_sync, records_pulled, status)
      VALUES ('all', NOW(), $1, 'success')
    `, [contacts + deals + journeys]);

    console.log('=== Sync complete ===');
  } catch (err) {
    console.error('Sync failed:', err.message);
    await db.query(`
      INSERT INTO sync_log (source, last_sync, records_pulled, status)
      VALUES ('all', NOW(), 0, $1)
    `, [`error: ${err.message}`]).catch(() => {});
  } finally {
    await db.end();
  }
}

run();
