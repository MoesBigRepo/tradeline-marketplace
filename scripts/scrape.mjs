#!/usr/bin/env node
/**
 * Tradeline Marketplace — GitHub Actions scraper
 * Port of /Users/mo/scripts/tradeline-worker/src/index.js
 *
 * Writes data/<source>.json per upstream + data/all.json combined.
 * On per-source failure, leaves the existing JSON file untouched so the
 * site continues serving the last known-good snapshot.
 *
 * Exit codes: 0 if >=1 source succeeded, 1 if all failed.
 */

import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = resolve(__dirname, '..', 'data');

const SUPPLY_URL = 'https://www.tradelinesupply.com/pricing/';
const GENIE_URL = 'https://docs.google.com/spreadsheets/d/1DXM1p0LlmQ9H5vY_1mmJWO35P-dyq4BXJgCRmB6sb-g/export?format=csv&gid=244641818';
const BOOST_URL = 'https://www.boostcredit101.com/tradelines';
const GFS_URL = 'https://gfsgroup.org/tradelines-for-sale?limit=50&offset=0';

// ─── HTML / CSV parsers (verbatim port from Worker) ─────────────────────

function parseHTMLTables(html) {
  const rows = [];
  const trRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let trMatch;
  while ((trMatch = trRegex.exec(html)) !== null) {
    const cells = [];
    const tdRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
    let tdMatch;
    while ((tdMatch = tdRegex.exec(trMatch[1])) !== null) {
      const text = tdMatch[1].replace(/<[^>]*>/g, '').replace(/&[^;]+;/g, ' ').replace(/\s+/g, ' ').trim();
      cells.push(text);
    }
    if (cells.length > 0) rows.push(cells);
  }
  return rows;
}

function dedupBank(name) {
  const parts = name.split(/\s+/);
  if (parts.length === 2 && parts[0].toLowerCase() === parts[1].toLowerCase()) return parts[0];
  if (parts.length >= 2 && parts[parts.length - 1].length <= 4 &&
      parts[0].toLowerCase().includes(parts[parts.length - 1].toLowerCase())) {
    return parts[0];
  }
  return name;
}

function parseSupply(html) {
  const tableRows = parseHTMLTables(html);
  const accounts = [];
  for (const row of tableRows) {
    if (row.length < 9) continue;
    if (row[1].toLowerCase().includes('bank') && row[2].toLowerCase().includes('card')) continue;
    const bank = dedupBank(row[1].replace(/\s+/g, ' ').trim());
    if (!bank) continue;
    accounts.push([
      bank,
      row[2].replace(/\s+/g, ' ').trim(),
      row[3].replace(/\s+/g, ' ').trim(),
      row[4].replace(/\s+/g, ' ').trim(),
      row[5].replace(/\s+/g, ' ').trim(),
      row[6].replace(/\s+/g, ' ').trim(),
      row[7].replace(/\s+/g, ' ').trim(),
      row[8].replace(/\s+/g, ' ').trim(),
    ]);
  }
  return accounts;
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (c === ',' && !inQuotes) { result.push(current); current = ''; }
    else current += c;
  }
  result.push(current);
  return result;
}

function parseAccount(raw) {
  const s = raw.replace(/[\u202a\u202c]/g, '').trim();
  if (!s) return { age: '', bank: '', limit: '' };
  const tokens = s.split(/\s+/);
  const amountRe = /^\$?\d[\d.,]*[kK]?$/;
  let clIdx = -1;
  for (let i = tokens.length - 1; i >= 0; i--) {
    if (amountRe.test(tokens[i])) { clIdx = i; break; }
  }
  if (clIdx === -1) return { age: '', bank: s, limit: '' };
  const limit = tokens[clIdx];
  const before = tokens.slice(0, clIdx);
  let age = '';
  let bankStart = 0;
  if (before.length > 0) {
    const first = before[0];
    if (/^\d{4}$/.test(first)) { age = first; bankStart = 1; }
    else if (/^\d+MO$/i.test(first)) { age = first; bankStart = 1; }
    else if (before.length >= 2 &&
             /^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i.test(first) &&
             /^\d{4}$/.test(before[1])) {
      age = `${first} ${before[1]}`;
      bankStart = 2;
    }
  }
  const bank = before.slice(bankStart).join(' ');
  return { age, bank, limit };
}

function parseGenie(csv) {
  const lines = csv.split('\n');
  const accounts = [];
  for (let i = 0; i < lines.length; i++) {
    if (i < 3) continue;
    const cells = parseCSVLine(lines[i]);
    if (cells.length < 14) continue;
    const spots = cells[0].trim();
    const account = cells[11].trim();
    const closingDate = cells[12].trim();
    const price = cells[13].trim();
    if (account.toUpperCase().includes('BLACK BAR')) continue;
    if (!account && !spots) continue;
    const parsed = parseAccount(account);
    if (!parsed.bank) continue;
    accounts.push([spots, parsed.age, parsed.bank, parsed.limit, closingDate, price, String(i + 1)]);
  }
  return accounts;
}

function calcAge(dateOpenedISO) {
  const opened = new Date(dateOpenedISO);
  if (isNaN(opened.getTime())) return '0.0';
  const now = new Date();
  let years = now.getFullYear() - opened.getFullYear();
  let months = now.getMonth() - opened.getMonth();
  if (months < 0) { years--; months += 12; }
  return `${years}.${months}`;
}

function dayOrdinal(dateISO) {
  const d = new Date(dateISO);
  if (isNaN(d.getTime())) return '';
  const day = d.getDate();
  const suffix = (day >= 11 && day <= 13) ? 'th'
    : day % 10 === 1 ? 'st'
    : day % 10 === 2 ? 'nd'
    : day % 10 === 3 ? 'rd' : 'th';
  return `${day}${suffix}`;
}

function formatPostDate(dateISO) {
  const d = new Date(dateISO);
  if (isNaN(d.getTime())) return '';
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return `${months[d.getMonth()]} ${String(d.getDate()).padStart(2,'0')},${d.getFullYear()}`;
}

function formatLimit(cents) {
  const n = typeof cents === 'number' ? cents : parseInt(cents, 10);
  if (isNaN(n)) return '$0';
  return `$${n.toLocaleString('en-US')}`;
}

function formatPrice(cents) {
  const n = typeof cents === 'number' ? cents : parseFloat(cents);
  if (isNaN(n)) return '$0';
  return `$${n.toFixed(2)}`;
}

function parseBoostJSON(html) {
  const rscPattern = /\[(\{\\"Id\\":\d+.*?\})\]/g;
  let rscMatch;
  while ((rscMatch = rscPattern.exec(html)) !== null) {
    try {
      const cleaned = rscMatch[0].replace(/\\"/g, '"');
      const arr = JSON.parse(cleaned);
      if (Array.isArray(arr) && arr.length > 0 && arr[0].Id && arr[0].Lender) return arr;
    } catch { /* try next */ }
  }
  const objPattern = /\{"Id":\d+,"Price":\d+,"SpotsAvailable":\d+,"Lender":"[^"]+","Cycles":\d+,"Limit":\d+,"DateOpened":"[^"]+","StatementDate":"[^"]+","PostingDate":"[^"]+","CardholderAddressID":\d+\}/g;
  const objects = [];
  let objMatch;
  while ((objMatch = objPattern.exec(html)) !== null) {
    try { objects.push(JSON.parse(objMatch[0])); } catch {}
  }
  return objects.length > 0 ? objects : null;
}

function parseBoostCards(html) {
  const accounts = [];
  const articleRegex = /<article[^>]*>([\s\S]*?)<\/article>/gi;
  let articleMatch;
  while ((articleMatch = articleRegex.exec(html)) !== null) {
    const card = articleMatch[1];
    const bankMatch = card.match(/<h3[^>]*class="[^"]*text-lg font-bold[^"]*"[^>]*>([^<]+)<\/h3>/i);
    const priceMatch = card.match(/<div[^>]*class="[^"]*text-2xl font-bold[^"]*"[^>]*>\$([0-9,]+)<\/div>/i);
    const statsMatches = [...card.matchAll(/<div[^>]*class="[^"]*text-base font-bold[^"]*"[^>]*>([^<]+)<\/div>/gi)];
    const spotsMatch = card.match(/Only (\d+) left!|(\d+) spots? left/i);
    if (!bankMatch || !priceMatch) continue;
    const lender = bankMatch[1].trim();
    const price = `$${priceMatch[1]}`;
    const limit = statsMatches[0] ? statsMatches[0][1].trim() : '';
    const ageRaw = statsMatches[1] ? statsMatches[1][1].trim() : '';
    const stmtDay = statsMatches[2] ? statsMatches[2][1].trim() : '';
    const spots = spotsMatch ? (spotsMatch[1] || spotsMatch[2]) : '0';
    const ageMatch = ageRaw.match(/(\d+)\s*yr/i);
    const age = ageMatch ? `${ageMatch[1]}.0` : ageRaw;
    accounts.push([lender, limit, age, spots, stmtDay, '', price]);
  }
  return accounts;
}

function parseBoost(html) {
  const jsonData = parseBoostJSON(html);
  if (jsonData && jsonData.length > 0) {
    return jsonData.map((t) => [
      t.Lender,
      formatLimit(t.Limit),
      calcAge(t.DateOpened),
      String(t.SpotsAvailable),
      dayOrdinal(t.StatementDate),
      formatPostDate(t.PostingDate),
      formatPrice(t.Price),
    ]);
  }
  const cardAccounts = parseBoostCards(html);
  if (cardAccounts.length > 0) return cardAccounts;
  const tableRows = parseHTMLTables(html);
  const accounts = [];
  for (const row of tableRows) {
    if (row.length < 7) continue;
    if (row[2].toLowerCase() === 'lender' || row[0].toLowerCase() === 'price') continue;
    if (!row[2]) continue;
    const m = row[4].match(/(\d+)\s*years?\s*(\d+)?\s*months?/i);
    const age = m ? `${m[1]}.${m[2] || '0'}` : row[4];
    accounts.push([row[2], row[3], age, row[1], row[5], row[6], row[0]]);
  }
  return accounts;
}

function parseGFS(html) {
  const tableRows = parseHTMLTables(html);
  const accounts = [];
  for (const row of tableRows) {
    if (row.length < 9) continue;
    let lender = row[0].replace(/\s*Details\s*$/i, '').trim();
    const limit = row[1].trim();
    const age = row[2].trim();
    const price = row[3].trim();
    const postingDates = row[4].replace(/\s+/g, ' ').trim();
    const purchaseBy = row[5].trim();
    const stmtDate = row[6].trim();
    const tradelineId = row[8].trim();
    if (lender.toLowerCase() === 'lender' || limit.toLowerCase() === 'card limit') continue;
    if (!lender) continue;
    accounts.push([lender, limit, age, price, postingDates, purchaseBy, stmtDate, tradelineId]);
  }
  return accounts;
}

// ─── Fetch with timeout ──────────────────────────────────────────────────

async function fetchWithTimeout(url, timeoutMs = 20000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,text/csv,application/xhtml+xml',
        'Accept-Encoding': 'gzip, deflate',
      },
      redirect: 'follow',
    });
    return resp;
  } finally {
    clearTimeout(timer);
  }
}

// ─── Source scrapers ─────────────────────────────────────────────────────

async function scrapeSupply() {
  const resp = await fetchWithTimeout(SUPPLY_URL, 20000);
  if (!resp.ok) throw new Error(`Supply HTTP ${resp.status}`);
  const html = await resp.text();
  const accounts = parseSupply(html);
  if (accounts.length === 0) throw new Error('Supply parsed 0 accounts');
  return { accounts, timestamp: new Date().toISOString(), source: 'tradelinesupply.com', count: accounts.length };
}

async function scrapeGenie() {
  // Hardened for "sheet closed" scenarios — sheet owner toggles access when inventory is low.
  const resp = await fetchWithTimeout(GENIE_URL, 15000);
  if (!resp.ok) throw new Error(`Genie HTTP ${resp.status} (sheet may be closed)`);
  const ctype = (resp.headers.get('content-type') || '').toLowerCase();
  const text = await resp.text();
  // Google returns HTML error pages when the sheet is private; detect before parsing.
  if (!ctype.includes('csv') && !ctype.includes('text/plain') && !ctype.includes('application/octet-stream')) {
    throw new Error(`Genie returned non-CSV content-type "${ctype}" (sheet may be closed)`);
  }
  if (text.trim().startsWith('<')) throw new Error('Genie returned HTML (sheet may be closed)');
  const accounts = parseGenie(text);
  if (accounts.length === 0) throw new Error('Genie parsed 0 accounts (sheet may be closed or empty)');
  return { accounts, timestamp: new Date().toISOString(), source: 'tradelinegenie.com', count: accounts.length };
}

async function scrapeBoost() {
  const resp = await fetchWithTimeout(BOOST_URL, 15000);
  if (!resp.ok) throw new Error(`Boost HTTP ${resp.status}`);
  const html = await resp.text();
  const accounts = parseBoost(html);
  if (accounts.length === 0) throw new Error('Boost parsed 0 accounts');
  return { accounts, timestamp: new Date().toISOString(), source: 'boostcredit101.com', count: accounts.length };
}

async function scrapeGFS() {
  const resp = await fetchWithTimeout(GFS_URL, 15000);
  if (!resp.ok) throw new Error(`GFS HTTP ${resp.status}`);
  const html = await resp.text();
  const accounts = parseGFS(html);
  if (accounts.length === 0) throw new Error('GFS parsed 0 accounts');
  return { accounts, timestamp: new Date().toISOString(), source: 'gfsgroup.org', count: accounts.length };
}

// ─── Runner ──────────────────────────────────────────────────────────────

async function readExisting(name) {
  const path = resolve(DATA_DIR, `${name}.json`);
  if (!existsSync(path)) return null;
  try { return JSON.parse(await readFile(path, 'utf-8')); }
  catch { return null; }
}

async function writeJSON(name, data) {
  const path = resolve(DATA_DIR, `${name}.json`);
  await writeFile(path, JSON.stringify(data, null, 0) + '\n', 'utf-8');
}

function emptySource(sourceName) {
  return { accounts: [], error: 'never fetched', source: sourceName, count: 0 };
}

async function main() {
  await mkdir(DATA_DIR, { recursive: true });

  const sources = [
    { name: 'supply', fn: scrapeSupply, label: 'tradelinesupply.com' },
    { name: 'genie', fn: scrapeGenie, label: 'tradelinegenie.com' },
    { name: 'boost', fn: scrapeBoost, label: 'boostcredit101.com' },
    { name: 'gfs', fn: scrapeGFS, label: 'gfsgroup.org' },
  ];

  const results = {};
  let successCount = 0;

  for (const src of sources) {
    try {
      const data = await src.fn();
      await writeJSON(src.name, data);
      results[src.name] = data;
      successCount++;
      console.log(`[${src.name}] OK — ${data.count} accounts`);
    } catch (err) {
      console.error(`[${src.name}] FAIL — ${err.message} (keeping last-known-good)`);
      const existing = await readExisting(src.name);
      if (existing) {
        results[src.name] = existing;
      } else {
        // First-run failure: write placeholder so the master gets 200 + shape, not 404.
        const placeholder = emptySource(src.label);
        await writeJSON(src.name, placeholder);
        results[src.name] = placeholder;
      }
    }
  }

  await writeJSON('all', {
    supply: results.supply,
    genie: results.genie,
    boost: results.boost,
    gfs: results.gfs,
  });

  console.log(`\n${successCount}/${sources.length} sources refreshed.`);
  if (successCount === 0) {
    console.error('All sources failed — exiting 1 to signal workflow failure.');
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
