#!/usr/bin/env node
/**
 * Funga Capixaba · coleta semanal das bases abertas
 *
 * Gera data/funga-capixaba-dados.json NO MESMO FORMATO que o index.html usa
 * ({ funga:1, raw:{ inat, jabot, gbif, sibbr, splink } }), de modo que o app
 * carrega o arquivo diretamente. Autor do app: Dr. Carlos Augusto Vidigal Fraga Jr.
 *
 * Regras:
 *  - Baixa TUDO de cada fonte, com paginação completa (iNaturalist, GBIF, SiBBr, speciesLink).
 *  - Compara com a versão anterior por id e conta novos, alterados e removidos.
 *  - Fonte que falha (ou volta com menos de metade dos registros antigos) mantém os dados antigos.
 *  - Só grava a base (e sobe a versão) quando algo mudou. data/status.json é regravado
 *    sempre, o que mantém o agendamento do GitHub ativo.
 *  - Sai com erro (workflow vermelho) se NENHUMA fonte responder.
 *
 * A deduplicação entre fontes (iNaturalist x GBIF etc.) continua no app (build()).
 */
import fs from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { spawn } from 'node:child_process';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import readline from 'node:readline';
import os from 'node:os';
import path from 'node:path';

const DATA_PATH = path.resolve('data/funga-capixaba-dados.json');
const STATUS_PATH = path.resolve('data/status.json');
const FFB_PATH = path.resolve('data/funga-ffb.json');

const INAT_API = 'https://api.inaturalist.org/v1';
const INAT_SLUG = 'funga-capixaba';
const INAT_DS = '50c9509d-22c7-4a22-a47d-8c48425ef4a7';
const RB_DS = '4300f8d5-1ae5-49e5-a101-63894b005868'; // herbário RB (JBRJ), coleção do JABOT
const JABOT_HERB = ['RB', 'VIES', 'CAP', 'CEUNES', 'MBML', 'CVRD', 'UFES', 'SRVD'];
const ES_WKT = 'POLYGON((-41.9 -21.35,-39.6 -21.35,-39.6 -17.85,-41.9 -17.85,-41.9 -21.35))';
const ST_ES = 'stateProvince=Esp%C3%ADrito%20Santo&stateProvince=Espirito%20Santo';
const SB = 'https://biocache-service.sibbr.gov.br/biocache-service/occurrences/search';
const SPLINK = 'https://specieslink.net/ws/1.0/search';
// Flora e Funga do Brasil: arquivo Darwin Core do IPT do JBRJ (fonte primária; a cópia do GBIF é mais antiga)
const FFB_URLS = [
  'https://ipt.jbrj.gov.br/jbrj/archive.do?r=lista_especies_flora_brasil',
  'http://ipt.jbrj.gov.br/jbrj/archive.do?r=lista_especies_flora_brasil'
];

const FONTES = ['inat', 'jabot', 'gbif', 'sibbr', 'splink'];
const ENRICH = new Set(['mun', 'ck0', 'ak', 'an', 'ts', 'cks', 'rg', 'rgR']); // campos que o app calcula sozinho
const MIN_RATIO = 0.5; // proteção contra respostas parciais
const MAX_HISTORY = 104; // ~2 anos de histórico semanal

/* ---------- utilidades (iguais às do index.html) ---------- */
const sleep = ms => new Promise(r => setTimeout(r, ms));
const norm = s => String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
const num = v => { const n = parseFloat(v); return isFinite(n) ? n : null; };
const LOW = ['subspecies', 'variety', 'form'];
const keyOf = (n, rank) => norm(LOW.includes(rank) ? String(n || '').split(' ').slice(0, 2).join(' ') : n).replace(/\s+/g, ' ').trim();
const isoDate = d => {
  if (typeof d === 'number') { try { return new Date(d).toISOString().slice(0, 10); } catch { return ''; } }
  return String(d || '').slice(0, 10);
};
const rk = o => o.species ? 'species' : o.genus ? 'genus' : o.family ? 'family' : o.order ? 'order' : o.class ? 'class' : o.phylum ? 'phylum' : o.kingdom ? 'kingdom' : '';
const JB = { pub: '' }; // organização publicadora do JBRJ (descoberta ao carregar o GBIF)

async function getJSON(url, tentativas = 4) {
  let erro;
  for (let t = 0; t < tentativas; t++) {
    try {
      const r = await fetch(url, {
        signal: AbortSignal.timeout(60000),
        headers: { Accept: 'application/json', 'User-Agent': 'funga-capixaba-atualizador (GitHub Actions)' }
      });
      if (r.ok) return await r.json();
      erro = new Error(`HTTP ${r.status}`);
      if (r.status >= 400 && r.status < 500 && r.status !== 429) break; // erro do cliente: não adianta repetir
    } catch (e) { erro = e; }
    await sleep(2000 * (t + 1));
  }
  throw erro;
}

/* ---------- conversão para o formato do app ---------- */
function slimInat(o) {
  const t = o.taxon || {}, c = o.geojson?.coordinates;
  return {
    src: 'inat', id: 'i' + o.id, oid: o.id, k: keyOf(t.name, t.rank), sci: t.name || '', com: t.preferred_common_name || '', rank: t.rank || '',
    tph: t.default_photo?.square_url || '', ph: (o.photos?.[0]?.url || '').replace('square', 'medium'),
    lat: c ? +c[1] : null, lng: c ? +c[0] : null, date: o.observed_on || '', grade: o.quality_grade, user: o.user?.login || '',
    place: o.place_guess || '', acc: o.positional_accuracy, obs: !!o.obscured, uri: o.uri, ds: 'Projeto Funga Capixaba'
  };
}

function slimGbif(o) {
  let ic = (o.institutionCode || '').toUpperCase();
  if (ic === 'SAMES') ic = 'CEUNES';
  if (ic === 'UFES') ic = 'VIES';
  if (ic === 'SRVD' || ic === 'VALE') ic = 'CVRD';
  const sci = o.species || o.genus || o.family || o.order || o.scientificName || '';
  const jb = o.datasetKey === RB_DS || JABOT_HERB.includes(ic) || (JB.pub && o.publishingOrgKey === JB.pub) ||
    /jabot|jbrj/i.test([o.references, o.occurrenceID, o.datasetName].filter(Boolean).join(' '));
  const tr = (o.taxonRank || '').toLowerCase();
  const img = (o.media || []).find(m => m.type === 'StillImage' && m.identifier);
  const m = String(o.occurrenceID || '').match(/inaturalist\.org\/observations\/(\d+)/);
  return {
    src: jb ? 'jabot' : 'gbif', via: 'gbif', id: 'g' + o.key, inatDs: o.datasetKey === INAT_DS, rec: o.recordedBy || '', uf: o.stateProvince || '',
    cty: o.county || o.municipality || '', inst: ic, alt: num(o.elevation ?? o.minimumElevationInMeters), jflag: !!jb, k: keyOf(sci), sci,
    com: o.vernacularName || '', rank: LOW.includes(tr) ? tr : (rk(o) || tr), tph: '', ph: img ? img.identifier : '',
    lat: o.decimalLatitude ?? null, lng: o.decimalLongitude ?? null, date: isoDate(o.eventDate), grade: o.datasetKey === INAT_DS ? 'research' : '',
    user: o.recordedBy || ic || '', place: [o.locality, o.county, o.stateProvince].filter(Boolean).join(', '), acc: o.coordinateUncertaintyInMeters, obs: false,
    uri: 'https://www.gbif.org/occurrence/' + o.key,
    ds: (jb ? 'JABOT (herbário RB) · ' : '') + [ic, o.collectionCode, o.basisOfRecord].filter(Boolean).join(' · '),
    inatId: m ? +m[1] : 0
  };
}

function slimSibbr(o) {
  let ic = (o.institutionCode || o.institutionName || '').toUpperCase();
  if (ic.includes('SAMES')) ic = 'CEUNES';
  if (ic === 'UFES' || ic.includes('UNIVERSIDADE FEDERAL DO ESPÍRITO SANTO')) ic = 'VIES';
  if (ic === 'SRVD' || ic.includes('RESERVA NATURAL VALE')) ic = 'CVRD';
  const sci = o.species || o.genus || o.family || o.scientificName || '';
  const jb = /Jardim Bot[aâ]nico do Rio de Janeiro|\bRB\b|jabot|jbrj/i.test([o.dataResourceName, ic, ic].filter(Boolean).join(' '));
  return {
    src: jb ? 'jabot' : 'sibbr', via: 'sibbr', id: 's' + o.uuid, rec: o.recordedBy || '', uf: o.stateProvince || o.state || '', cty: o.municipality || o.county || '',
    inst: ic, alt: num(o.minimumElevationInMeters ?? o.elevation ?? o.verbatimElevation), jflag: jb, k: keyOf(sci), sci, com: o.vernacularName || '',
    rank: rk(o), tph: '', ph: o.image || '',
    lat: o.decimalLatitude ?? null, lng: o.decimalLongitude ?? null, date: isoDate(o.eventDate), grade: '',
    user: o.recordedBy || ic || '', place: [o.locality, o.municipality || o.county, o.stateProvince || o.state].filter(Boolean).join(', '),
    acc: o.coordinateUncertaintyInMeters, obs: false, uri: 'https://ala-hub.sibbr.gov.br/ala-hub/occurrences/' + o.uuid, ds: o.dataResourceName || ic || ''
  };
}

const hash = s => { let h = 5381; for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) >>> 0; return h.toString(36); };
const pad2 = v => String(v).padStart(2, '0');
function normInst(v) {
  let ic = String(v || '').toUpperCase();
  if (ic === 'SAMES') ic = 'CEUNES';
  if (ic === 'UFES') ic = 'VIES';
  if (ic === 'SRVD' || ic === 'VALE') ic = 'CVRD';
  return ic;
}
/* speciesLink API v1.0: GeoJSON; os campos ficam em feature.properties, em camelCase (DwC) */
function slimSplink(f) {
  const p = f.properties || {}, c = f.geometry?.coordinates;
  const lat = num(p.decimalLatitude) ?? (c ? +c[1] : null);
  const lng = num(p.decimalLongitude) ?? (c ? +c[0] : null);
  const gen = String(p.genus || '').trim(), ep = String(p.specificEpithet || '').trim();
  let sci = '', rank = '';
  if (gen && ep) { sci = `${gen} ${ep}`; rank = 'species'; }
  else if (gen) { sci = gen; rank = 'genus'; }
  else {
    const w = String(p.scientificName || '').trim().split(/\s+/);
    if (/^[A-Z][a-z-]+$/.test(w[0] || '') && /^[a-z-]+$/.test(w[1] || '')) { sci = `${w[0]} ${w[1]}`; rank = 'species'; }
    else if (/^[A-Z][a-z-]+$/.test(w[0] || '') && w.length === 1) { sci = w[0]; rank = /aceae$/.test(w[0]) ? 'family' : 'genus'; }
    else if (p.family) { sci = String(p.family); rank = 'family'; }
  }
  const y = p.yearCollected ? String(p.yearCollected) : '';
  const date = y && +p.monthCollected && +p.dayCollected ? `${y}-${pad2(p.monthCollected)}-${pad2(p.dayCollected)}` : y && +p.monthCollected ? `${y}-${pad2(p.monthCollected)}` : y;
  const inst = normInst(p.institutionCode);
  const chave = p.catalogNumber ? [p.institutionCode, p.collectionCode, p.catalogNumber].filter(Boolean).join(':')
    : p.barcode || hash([sci, lat, lng, date, p.locality].join('|'));
  return {
    src: 'splink', via: 'splink', id: 'sl' + chave, rec: p.recordedBy || '', uf: p.stateProvince || '', cty: p.county || '', inst,
    alt: num(p.minimumElevationInMeters), jflag: false, k: keyOf(sci, rank), sci, com: '', rank, tph: '', ph: '',
    lat, lng, date, grade: '', user: p.recordedBy || inst || '',
    place: [p.locality, p.county, p.stateProvince].filter(Boolean).join(', '), acc: num(p.coordinatePrecision) ?? undefined, obs: false,
    uri: 'https://specieslink.net/search/', ds: ['speciesLink', inst, p.collectionCode].filter(Boolean).join(' · '), inatId: 0
  };
}

/* ---------- coletores (um por fonte, todos com paginação completa) ---------- */
async function coletarInat() {
  const out = new Map();
  let depois = 0, total = 0;
  for (;;) {
    const j = await getJSON(`${INAT_API}/observations?project_id=${INAT_SLUG}&per_page=200&order=asc&order_by=id&id_above=${depois}&locale=pt-BR`);
    if (!total) total = j.total_results || 0;
    if (!j.results?.length) break;
    for (const o of j.results) { const r = slimInat(o); out.set(r.id, r); }
    depois = j.results[j.results.length - 1].id;
    if (out.size >= total) break;
    await sleep(1000); // limite recomendado pelo iNaturalist: ~1 requisição por segundo
  }
  return [...out.values()];
}

async function gbifPaginas(qs, mapa) {
  let off = 0;
  for (;;) {
    const j = await getJSON(`https://api.gbif.org/v1/occurrence/search?taxonKey=5&${qs}&limit=300&offset=${off}`);
    for (const o of j.results || []) { const r = slimGbif(o); mapa.set(r.id, r); }
    off += (j.results || []).length;
    if (j.endOfRecords || !j.results?.length || off + 300 > 100000) break;
    await sleep(200);
  }
}
async function coletarGbif() {
  try {
    const j = await getJSON(`https://api.gbif.org/v1/occurrence/search?datasetKey=${RB_DS}&limit=1`);
    JB.pub = j.results?.[0]?.publishingOrgKey || '';
  } catch { /* sem a organização, o JABOT é reconhecido pelos outros critérios */ }
  let mapa = null, ultimo = null;
  for (const qs of ['gadmGid=BRA.8_1', 'country=BR&geometry=' + encodeURIComponent(ES_WKT), 'country=BR&hasCoordinate=true&' + ST_ES]) {
    const m = new Map();
    try { await gbifPaginas(qs, m); mapa = m; break; } catch (e) { ultimo = e; }
  }
  if (!mapa) throw new Error((ultimo?.message || 'falha') + ' (tentadas 3 formas de consulta)');
  try { await gbifPaginas('country=BR&hasCoordinate=false&' + ST_ES, mapa); } catch { /* registros sem coordenadas: opcional */ }
  return [...mapa.values()];
}

async function coletarSibbr() {
  const tentativas = [{ fq: 'state:"Espírito Santo"' }, { fq: 'stateProvince:"Espírito Santo"' }, { fq: 'state:"Espirito Santo"' }, { wkt: ES_WKT }];
  const par = f => f.fq ? '&fq=' + encodeURIComponent(f.fq) : '&wkt=' + encodeURIComponent(f.wkt);
  let sel = null, total = 0, ultimoErro = '';
  for (const f of tentativas) {
    try {
      const j = await getJSON(`${SB}?q=kingdom:Fungi${par(f)}&pageSize=0&facet=false`);
      if (j.totalRecords > 0) { sel = f; total = j.totalRecords; break; }
      ultimoErro = 'resposta sem registros';
    } catch (e) { ultimoErro = String(e.message || e); /* tenta a próxima estratégia */ }
  }
  if (!sel) throw new Error(`SiBBr sem resultado para fungos do ES (último erro: ${ultimoErro})`);
  const out = new Map();
  for (let i = 0; i < total; i += 500) {
    const j = await getJSON(`${SB}?q=kingdom:Fungi${par(sel)}&pageSize=500&start=${i}&startIndex=${i}&facet=false`);
    if (!j.occurrences?.length) break;
    for (const o of j.occurrences) { const r = slimSibbr(o); out.set(r.id, r); }
    await sleep(300);
  }
  return [...out.values()];
}

/* speciesLink exige chave gratuita (secret SPLINK_API_KEY). Sem chave, é ignorado e os dados antigos ficam.
   Filtros iguais aos do site: reino Fungi e estado do ES. Inclui registros sem coordenadas. A API aceita vários
   valores separados por vírgula e devolve no máximo 5000 por página (numberMatched vem na primeira). */
async function coletarSplink() {
  const chave = process.env.SPLINK_API_KEY;
  if (!chave) return null;
  const estados = encodeURIComponent('ES,Espírito Santo,Espirito Santo');
  const LIM = 1000;
  const url = off => `${SPLINK}?kingdom=Fungi&stateProvince=${estados}&offset=${off}&limit=${LIM}&apikey=${encodeURIComponent(chave)}`;
  const out = new Map();
  let total = null;
  for (let off = 0; ; off += LIM) {
    const j = await getJSON(url(off));
    if (j.error || j.detail) throw new Error(`speciesLink: ${j.error || j.detail}`);
    if (total === null) total = j.numberMatched ?? null;
    const feats = j.features || [];
    for (const f of feats) { const r = slimSplink(f); out.set(r.id, r); }
    if (feats.length < LIM || (total !== null && off + LIM >= total) || off > 200000) break;
    await sleep(500);
  }
  console.log(`splink: ${total ?? '?'} registros informados pela API, ${out.size} únicos coletados.`);
  if (total && out.size < total * 0.9) console.log(`::warning::splink: coletados ${out.size} de ${total} (ids repetidos ou páginas incompletas).`);
  return [...out.values()];
}

/* ---------- Flora e Funga do Brasil (checklist oficial) ---------- */
async function baixarArquivo(url, destino) {
  const r = await fetch(url, { signal: AbortSignal.timeout(15 * 60 * 1000), headers: { 'User-Agent': 'funga-capixaba-atualizador (GitHub Actions)' } });
  if (!r.ok) throw new Error(`HTTP ${r.status} em ${url}`);
  await pipeline(Readable.fromWeb(r.body), createWriteStream(destino));
  return { lastModified: r.headers.get('last-modified') || null };
}

/* lê uma tabela TSV de dentro do zip (usa o unzip do sistema, presente no ubuntu-latest), linha a linha */
function lerTabela(zip, nome, aoLinha) {
  return new Promise((resolve, reject) => {
    const p = spawn('unzip', ['-p', zip, nome]);
    const rl = readline.createInterface({ input: p.stdout, crlfDelay: Infinity });
    let cab = null;
    rl.on('line', l => {
      if (!l) return;
      const c = l.split('\t');
      if (!cab) { cab = c.map(h => h.replace(/^\ufeff/, '').replace(/^.*[\/#]/, '').trim()); return; }
      const o = {};
      for (let i = 0; i < cab.length; i++) o[cab[i]] = c[i];
      aoLinha(o);
    });
    const fim = new Promise(r => rl.on('close', r));
    const saida = new Promise((res, rej) => { p.on('error', rej); p.on('close', code => (code === 0 || code === null ? res() : rej(new Error(`unzip falhou (${code}) ao abrir ${nome}`)))); });
    p.stderr.on('data', () => {});
    Promise.all([fim, saida]).then(() => resolve(cab), reject);
  });
}

const FFB_ACEITO = /^(NOME_ACEITO|ACCEPTED)$/i, FFB_SIN = /^(SINONIMO|SYNONYM)$/i;
async function atualizarFfb() {
  const zip = path.join(os.tmpdir(), 'ffb-dwca.zip');
  let meta = null, erro = null, urlUsada = '';
  for (const u of FFB_URLS) { try { meta = await baixarArquivo(u, zip); urlUsada = u; break; } catch (e) { erro = e; } }
  if (!meta) throw new Error(`não foi possível baixar o arquivo do IPT: ${erro?.message}`);

  const taxa = new Map();
  const cab = await lerTabela(zip, 'taxon.txt', o => {
    if (!/fungi/i.test(o.kingdom || '')) return;
    taxa.set(o.id, { g: (o.genus || '').trim(), e: (o.specificEpithet || '').trim(), r: o.taxonRank || '', st: o.taxonomicStatus || '', aid: o.acceptedNameUsageID || '', an: o.acceptedNameUsage || '' });
  });
  if (!cab || !['id', 'kingdom', 'genus', 'specificEpithet', 'taxonomicStatus'].every(c => cab.includes(c)))
    throw new Error(`taxon.txt com estrutura inesperada (colunas: ${(cab || []).join(', ')})`);
  if (taxa.size < 1000) throw new Error(`apenas ${taxa.size} registros de Fungi no taxon.txt; arquivo suspeito`);

  const dist = new Map(); // id -> 1 (consta o ES) | 0 (tem distribuição, sem o ES)
  await lerTabela(zip, 'distribution.txt', o => {
    if (!taxa.has(o.id)) return;
    const es = /^BR-ES$/i.test((o.locationID || '').trim()) || /Esp[ií]rito Santo/i.test(o.locality || '');
    dist.set(o.id, dist.get(o.id) === 1 || es ? 1 : 0);
  });
  const esDe = id => (dist.has(id) ? dist.get(id) : -1);
  const binomio = t => (t.g && t.e ? `${t.g} ${t.e}` : '');
  const ehEsp = t => /^(ESPECIE|SPECIES)$/i.test(t.r);

  const names = {};
  let nA = 0, nS = 0;
  // 1) nomes aceitos (espécies); 2) sinônimos; 3) infraespecíficos só se o binômio ainda não existir
  for (const [id, t] of taxa) if (ehEsp(t) && FFB_ACEITO.test(t.st) && binomio(t)) { const k = keyOf(binomio(t)); if (!names[k]) { names[k] = ['A', '', esDe(id)]; nA++; } }
  for (const [, t] of taxa) {
    if (!ehEsp(t) || !FFB_SIN.test(t.st) || !binomio(t)) continue;
    const k = keyOf(binomio(t));
    if (names[k]) continue;
    const ac = taxa.get(t.aid);
    const nome = ac && binomio(ac) ? binomio(ac) : String(t.an).split(/\s+/).slice(0, 2).join(' ');
    if (!nome) continue;
    names[k] = ['S', nome, ac ? esDe(t.aid) : -1]; nS++;
  }
  for (const [id, t] of taxa) {
    if (ehEsp(t) || !binomio(t)) continue;
    const k = keyOf(binomio(t));
    if (names[k]) continue;
    if (FFB_ACEITO.test(t.st)) names[k] = ['A', '', esDe(id)];
  }

  let antigos = null;
  try { antigos = JSON.parse(await fs.readFile(FFB_PATH, 'utf-8')); } catch { /* primeira vez */ }
  const mudou = !antigos || JSON.stringify(antigos.names) !== JSON.stringify(names);
  if (mudou) {
    await fs.mkdir(path.dirname(FFB_PATH), { recursive: true });
    await fs.writeFile(FFB_PATH, JSON.stringify({
      source: 'Flora e Funga do Brasil (JBRJ, arquivo Darwin Core do IPT)', url: urlUsada.replace(/^http:/, 'https:'),
      lastModified: meta.lastModified, generated: new Date().toISOString(),
      counts: { accepted: nA, synonyms: nS, names: Object.keys(names).length },
      // chave normalizada -> [situação (A aceito | S sinônimo), nome aceito (só para sinônimos), ES (1 consta | 0 não consta | -1 sem dados)]
      names
    }), 'utf-8');
  }
  console.log(`Flora e Funga do Brasil: ${nA} nomes aceitos, ${nS} sinônimos${mudou ? ' (arquivo atualizado)' : ' (sem mudanças)'}.`);
  await fs.rm(zip, { force: true });
  return { ok: true, names: Object.keys(names).length, accepted: nA, synonyms: nS, changed: mudou, lastModified: meta.lastModified };
}

/* ---------- base atual ---------- */
async function carregarBase() {
  try {
    const j = JSON.parse(await fs.readFile(DATA_PATH, 'utf-8'));
    for (const s of FONTES) j.raw[s] ||= [];
    j.syncHistory ||= [];
    j.version ||= 0;
    return j;
  } catch {
    return { funga: 1, version: 0, lastUpdated: null, syncHistory: [], raw: Object.fromEntries(FONTES.map(s => [s, []])) };
  }
}

const GRUPOS = {
  inat: o => o.src === 'inat',
  gbif: o => o.via === 'gbif',
  sibbr: o => o.via === 'sibbr',
  splink: o => o.src === 'splink'
};
const semEnriquecimento = o => JSON.stringify(Object.fromEntries(Object.entries(o).filter(([k]) => !ENRICH.has(k))));

async function main() {
  const base = await carregarBase();
  const antigos = FONTES.flatMap(s => base.raw[s]);

  const consultas = { inat: coletarInat, gbif: coletarGbif, sibbr: coletarSibbr, splink: coletarSplink };
  const nomes = Object.keys(consultas);
  const resultados = await Promise.allSettled(nomes.map(n => consultas[n]()));

  const novos = {}, falhas = [], puladas = [], motivos = {}, mudancas = {};
  resultados.forEach((res, i) => {
    const nome = nomes[i], velhos = antigos.filter(GRUPOS[nome]).length;
    if (res.status === 'rejected') { falhas.push(nome); motivos[nome] = String(res.reason?.message || res.reason); console.log(`::warning::${nome}: ${motivos[nome]}`); return; }
    if (res.value === null) { puladas.push(nome); console.log(`${nome}: ignorada (sem chave de API). Dados anteriores mantidos.`); return; }
    if (velhos >= 20 && res.value.length < velhos * MIN_RATIO) {
      falhas.push(nome);
      motivos[nome] = `voltou com ${res.value.length} registros contra ${velhos} na base (resposta parcial)`;
      console.log(`::warning::${nome}: voltou com ${res.value.length} registros contra ${velhos} na base. Resposta tratada como parcial; dados anteriores mantidos.`);
      return;
    }
    novos[nome] = res.value;
    console.log(`${nome}: ${res.value.length} registros coletados.`);
  });

  if (!Object.keys(novos).length) throw new Error('Nenhuma fonte retornou dados utilizáveis. Base não alterada.');

  /* upsert por id: mantém o objeto antigo (com campos calculados pelo app) quando nada mudou */
  const finais = [];
  let add = 0, chg = 0, rem = 0;
  for (const [nome, pertence] of Object.entries(GRUPOS)) {
    const velhos = antigos.filter(pertence);
    if (!novos[nome]) { finais.push(...velhos); continue; }
    const porId = new Map(velhos.map(o => [o.id, o]));
    const vistos = new Set();
    const d = (mudancas[nome] = { added: 0, changed: 0, removed: 0 });
    for (const r of novos[nome]) {
      vistos.add(r.id);
      const antigo = porId.get(r.id);
      if (!antigo) { add++; d.added++; finais.push(r); }
      else if (semEnriquecimento(antigo) === JSON.stringify(r)) finais.push(antigo);
      else { chg++; d.changed++; finais.push(r); }
    }
    for (const id of porId.keys()) if (!vistos.has(id)) { rem++; d.removed++; }
  }
  finais.push(...antigos.filter(o => !Object.values(GRUPOS).some(f => f(o)))); // registros fora dos grupos: preservados

  const raw = Object.fromEntries(FONTES.map(s => [s, []]));
  for (const r of finais) (raw[r.src] ||= []).push(r);
  for (const s in raw) raw[s].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)); // ordem estável = diffs pequenos no Git
  const porFonte = Object.fromEntries(Object.entries(raw).map(([s, a]) => [s, a.length]));
  const total = Object.values(porFonte).reduce((a, b) => a + b, 0);

  const agora = new Date();
  const mudou = add + chg + rem > 0 || base.version === 0;
  let version = base.version;

  if (mudou) {
    version += 1;
    const rotulo = agora.toLocaleDateString('pt-BR', { timeZone: 'America/Sao_Paulo' });
    const historico = [...base.syncHistory, {
      version: `v${version}`, label: `v${version} (${rotulo})`, date: agora.toISOString(),
      total, added: add, changed: chg, removed: rem, bySource: porFonte, changes: mudancas
    }].slice(-MAX_HISTORY);
    const saida = { funga: 1, version, lastUpdated: agora.toISOString(), syncHistory: historico, raw };
    await fs.mkdir(path.dirname(DATA_PATH), { recursive: true });
    const tmp = DATA_PATH + '.tmp';
    await fs.writeFile(tmp, JSON.stringify(saida), 'utf-8');
    await fs.rename(tmp, DATA_PATH);
    console.log(`Base atualizada para v${version}: +${add} novos, ${chg} alterados, ${rem} removidos (total ${total}).`);
  } else {
    console.log(`Nenhuma mudança. Versão v${version} mantida (total ${total}).`);
  }

  let ffb;
  try { ffb = await atualizarFfb(); }
  catch (e) { ffb = { ok: false, error: String(e.message || e) }; console.log(`::warning::Flora e Funga do Brasil: ${ffb.error}. Arquivo anterior mantido.`); }

  await fs.mkdir(path.dirname(STATUS_PATH), { recursive: true });
  await fs.writeFile(STATUS_PATH, JSON.stringify({
    lastChecked: agora.toISOString(), version, total, added: add, changed: chg, removed: rem, changes: mudancas, failureReasons: motivos,
    sourcesOk: Object.keys(novos), sourcesFailed: falhas, sourcesSkipped: puladas, ffb
  }, null, 2) + '\n', 'utf-8');

  if (process.env.GITHUB_STEP_SUMMARY) {
    const grupos = { inat: 'iNaturalist', gbif: 'GBIF (inclui JABOT via GBIF)', sibbr: 'SiBBr (inclui JABOT via SiBBr)', splink: 'speciesLink' };
    const md = `### Base Funga Capixaba · v${version}\n\n| Fonte | Registros |\n|---|---|\n` +
      Object.entries(porFonte).map(([s, n]) => `| ${s} | ${n} |`).join('\n') +
      `\n\n**Mudanças nesta execução** (total: +${add} novos · ${chg} alterados · ${rem} removidos)\n\n| Fonte | Novos | Alterados | Removidos | Situação |\n|---|---|---|---|---|\n` +
      Object.entries(grupos).map(([g, rot]) => {
        const m = mudancas[g], sit = novos[g] ? 'ok' : puladas.includes(g) ? 'ignorada (sem chave)' : `⚠️ falhou: ${motivos[g] || 'sem detalhe'}`;
        return `| ${rot} | ${m ? m.added : '–'} | ${m ? m.changed : '–'} | ${m ? m.removed : '–'} | ${sit} |`;
      }).join('\n') +
      (falhas.length ? `\n\n⚠️ Fontes com falha (dados antigos mantidos): ${falhas.join(', ')}` : '') +
      (ffb.ok ? `\n\nFlora e Funga do Brasil: ${ffb.names} nomes (${ffb.accepted} aceitos, ${ffb.synonyms} sinônimos)` : `\n\n⚠️ Flora e Funga do Brasil: ${ffb.error}`) + '\n';
    await fs.appendFile(process.env.GITHUB_STEP_SUMMARY, md);
  }
}

main().catch(err => { console.error(err); process.exit(1); });
