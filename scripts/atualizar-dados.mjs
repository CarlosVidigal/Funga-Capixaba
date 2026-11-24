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
import path from 'node:path';

const DATA_PATH = path.resolve('data/funga-capixaba-dados.json');
const STATUS_PATH = path.resolve('data/status.json');

const INAT_API = 'https://api.inaturalist.org/v1';
const INAT_SLUG = 'funga-capixaba';
const INAT_DS = '50c9509d-22c7-4a22-a47d-8c48425ef4a7';
const RB_DS = '4300f8d5-1ae5-49e5-a101-63894b005868'; // herbário RB (JBRJ), coleção do JABOT
const JABOT_HERB = ['RB', 'VIES', 'CAP', 'CEUNES', 'MBML', 'CVRD', 'UFES', 'SRVD'];
const ES_WKT = 'POLYGON((-41.9 -21.35,-39.6 -21.35,-39.6 -17.85,-41.9 -17.85,-41.9 -21.35))';
const ST_ES = 'stateProvince=Esp%C3%ADrito%20Santo&stateProvince=Espirito%20Santo';
const SB = 'https://biocache-service.sibbr.gov.br/biocache-service/occurrences/search';
const SPLINK = 'https://specieslink.net/ws/1.0/search';

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
function slimSplink(f) {
  const p = f.properties || {}, c = f.geometry?.coordinates;
  const sci = String(p.scientificname || '').trim();
  const rank = !sci || /^indet/i.test(sci) ? '' : /aceae$/i.test(sci) ? 'family' : sci.includes(' ') ? 'species' : 'genus';
  const nome = rank ? sci : '';
  const ano = p.yearcollected ? String(p.yearcollected) : '';
  const mes = String(p.monthcollected || '').padStart(2, '0'), dia = String(p.daycollected || '').padStart(2, '0');
  const date = ano && +p.monthcollected && +p.daycollected ? `${ano}-${mes}-${dia}` : ano;
  const inst = String(p.institutioncode || '').toUpperCase();
  const chave = p.catalognumber ? [p.institutioncode, p.collectioncode, p.catalognumber].filter(Boolean).join(':')
    : hash([sci, c?.join('_'), date, p.locality].join('|'));
  return {
    src: 'splink', via: 'splink', id: 'sl' + chave, rec: p.collector || '', uf: 'Espírito Santo', cty: p.county || '', inst,
    alt: num(p.altitude ?? p.minimumelevation), jflag: false, k: keyOf(nome, rank), sci: nome, com: '', rank, tph: '', ph: '',
    lat: c ? +c[1] : null, lng: c ? +c[0] : null, date, grade: '', user: p.collector || inst || '',
    place: [p.locality, p.county].filter(Boolean).join(', '), acc: undefined, obs: false, uri: 'https://specieslink.net/search/',
    ds: ['speciesLink', inst, p.collectioncode].filter(Boolean).join(' · '), inatId: 0
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
  let sel = null, total = 0;
  for (const f of tentativas) {
    try {
      const j = await getJSON(`${SB}?q=kingdom:Fungi${par(f)}&pageSize=0&facet=false`);
      if (j.totalRecords > 0) { sel = f; total = j.totalRecords; break; }
    } catch { /* tenta a próxima estratégia */ }
  }
  if (!sel) throw new Error('SiBBr respondeu sem registros de fungos para o ES (ou recusou os filtros)');
  const out = new Map();
  for (let i = 0; i < total; i += 500) {
    const j = await getJSON(`${SB}?q=kingdom:Fungi${par(sel)}&pageSize=500&start=${i}&startIndex=${i}&facet=false`);
    if (!j.occurrences?.length) break;
    for (const o of j.occurrences) { const r = slimSibbr(o); out.set(r.id, r); }
    await sleep(300);
  }
  return [...out.values()];
}

/* speciesLink exige chave gratuita (secret SPLINK_API_KEY). Sem chave, é ignorado e os dados antigos ficam. */
async function coletarSplink() {
  const chave = process.env.SPLINK_API_KEY;
  if (!chave) return null;
  const out = new Map();
  const limite = 300;
  for (let off = 0; ; off += limite) {
    const j = await getJSON(`${SPLINK}?kingdom=Fungi&stateprovince=Esp%C3%ADrito%20Santo&Coordinates=Yes&limit=${limite}&offset=${off}&apikey=${encodeURIComponent(chave)}`);
    const feats = j.features || [];
    for (const f of feats) if (f.geometry?.coordinates?.length === 2) { const r = slimSplink(f); out.set(r.id, r); }
    if (feats.length < limite || off > 200000) break;
    await sleep(500);
  }
  return [...out.values()];
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

  const novos = {}, falhas = [], puladas = [];
  resultados.forEach((res, i) => {
    const nome = nomes[i], velhos = antigos.filter(GRUPOS[nome]).length;
    if (res.status === 'rejected') { falhas.push(nome); console.log(`::warning::${nome}: ${res.reason?.message || res.reason}`); return; }
    if (res.value === null) { puladas.push(nome); console.log(`${nome}: ignorada (sem chave de API). Dados anteriores mantidos.`); return; }
    if (velhos >= 20 && res.value.length < velhos * MIN_RATIO) {
      falhas.push(nome);
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
    for (const r of novos[nome]) {
      vistos.add(r.id);
      const antigo = porId.get(r.id);
      if (!antigo) { add++; finais.push(r); }
      else if (semEnriquecimento(antigo) === JSON.stringify(r)) finais.push(antigo);
      else { chg++; finais.push(r); }
    }
    for (const id of porId.keys()) if (!vistos.has(id)) rem++;
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
      total, added: add, changed: chg, removed: rem, bySource: porFonte
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

  await fs.mkdir(path.dirname(STATUS_PATH), { recursive: true });
  await fs.writeFile(STATUS_PATH, JSON.stringify({
    lastChecked: agora.toISOString(), version, total, added: add, changed: chg, removed: rem,
    sourcesOk: Object.keys(novos), sourcesFailed: falhas, sourcesSkipped: puladas
  }, null, 2) + '\n', 'utf-8');

  if (process.env.GITHUB_STEP_SUMMARY) {
    const md = `### Base Funga Capixaba · v${version}\n\n| Fonte | Registros |\n|---|---|\n` +
      Object.entries(porFonte).map(([s, n]) => `| ${s} | ${n} |`).join('\n') +
      `\n\n+${add} novos · ${chg} alterados · ${rem} removidos` +
      (falhas.length ? `\n\n⚠️ Fontes com falha (dados antigos mantidos): ${falhas.join(', ')}` : '') + '\n';
    await fs.appendFile(process.env.GITHUB_STEP_SUMMARY, md);
  }
}

main().catch(err => { console.error(err); process.exit(1); });
