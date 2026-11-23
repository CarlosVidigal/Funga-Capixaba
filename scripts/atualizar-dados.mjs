import fs from 'node:fs/promises';
import path from 'node:path';

const DATA_PATH = path.resolve('data/funga-es.json');

// Carrega base existente ou cria estrutura inicial
async function carregarBaseAtual() {
  try {
    const raw = await fs.readFile(DATA_PATH, 'utf-8');
    return JSON.parse(raw);
  } catch {
    return {
      version: 0,
      lastUpdated: null,
      syncHistory: [],
      records: []
    };
  }
}

// 1. GBIF (Fungi + Espírito Santo)
async function buscarGBIF() {
  const url = 'https://api.gbif.org/v1/occurrence/search?kingdomKey=5&country=BR&stateProvince=Esp%C3%ADrito%20Santo&hasCoordinate=true&limit=300';
  const res = await fetch(url);
  if (!res.ok) throw new Error(`GBIF HTTP ${res.status}`);
  const data = await res.json();

  return (data.results || [])
    .filter(r => r.decimalLatitude && r.decimalLongitude)
    .map(r => ({
      id: `gbif-${r.key}`,
      source: 'GBIF',
      species: r.species || r.scientificName || 'Fungi sp.',
      lat: Number(r.decimalLatitude),
      lng: Number(r.decimalLongitude),
      municipality: r.municipality || r.locality || 'Espírito Santo',
      date: r.eventDate ? r.eventDate.slice(0, 10) : 'Sem data'
    }));
}

// 2. iNaturalist (Fungi + Espírito Santo - place_id 7503)
async function buscarINat() {
  const url = 'https://api.inaturalist.org/v1/observations?taxon_id=47170&place_id=7503&per_page=200&geo=true';
  const res = await fetch(url);
  if (!res.ok) throw new Error(`iNaturalist HTTP ${res.status}`);
  const data = await res.json();

  return (data.results || [])
    .filter(r => r.geojson?.coordinates?.length === 2)
    .map(r => ({
      id: `inat-${r.id}`,
      source: 'iNaturalist',
      species: r.taxon?.name || 'Fungi sp.',
      lat: Number(r.geojson.coordinates[1]),
      lng: Number(r.geojson.coordinates[0]),
      municipality: r.place_guess || 'Espírito Santo',
      date: r.observed_on || 'Sem data'
    }));
}

// 3. SiBBr (Fungi + Espírito Santo)
async function buscarSiBBr() {
  const url = 'https://ala-bie.sibbr.gov.br/ala-bie-ws/occurrences/search?q=kingdom:Fungi&fq=state:Esp%C3%ADrito%20Santo&pageSize=200';
  const res = await fetch(url);
  if (!res.ok) throw new Error(`SiBBr HTTP ${res.status}`);
  const data = await res.json();

  return (data.occurrences || [])
    .filter(r => r.decimalLatitude && r.decimalLongitude)
    .map(r => ({
      id: `sibbr-${r.uuid}`,
      source: 'SiBBr',
      species: r.scientificName || 'Fungi sp.',
      lat: Number(r.decimalLatitude),
      lng: Number(r.decimalLongitude),
      municipality: r.municipality || 'Espírito Santo',
      date: r.eventDate ? String(r.eventDate).slice(0, 10) : 'Sem data'
    }));
}

// 4. speciesLink (Requer chave gratuita em SPLINK_API_KEY)
async function buscarSpeciesLink() {
  const apiKey = process.env.SPLINK_API_KEY;
  if (!apiKey) {
    console.log('SPLINK_API_KEY não configurada. Pulando consulta ao speciesLink.');
    return [];
  }

  const url = `https://specieslink.net/ws/1.0/search?kingdom=Fungi&stateprovince=Esp%C3%ADrito%20Santo&Coordinates=Yes&limit=300&apikey=${encodeURIComponent(akLGteRvja2v8XYq8tKJ)}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`speciesLink HTTP ${res.status}`);
  const data = await res.json();

  return (data.features || [])
    .filter(f => f.geometry?.coordinates?.length === 2)
    .map((f, idx) => ({
      id: `splink-${f.properties?.catalognumber || idx}-${f.geometry.coordinates.join('_')}`,
      source: 'speciesLink',
      species: f.properties?.scientificname || 'Fungi sp.',
      lat: Number(f.geometry.coordinates[1]),
      lng: Number(f.geometry.coordinates[0]),
      municipality: f.properties?.county || f.properties?.locality || 'Espírito Santo',
      date: f.properties?.yearcollected ? String(f.properties.yearcollected) : 'Sem data'
    }));
}

async function main() {
  const db = await carregarBaseAtual();
  const idsExistentes = new Set(db.records.map(r => r.id));

  const consultas = await Promise.allSettled([
    buscarGBIF(),
    buscarINat(),
    buscarSiBBr(),
    buscarSpeciesLink()
  ]);

  const recebidos = [];
  consultas.forEach((res, i) => {
    const nomes = ['GBIF', 'iNaturalist', 'SiBBr', 'speciesLink'];
    if (res.status === 'fulfilled') {
      console.log(`${nomes[i]}: ${res.value.length} registros retornados.`);
      recebidos.push(...res.value);
    } else {
      console.warn(`Aviso ao consultar ${nomes[i]}:`, res.reason.message);
    }
  });

  // Filtra apenas registros inéditos
  const novos = [];
  for (const reg of recebidos) {
    if (!idsExistentes.has(reg.id)) {
      idsExistentes.add(reg.id);
      novos.push(reg);
    }
  }

  if (novos.length > 0) {
    db.records.push(...novos);
    db.version += 1;
    db.lastUpdated = new Date().toISOString();

    const dataLabel = new Date().toLocaleDateString('pt-BR');
    db.syncHistory.push({
      version: `v${db.version}`,
      label: `v${db.version} (${dataLabel})`,
      total: db.records.length,
      added: novos.length
    });

    await fs.mkdir(path.dirname(DATA_PATH), { recursive: true });
    await fs.writeFile(DATA_PATH, JSON.stringify(db, null, 2), 'utf-8');
    console.log(`Base atualizada para v${db.version}: +${novos.length} novos registros (Total: ${db.records.length}).`);
  } else {
    console.log('Nenhum registro novo encontrado. Versão mantida sem alterações.');
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
