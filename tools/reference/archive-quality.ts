import { parseArgs } from 'node:util';
import { writeFile } from 'node:fs/promises';
import { readPendulumflow } from './pendulumflow.ts';

const { values } = parseArgs({ options: {
  directory: { type: 'string' }, out: { type: 'string' },
  'max-events': { type: 'string', default: '1000000' },
  'max-bytes': { type: 'string', default: '256000000' },
  'max-book-gap-ms': { type: 'string', default: '1000' },
} });
if (!values.directory || !values.out) throw new Error('--directory and --out are required');
const dataset = await readPendulumflow(values.directory, {
  maxEvents: Number(values['max-events']), maxBytes: Number(values['max-bytes']),
  maxBookGapMs: Number(values['max-book-gap-ms']),
});
await writeFile(values.out, JSON.stringify(dataset.quality, null, 2), { flag: 'wx' });
console.log(JSON.stringify({ diagnosticWindows: dataset.quality.markets.filter(m => m.replayable).length,
  windows: dataset.markets.length, scoringEligible: false, liveEligible: false }));
