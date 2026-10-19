import { fileURLToPath } from 'node:url';
import { resolve, join } from 'node:path';
import { ApplicationError, commandMain, invokeCLI, privateDirectory } from './adapter-runtime.mjs';
const root = fileURLToPath(new URL('../', import.meta.url));
const cases = new Set(['starter', 'busy', 'conflicting-correction', 'unreadable-total', 'hostile-note', 'duplicate-delivery', 'queued-cancelled', 'interrupted-lease']);
export async function runApplication(input, parameters, signal, invocation) {
  if (Object.keys(input).some(key => !['caseId', 'action'].includes(key)) || !cases.has(input.caseId) || !['work', 'inspect'].includes(input.action)) throw new Error('Select a reviewed case and explicit work/inspect action.');
  const directory = await privateDirectory(root, invocation), db = join(directory, 'jobs.db');
  const cli = ['uv', 'run', '--locked', '--no-sync', 'document-worker'];
  try {
    const initialized = await invokeCLI([...cli, '--db', db, 'init-case', input.caseId], { cwd: root, signal });
    const argv = ['uv', 'run', '--locked', '--no-sync', 'python', 'firedrill/request.py', '--db', db, '--action', input.action];
    if (parameters.model) argv.push('--model', parameters.model);
    const output = await invokeCLI(argv, { cwd: root, signal, timeoutMs: 180000 });
    return { application: output, initialized, localState: { directory, databaseFile: db } };
  } catch (error) { if (error instanceof ApplicationError) error.evidence.runDirectory = directory; throw error; }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await commandMain(runApplication);
