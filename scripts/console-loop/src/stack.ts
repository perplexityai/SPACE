// Which stack a run acts on. The loop drives the console, whose `/auth` and
// `/admin` proxies reach the backends its build names, while the seeder and
// cleanup call `apiUrl` directly; the loopback check on the two URLs does not
// show that they are one stack. Every stack's Postgres holds one cluster row
// whose `id` is generated when the database is created (`managing_org_id` is
// the same fixed ID on every stack), so the seed records that `id`, and every
// mutating run reads it through the console and through the API and refuses
// when the readings differ from each other or from the seed state.
import { Api } from './api.ts';
import type { Config } from './config.ts';

export interface StackIdentity {
  clusterId: string;
  clusterCreatedAt: string;
}

interface Cluster { id?: string; created_at?: string }

function identityOf(cluster: Cluster | null, where: string): StackIdentity {
  if (!cluster?.id || !cluster.created_at) throw new Error(`${where} returned no cluster identity`);
  return { clusterId: cluster.id, clusterCreatedAt: cluster.created_at };
}

export function sameStack(a: StackIdentity, b: StackIdentity): boolean {
  return a.clusterId === b.clusterId && a.clusterCreatedAt === b.clusterCreatedAt;
}

const show = (identity: StackIdentity) => `cluster ${identity.clusterId} (created ${identity.clusterCreatedAt})`;

// The readings that must agree: the API's admin and session routes, and the
// same two through the console's origin, each signed in on its own.
export interface StackReadings {
  readings: [string, StackIdentity][];
  api: Api;
}

export async function readStack(config: Config): Promise<StackReadings> {
  const readings: [string, StackIdentity][] = [];
  let api: Api | null = null;
  for (const [name, base] of [['API', config.apiUrl], ['console', config.baseUrl]] as const) {
    const client = new Api(config, base);
    try {
      await client.signIn(config.email, config.password);
      readings.push([`${name} ${base}/admin/api/v1/cluster`, identityOf(await client.admin<Cluster>('GET', '/cluster'), `${name} admin`)]);
      readings.push([`${name} ${base}/auth/v1/cluster`, identityOf(await client.request<Cluster>('GET', '/auth/v1/cluster'), `${name} auth`)]);
    } catch (err) {
      throw new Error(`cannot read the stack's identity through the ${name} at ${base}: ${err instanceof Error ? err.message : String(err)}`);
    }
    api ??= client;
  }
  return { readings, api: api! };
}

// Refuse unless every reading agrees and, when the state recorded a stack,
// matches it. Returns the API client, signed in, and the identity.
export async function proveSameStack(config: Config, recorded: StackIdentity | null, what: string, stateFile: string): Promise<{ api: Api; identity: StackIdentity }> {
  const { readings, api } = await readStack(config);
  const [, identity] = readings[0];
  const disagree = readings.filter(([, reading]) => !sameStack(reading, identity));
  if (disagree.length) {
    const lines = readings.map(([where, reading]) => `${where}: ${show(reading)}`).join('; ');
    throw new Error(`refusing to ${what}: the console and the API are not one stack (${lines})`);
  }
  if (recorded && !sameStack(recorded, identity)) {
    throw new Error(
      `refusing to ${what}: the seed state ${stateFile} was recorded on ${show(recorded)}, but the target is ${show(identity)}. `
      + `If the stack the state was recorded on no longer exists, delete ${stateFile}; its objects went with that stack. `
      + 'Otherwise point --api-url and --base-url at that stack.',
    );
  }
  console.log(`[${what}] stack: ${show(identity)}, read through the console and the API`);
  return { api, identity };
}
