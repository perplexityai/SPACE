// Where the Templates views link to.

export type TemplateSection = 'overview' | 'releases' | 'builds' | 'pool' | 'nodes' | 'sandboxes';
export const TEMPLATE_SECTIONS: readonly TemplateSection[] = ['overview', 'releases', 'builds', 'pool', 'nodes', 'sandboxes'];

// A template's page, on one of its sections; `release` marks a release there.
export function templateHref(name: string, section: TemplateSection = 'overview', release?: string): string {
  const params = new URLSearchParams();
  if (section !== 'overview') params.set('tab', section);
  if (release) params.set('release', release);
  const query = params.toString();
  return `/dashboard/templates/${encodeURIComponent(name)}${query ? `?${query}` : ''}`;
}

// The sandbox list, filtered by the template.
export function templateSandboxesHref(name: string): string {
  return `/dashboard/sandboxes?${new URLSearchParams({ template: name })}`;
}
