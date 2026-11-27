# SPACE agent plugins

Plugin source lives under `.agents/plugins/<plugin>/`. Use this cross-agent layout:

```text
<plugin>/
├── plugin.yaml
├── skills/<skill>/SKILL.md
├── .claude-plugin/plugin.json
└── .codex-plugin/plugin.json
```

When adding or changing plugin content:

1. Keep `plugin.yaml`, `.claude-plugin/plugin.json`, and
   `.codex-plugin/plugin.json` identity, version, and description aligned.
2. Do not bump the plugin version in skill or content pull requests. Bump
   versions only when cutting a plugin release with
   `python3 scripts/bump_plugin_version.py --bump <plugin>`, which updates all
   three manifests together.
3. Update the repository marketplace entries when adding or renaming a plugin.
4. Run `python3 scripts/bump_plugin_version.py --check` and the applicable
   skill and plugin validators.
