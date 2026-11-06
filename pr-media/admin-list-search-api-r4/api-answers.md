# Admin list search on Kind (API at #1523's head, drop migration applied)

Seeded running sandbox: sbx_01a1af0c-af1f-73c0-bbcd-e390b041cae7 (label b041cae7). Each line: query, HTTP status, then the listed IDs' last 8 digits or the error.

- `search=b041cae7&cursor=` → 200; rows: b041cae7
- `search=b041cae7` → 200; rows: b041cae7
- `search=cae7&cursor=` → 200; rows: b041cae7
- `search=cae&cursor=` → 400; error: search takes 4 to 32 hex digits of a sandbox ID, after an optional sbx_ and with dashes left out: IDs that end with them match, and from 8 digits IDs that start with them too. To match the start of a name, use prefix, which is case-sensitive
- `search=01a1af0c&cursor=` → 200; rows: 913633b8, 65d6c7b7, b96eda40, b041cae7
- `search=01a1af0&cursor=` → 200; rows: (none)
- `search=sbx_01A1AF0C-AF1F&cursor=` → 200; rows: b041cae7
- `search=loop-suspended&cursor=` → 400; error: search takes 4 to 32 hex digits of a sandbox ID, after an optional sbx_ and with dashes left out: IDs that end with them match, and from 8 digits IDs that start with them too. To match the start of a name, use prefix, which is case-sensitive
- `search=loop-suspended` → 400; error: search takes 4 to 32 hex digits of a sandbox ID, after an optional sbx_ and with dashes left out: IDs that end with them match, and from 8 digits IDs that start with them too. To match the start of a name, use prefix, which is case-sensitive
- `prefix=loop-suspended&cursor=` → 200; rows: 65d6c7b7
- `prefix=LOOP-SUSPENDED&cursor=` → 200; rows: (none)
