# `.fig` container probe

A dependency-free reader for the container layer of Figma's `.fig` design files,
plus the notes we collected while reverse-engineering the shipped Figma desktop
client.

> Status: **research / groundwork**. This does not import `.fig` content into
> Penpot yet. It answers "what is inside this file, byte-wise" so an importer
> has something to build on.

## What the shipped client tells us

The Figma desktop client decides how to read a dropped `.fig` file by looking at
the first bytes of the blob. Recovered from the client bundle (`figma_app`):

```js
// container classifier, from the shipped web client
const isZip = (b) => b.length >= 2 && 80 === b[0] && 75 === b[1]; // 0x50 0x4B = "PK"
const kind  = isZip(head) ? "fig-zip" : "fig-kiwi";
```

The same module also carries four container kind strings:

| kind | meaning |
| --- | --- |
| `fig-kiwi` | raw kiwi-encoded binary container |
| `fig-zip` | ZIP wrapper (`.fig` files saved by recent builds) |
| `fig-diff` | delta container |
| `fig-spec` | spec/inspection container |
| `fig-jam.` | FigJam variant |

So the on-disk format is **not** self-describing through a magic string alone:
the discriminator is simply "is this a ZIP or not".

## Usage

```bash
node probe.mjs path/to/file.fig
node probe.mjs path/to/file.fig --json
```

Output: size, SHA-256, detected container kind, ZIP entry table when zipped, and
a hex/ASCII preview of the header otherwise.

## What this probe does *not* do

- It does not decode kiwi-compressed payloads into nodes.
- It makes no claim about the kiwi schema field order; that is a separate piece
  of work and needs a real sample corpus plus the schema recovered from the
  client.
- It never writes to the input file.

## Why it lives here

Penpot already speaks SVG/JSON natively. `.fig` is the one mainstream format it
cannot read, and it is the main interoperability gap for teams migrating from
Figma. A container reader is the smallest useful first step: it makes the
remaining work (schema, node mapping, assets) measurable instead of speculative.
