#!/usr/bin/env node
// Zero-dependency probe for Figma `.fig` containers.
// Reads the container layer only; never decodes kiwi payloads, never writes.

import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

const ZIP_LOCAL = 0x04034b50;
const ZIP_CENTRAL = 0x02014b50;
const ZIP_EOCD = 0x06054b50;

const hex = (buf, n = 32) => buf.subarray(0, n).toString('hex').match(/.{1,2}/g)?.join(' ') ?? '';
const ascii = (buf, n = 32) =>
  [...buf.subarray(0, n)].map((b) => (b >= 32 && b < 127 ? String.fromCharCode(b) : '.')).join('');

export function classify(buf) {
  // The shipped client checks only the first two bytes: 0x50 0x4B ("PK").
  const zipped = buf.length >= 2 && buf[0] === 0x50 && buf[1] === 0x4b;
  return { zipped, kind: zipped ? 'fig-zip' : 'fig-kiwi' };
}

function findEocd(buf) {
  const min = Math.max(0, buf.length - 65557 - 22);
  for (let i = buf.length - 22; i >= min; i--) {
    if (buf.readUInt32LE(i) === ZIP_EOCD) return i;
  }
  return -1;
}

export function readZipDirectory(buf) {
  const eocd = findEocd(buf);
  if (eocd < 0) return { error: 'end-of-central-directory not found' };

  let entries = buf.readUInt16LE(eocd + 10);
  let cdSize = buf.readUInt32LE(eocd + 12);
  let cdOffset = buf.readUInt32LE(eocd + 16);
  let truncated = false;

  if (entries === 0xffff || cdOffset === 0xffffffff) {
    truncated = true; // zip64 marker; not needed for a format probe
  }
  if (cdOffset >= buf.length) {
    return { error: 'central directory offset outside file', entries, cdOffset, cdSize };
  }

  const out = [];
  let p = cdOffset;
  let guard = 0;
  while (p + 46 <= buf.length && guard++ < entries + 8) {
    if (buf.readUInt32LE(p) !== ZIP_CENTRAL) break;
    const method = buf.readUInt16LE(p + 10);
    const compSize = buf.readUInt32LE(p + 20);
    const uncompSize = buf.readUInt32LE(p + 24);
    const nameLen = buf.readUInt16LE(p + 28);
    const extraLen = buf.readUInt16LE(p + 30);
    const commentLen = buf.readUInt16LE(p + 32);
    const localOffset = buf.readUInt32LE(p + 42);
    const name = buf.subarray(p + 46, p + 46 + nameLen).toString('utf8');
    out.push({ name, method, compSize, uncompSize, localOffset });
    p += 46 + nameLen + extraLen + commentLen;
  }

  return { entries, cdOffset, cdSize, zip64: truncated, files: out };
}

export function probe(path) {
  const buf = readFileSync(path);
  const { zipped, kind } = classify(buf);
  const report = {
    path,
    bytes: buf.length,
    sha256: createHash('sha256').update(buf).digest('hex'),
    container: kind,
    header: { hex: hex(buf, 32), ascii: ascii(buf, 32) },
    zip: null,
    notes: [],
  };

  if (zipped) {
    const dir = readZipDirectory(buf);
    report.zip = dir;
    if (dir.files) {
      dir.files.sort((a, b) => b.uncompSize - a.uncompSize);
      report.notes.push(`${dir.files.length} entries, ${dir.files.reduce((s, f) => s + f.uncompSize, 0)} uncompressed bytes`);
      const canvas = dir.files.find((f) => /canvas\.fig$/i.test(f.name));
      if (canvas) report.notes.push(`canvas payload: ${canvas.name} (${canvas.uncompSize} bytes)`);
    }
  } else {
    report.notes.push('not a ZIP: treated as a raw kiwi container by the client');
    if (buf.length > 12) {
      report.notes.push(`first u32: ${buf.readUInt32LE(0)}`);
      report.notes.push(`second u32: ${buf.readUInt32LE(4)}`);
    }
  }
  return report;
}

function format(report) {
  const lines = [];
  lines.push(`file        ${report.path}`);
  lines.push(`size        ${report.bytes.toLocaleString()} bytes`);
  lines.push(`sha256      ${report.sha256}`);
  lines.push(`container   ${report.container}`);
  lines.push(`header      ${report.header.hex}`);
  lines.push(`            ${report.header.ascii}`);
  if (report.zip?.files) {
    lines.push('');
    lines.push(`entries     ${report.zip.files.length}`);
    const width = Math.min(48, Math.max(...report.zip.files.map((f) => f.name.length), 4));
    for (const f of report.zip.files.slice(0, 40)) {
      lines.push(`  ${f.name.padEnd(width)}  ${String(f.uncompSize).padStart(9)} B  method=${f.method}`);
    }
    if (report.zip.files.length > 40) lines.push(`  … ${report.zip.files.length - 40} more`);
  } else if (report.zip?.error) {
    lines.push(`zip error   ${report.zip.error}`);
  }
  if (report.notes.length) {
    lines.push('');
    for (const n of report.notes) lines.push(`note        ${n}`);
  }
  return lines.join('\n');
}

const isMain = process.argv[1] && import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/').split('/').pop());
if (isMain || process.argv[1]?.endsWith('probe.mjs')) {
  const args = process.argv.slice(2);
  const json = args.includes('--json');
  const file = args.find((a) => !a.startsWith('--'));
  if (!file) {
    console.error('usage: node probe.mjs <file.fig> [--json]');
    process.exit(2);
  }
  try {
    const report = probe(file);
    console.log(json ? JSON.stringify(report, null, 2) : format(report));
  } catch (err) {
    console.error('probe failed:', err.message);
    process.exit(1);
  }
}
