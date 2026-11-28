import { expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";

const CORE = new URL("../src/core/", import.meta.url);
const IMPORT = /from\s+["']([^"']+)["']/g;

/** Redesign rule: `core/` is sans-IO and never reaches `driver/`, directly or by re-export. */
test("core never imports driver", () => {
  const offenders: string[] = [];
  for (const file of readdirSync(CORE).filter((name) => name.endsWith(".ts"))) {
    const source = readFileSync(new URL(file, CORE), "utf8");
    for (const [, specifier] of source.matchAll(IMPORT)) {
      if (specifier !== undefined && /(^|\/)driver(\/|$)/.test(specifier)) {
        offenders.push(`${file} -> ${specifier}`);
      }
    }
  }
  expect(offenders).toEqual([]);
});
