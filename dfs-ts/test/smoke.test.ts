import { expect, test } from "bun:test";

import type { NodeAddress, NodeConfig } from "../src/index.ts";

const dns: NodeAddress = { ip: "192.168.0.133", port: 7889 };

test("public types describe a joinable node", () => {
  const config: NodeConfig = { role: "data", listen: { ip: "192.168.0.10", port: 9000 }, dns };

  expect(config.role).toBe("data");
  expect(config.dns).toEqual({ ip: "192.168.0.133", port: 7889 });
});
