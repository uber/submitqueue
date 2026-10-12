import { createClient } from "@connectrpc/connect";
import { Stovepipe } from "@submitqueue/api/stovepipe";
import { createNextHealthRoute } from "@submitqueue/web-service-shared/health";
import { loadConfiguration } from "../../server/config";
import { loadAuthConfiguration } from "../../server/auth";
import { newConnectTransport } from "@submitqueue/web-service-shared/transport";
import { onLog } from "../../server/host";

export const dynamic = "force-dynamic";

export const GET = createNextHealthRoute(
  {
    configuration: () => {
      loadAuthConfiguration();
      loadConfiguration();
    },
    stovepipe: async () => {
      const client = createClient(
        Stovepipe,
        newConnectTransport({
          ...loadConfiguration(),
          defaultTimeoutMs: 1_000,
        }),
      );
      await client.ping({ message: "stovepipe-web-readiness" });
    },
  },
  onLog,
);
