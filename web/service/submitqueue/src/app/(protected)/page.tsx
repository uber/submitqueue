import { connection } from "next/server";
import { QueueDirectory } from "@submitqueue/web-submitqueue";

import { HOST_QUEUES } from "../../server/config";
import { requireAuthorization } from "../../server/request-auth";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function HomePage() {
  await connection();
  await requireAuthorization();
  return <main className="shell"><QueueDirectory queues={HOST_QUEUES} /></main>;
}
