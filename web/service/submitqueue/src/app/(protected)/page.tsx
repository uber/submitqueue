import { connection } from "next/server";
import { requireAuthorization } from "../../server/request-auth";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function HomePage() {
  await connection();
  await requireAuthorization();
  return <main className="shell"><h1>SubmitQueue</h1><p>Read-only demo host</p></main>;
}
