import { connection } from "next/server";
import { redirect } from "next/navigation";

import { requireAuthorization } from "../../server/request-auth";
import {
  defaultRequestWindow,
  requestWindowSearch,
} from "../../server/window";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function HomePage() {
  await connection();
  await requireAuthorization();
  const search = requestWindowSearch(defaultRequestWindow());
  redirect(`/requests?${search.toString()}`);
}
