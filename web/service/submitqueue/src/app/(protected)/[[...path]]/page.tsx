import { createNextPage } from "../../../next/page";
import { resolveReferenceWeb } from "../../../server/host";
import { requireAuthorization } from "../../../next/authorize";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const page = createNextPage({ web: resolveReferenceWeb, authenticate: requireAuthorization });

export const generateMetadata = page.generateMetadata;
export default page.Page;
