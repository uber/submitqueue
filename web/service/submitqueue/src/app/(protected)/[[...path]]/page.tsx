import { createNextPage } from "../../../next/page";
import { resolveReferenceWeb } from "../../../server/host";
import { requireAuthorization } from "../../../next/authorize";
import { resolveReferenceLogCallback } from "../../../server/observability";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const page = createNextPage({
  web: resolveReferenceWeb,
  authenticate: requireAuthorization,
  onLog: resolveReferenceLogCallback(),
});

export const generateMetadata = page.generateMetadata;
export default page.Page;
