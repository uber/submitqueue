import type { RequestSummaryModel } from "./request.js";

/** One request that submitted a version of a change. */
export interface ChangeSubmissionModel {
  /** The submitting request. */
  request: RequestSummaryModel;
  /** Submitted version, such as a commit SHA or a revision number. */
  version: string;
  /** In-app link to the submissions of exactly this version. */
  versionHref: string;
}

/** Submissions of one change (a review or a Git ref) to one queue. */
export interface ChangeDetailModel {
  /** Queue whose requests were searched. */
  queue: string;
  /** Code-review provider name, e.g. `GitHub`. */
  provider: string;
  /** Provider host, lowercase, with an optional port. */
  host: string;
  /** Review label, e.g. `PR #123`, `D123`, or a Git ref. */
  review: string;
  /** Repository path; empty for providers without one. */
  repository: string;
  /** The single version shown, or `null` when all versions are shown. */
  pinnedVersion: string | null;
  /** In-app link to all versions of the change, when one exists. */
  logicalHref?: string;
  /** Matching submissions, newest received first; repeated submissions stay separate. */
  submissions: ChangeSubmissionModel[];
  /** Receipt window that was scanned, or `null` when lookup was exact. */
  window: { fromMs: number; toMs: number } | null;
  /** Links to continue or restart a bounded scan; absent when the scan was not paged. */
  pagination?: { nextHref: string | null; latestHref: string | null };
}
