"use client";

import { Timestamp } from "@submitqueue/web-platform";
import type { ProjectStatusModel } from "../entity/index.js";
import { Link, Table } from "./components.js";

function Result({ degree }: { degree: number | null }) {
  return (
    <span className="sq-status" data-tone={
      degree === null ? "neutral" : degree === 0 ? "success" : "danger"
    }>
      {degree === null ? "No result recorded" : (
        <>{degree === 0 ? "Green" : "Breakage"} ({
          (degree * 100).toLocaleString("en-US", { maximumSignificantDigits: 6 })
        }%)</>
      )}
    </span>
  );
}

export function ProjectResults({ model }: { model: ProjectStatusModel }) {
  return (
    <>
      <p className="sq-secondary">
        Breakage ranges from 0% (green) to 100%; missing results are not green.
      </p>
      <Table label="Project results" headers={["Scope", "Recorded result"]}>
        <tr>
          <th scope="row">Repository</th>
          <td><Result degree={model.repositoryBreakageDegree} /></td>
        </tr>
        {model.projects.map((entry) => (
          <tr key={entry.project}>
            <td className="sq-project-name">{entry.project}</td>
            <td><Result degree={entry.breakageDegree} /></td>
          </tr>
        ))}
      </Table>
      {!model.projects.length && <p>No project results recorded on this page.</p>}
      <p className="sq-secondary">{model.complete
        ? "Project result recording is complete."
        : "Project result recording is still in progress; more results may arrive."}</p>
      {(model.firstHref || model.nextHref) && (
        <nav aria-label="Project pages" className="page-actions">
          {model.firstHref && <Link href={model.firstHref}>First projects</Link>}
          {model.nextHref && <Link href={model.nextHref}>Next projects →</Link>}
        </nav>
      )}
      <p className="sq-secondary">Results updated: {model.updatedAtMs === "0"
        ? "Unknown" : <Timestamp value={model.updatedAtMs} />}</p>
    </>
  );
}
