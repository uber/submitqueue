"use client";

import { useEffect, useRef, useState } from "react";
import { Timestamp } from "./timestamp.js";
import { useWebNavigation } from "./navigation.js";
import type { StovepipePageModel } from "../entity/index.js";
import { Link, Section } from "./components.js";
import { RequestList, StatusDetails, ProjectResults, History } from "./pages.js";

function Page({ model }: { model: StovepipePageModel }) {
  const navigation = useWebNavigation();
  const [refreshing, setRefreshing] = useState(false);
  const [automatic, setAutomatic] = useState(false);
  const [refreshError, setRefreshError] = useState("");
  const inFlight = useRef(false);
  async function refresh() {
    if (inFlight.current) return;
    if (!navigation.refresh) {
      window.location.reload();
      return;
    }
    inFlight.current = true;
    setRefreshing(true);
    setRefreshError("");
    try {
      await navigation.refresh(null);
    } catch {
      setRefreshError("Refresh failed. Showing the last successful result.");
    } finally {
      inFlight.current = false;
      setRefreshing(false);
    }
  }
  const refreshRef = useRef(refresh);
  refreshRef.current = refresh;
  useEffect(() => {
    if (!automatic || !navigation.refresh) return;
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void refreshRef.current();
    }, 10_000);
    return () => window.clearInterval(timer);
  }, [automatic, navigation.refresh]);
  return <div className="sq-app site-frame"><header className="site-header"><Link className="brand" href={model.queueHref}><span className="brand-mark">SP</span>Stovepipe</Link></header>
    <main className="shell">
      <nav aria-label="Queues" className="sq-breadcrumb">{model.queues.map(queue => <Link key={queue.name} href={queue.href} aria-current={queue.name === model.queue ? "page" : undefined}>{queue.name}</Link>)}</nav>
      <div className="page-heading"><div>{model.kind === "request" && <p><Link href={model.queueHref}>← Queue requests</Link></p>}<h1>{model.kind === "queue" ? model.queue : (model.requestId ? `Request ${model.requestId}` : "Change status")}</h1><p>{model.kind === "queue" ? "Requests ordered by acceptance time, newest first." : model.queue}</p></div>
        <div className="sq-refresh"><button disabled={refreshing} onClick={() => void refresh()}>{refreshing ? "Refreshing…" : "Refresh"}</button>{navigation.refresh && <label><input type="checkbox" checked={automatic} onChange={event => setAutomatic(event.currentTarget.checked)} /> Auto refresh every 10s</label>}</div>
      </div>
      {refreshError && <p role="alert">{refreshError}</p>}
      {model.kind === "queue" && model.showLatestLink && <p><Link href={model.latestHref}>Latest page</Link></p>}
      {model.kind === "queue" ? <Section result={model.requests}>{data => <RequestList {...data} />}</Section> : <div className="sq-stacked-sections">
        <Section result={model.status}>{status => <div className="sq-detail-columns">
          <section aria-label="Project status"><h2>Project status</h2><StatusDetails status={status} /></section>
          <section aria-label="Project results"><h2>Project results</h2><ProjectResults status={status} /></section>
        </div>}</Section>
        <section aria-label="History"><h2>History</h2><Section result={model.history}>{events => <History events={events} />}</Section></section>
      </div>}
    </main><footer className="site-footer">Loaded <Timestamp value={model.loadedAtMs} /></footer>
  </div>;
}
export function StovepipeApp({ model }: { model: StovepipePageModel }) {
  return <Page key={model.key} model={model} />;
}
