import Link from "next/link";

export default function NotFound() {
  return (
    <main className="shell centered-state">
      <section className="state-card" aria-labelledby="not-found-title">
        <p className="eyebrow">Request lookup</p>
        <h1 id="not-found-title">Request not found</h1>
        <p>The request may not exist in the demo queue or may have expired.</p>
        <Link className="button-link" href="/requests">
          Return to requests
        </Link>
      </section>
    </main>
  );
}
