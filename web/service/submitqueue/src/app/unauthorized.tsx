export default function Unauthorized() {
  return (
    <main className="shell centered-state">
      <section className="state-card" aria-labelledby="unauthorized-title">
        <p className="eyebrow">SubmitQueue</p>
        <h1 id="unauthorized-title">Authentication required</h1>
        <p>Use the demo credentials configured for this host.</p>
      </section>
    </main>
  );
}
