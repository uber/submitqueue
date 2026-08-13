# client

The SubmitQueue client: dialling a gateway, the calls made against it, and the terminal view of what a queue is doing.

It exists so the tools are thin. A binary under `service/` is flag parsing over this package, which is what keeps the gateway CLI and the demo from each growing their own dialling, their own strategy parsing, and their own status table — as they had begun to.

## Connecting

`Options` is a plain struct, not a set of flags: binaries parse their own flags and fill it, so the package stays usable from a test or another program.

`Addr` is handed to the dialler untouched, so the full gRPC target syntax works — a plain `host:port`, but also `dns:///host:port` or `unix:///path.sock`. Transport security is a separate option rather than something encoded in the address, because gRPC keeps target resolution and transport credentials apart; there is no scheme that means "use TLS", and inventing one would only mislead.

`TokenEnv` names the variable holding a bearer token rather than carrying the token, so a credential never reaches a command line. An unset variable is not an error — it is how a client against a gateway that wants no credential runs, which today is every gateway in this repository. The token is for one reached through something that does check it: a proxy, a sidecar, an ingress terminating auth ahead of the service.

## The view

A `Row` is one land request and everything shown about it. A `Tracker` owns a set of rows, polls their histories, and redraws as they move; `Draw` renders once, for a listing that is not following anything.

Two things move a run forward at once — whatever is producing requests, and the poll reading their statuses — and both draw the same table, so the tracker's mutex is what keeps one from redrawing halfway through the other's update. Reads happen outside that lock: holding it across a round of RPCs would stall the producer behind the network, and letting the producer run ahead is the whole point of watching each request the moment it exists.

The renderer draws in place on a terminal and appends a fresh block when piped, so redirecting to a file gives something readable rather than escape codes. Piped output skips a redraw when nothing in the table moved, which is why the signature it compares leaves out the elapsed clock: that advances every second, and a log reprinting the table for it alone would say nothing while saying it often.

Column widths only ever grow, so a value wider than its header does not make the table jitter as rows fill in.

### The changes column

Each row carries `[]Cell`, and a cell is text with an optional URL. The column is supplied by the caller rather than derived, because what identifies a change depends on who is watching: a tool that just opened the pull requests knows their numbers and can link to them, while a client watching a queue it did not create knows only the change URIs the gateway reports. `RowsFromSummaries` builds the second kind from a listing.

On a terminal a cell with a URL is rendered as an OSC 8 hyperlink; piped, the address itself is printed, since a log has nothing to click and the address is the part worth copying. Padding counts on-screen width rather than string length — a hyperlink is mostly escape bytes that occupy no columns, and padding by length would shove the table sideways.

## Settling

`Tracker.Seal` declares that nothing further will join the set, which is what lets an otherwise-finished run conclude; without it a poll finding nothing outstanding before the first request existed would call the run finished. `Conclude` draws the verdict and returns an error naming every request that ended anywhere other than `landed`, so a scripted caller notices.
