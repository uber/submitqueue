# Issue history API

The issue-history lookup returns retained immutable occurrences for one queue, task, and unit across requests. An empty unit selects task-wide failures. Events preserve openings (including reopenings), repeat reports, closures, original timestamps, and opening build references. Source requests are resolved from builds; an expired build leaves its request reference empty without hiding the retained occurrence.

Pages use ascending recorded timestamp order with bytewise build ID and event tie-breakers. This is recorded history, not request ingest order or a reconstruction of current issue status. The default page size is 50 and the maximum is 200. An issue-bound opaque continuation excludes the last returned occurrence; one extra row establishes whether another page exists. The MySQL reader scans only the selected issue's primary-key prefix and sorts those rows, with no schema change. Returned page size is bounded, but the number of rows examined grows with that issue's retained history.

Pages are live, not snapshots. Concurrent late writes may precede an existing cursor, so readers restart from the first page to refresh coverage. Empty results mean no retained occurrences, not that an issue never existed. There is no backfill: older lifecycle rows may lack opening references, and repeat reports are available only where they were recorded.
