---------------------------- MODULE LandOutcome ----------------------------
(***************************************************************************)
(* One batch's trip from `landing` to a terminal state, across the         *)
(* orchestrator and Runway.                                                *)
(*                                                                         *)
(* Modelled: the orchestrator `land` stage, which publishes a MergeRequest *)
(* to Runway as its last step; Runway's merge controller, whose git merger *)
(* is idempotent on redelivery; Runway's DLQ reconciler; `landsignal`,     *)
(* which applies a verdict unless the batch is already terminal; and the   *)
(* orchestrator DLQ reconcilers for `land` and `landsignal`. Delivery is   *)
(* at-least-once. A stage may fail after its side effect already happened  *)
(* (a lost ack, an ambiguous publish), and a message that keeps failing is *)
(* dead-lettered. Faults are bounded by MaxFaults so liveness can be       *)
(* checked.                                                                *)
(*                                                                         *)
(* Abstracted away: speculation (DecideLand is the whole of it), other     *)
(* batches, request fan-out (a request's outcome is its batch's), retry    *)
(* counts below the DLQ threshold, and queue GC (dedup is modelled at its  *)
(* worst case: a prior answer row is never collected).                     *)
(*                                                                         *)
(* The constants are the design choices under test:                        *)
(*                                                                         *)
(*   DLQPolicy - what the orchestrator's land/landsignal DLQ does          *)
(*     "FailAlways"  : failBatch today: any non-terminal batch -> failed   *)
(*     "SkipLanding" : leave a landing batch alone, wait for Runway        *)
(*     "ResendMerge" : re-send the MergeRequest, so Runway re-answers      *)
(*                                                                         *)
(*   RunwayDLQ - what Runway's DLQ answers                                 *)
(*     "AnswerFailed" : today: always FAILED                               *)
(*     "CheckBranch"  : MERGED if the change is on the target, else FAILED *)
(*                                                                         *)
(*   RunwayRemembers - TRUE: Runway records its verdict per request ID and *)
(*     replays it instead of acting again. FALSE: stateless, as today.     *)
(*                                                                         *)
(*   AnswerDedup - TRUE: an answer's message ID is the request ID alone,   *)
(*     as today, so a second answer from the primary controller is        *)
(*     dropped by the queue. FALSE: every answer is delivered.             *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS DLQPolicy, RunwayDLQ, RunwayRemembers, AnswerDedup, MaxFaults

ASSUME DLQPolicy \in {"FailAlways", "SkipLanding", "ResendMerge"}
ASSUME RunwayDLQ \in {"AnswerFailed", "CheckBranch"}
ASSUME RunwayRemembers \in BOOLEAN
ASSUME AnswerDedup \in BOOLEAN
ASSUME MaxFaults \in Nat

VARIABLES
    batch,        \* orchestrator batch state
    landMsg,      \* batch ID on submitqueue-land: none | queued | dlq | acked
    runwayInbox,  \* a MergeRequest is queued for Runway's merge controller
    runwayDLQ,    \* a MergeRequest is queued for Runway's DLQ reconciler
    repo,         \* target branch: unmerged | merged | rejected
    verdict,      \* Runway's recorded verdict, if RunwayRemembers: none | merged | rejected
    answers,      \* MergeResults queued for landsignal
    answersDLQ,   \* MergeResults dead-lettered by landsignal
    answered,     \* answer message IDs the queue has seen (for dedup)
    faults        \* faults injected so far

vars == <<batch, landMsg, runwayInbox, runwayDLQ, repo, verdict,
          answers, answersDLQ, answered, faults>>

Terminal == {"succeeded", "failed"}
AnswerIDs == {"primary", "dlq"}
Answers == [id : AnswerIDs, verdict : {"merged", "rejected"}]

TypeOK ==
    /\ batch \in {"speculating", "landing"} \cup Terminal
    /\ landMsg \in {"none", "queued", "dlq", "acked"}
    /\ runwayInbox \in BOOLEAN
    /\ runwayDLQ \in BOOLEAN
    /\ repo \in {"unmerged", "merged", "rejected"}
    /\ verdict \in {"none", "merged", "rejected"}
    /\ answers \subseteq Answers
    /\ answersDLQ \subseteq Answers
    /\ answered \subseteq AnswerIDs
    /\ faults \in 0..MaxFaults

Init ==
    /\ batch = "speculating"
    /\ landMsg = "none"
    /\ runwayInbox = FALSE
    /\ runwayDLQ = FALSE
    /\ repo = "unmerged"
    /\ verdict = "none"
    /\ answers = {}
    /\ answersDLQ = {}
    /\ answered = {}
    /\ faults = 0

---------------------------------------------------------------------------
(* speculate *)

DecideLand ==
    /\ batch = "speculating"
    /\ batch' = "landing"
    /\ landMsg' = "queued"
    /\ UNCHANGED <<verdict, runwayInbox, runwayDLQ, repo, answers, answersDLQ, answered, faults>>

(* land *)

LandPublishes ==
    /\ landMsg = "queued"
    /\ runwayInbox' = TRUE
    /\ landMsg' = "acked"
    /\ UNCHANGED <<verdict, batch, runwayDLQ, repo, answers, answersDLQ, answered, faults>>

\* Retries exhausted. The Runway publish is land's last step, so an earlier
\* attempt may already have delivered it.
LandDeadLetters ==
    /\ landMsg = "queued"
    /\ faults < MaxFaults
    /\ landMsg' = "dlq"
    /\ runwayInbox' \in {runwayInbox, TRUE}
    /\ faults' = faults + 1
    /\ UNCHANGED <<verdict, batch, runwayDLQ, repo, answers, answersDLQ, answered>>

(* Runway *)

PublishAnswer(id, v) ==
    IF AnswerDedup /\ id \in answered
      THEN UNCHANGED <<answers, answered>>
      ELSE /\ answers' = answers \cup {[id |-> id, verdict |-> v]}
           /\ answered' = answered \cup {id}

\* Already-applied changes produce no commits, so a repeat replays the verdict.
MergeOutcome == IF repo = "unmerged" THEN {"merged", "rejected"} ELSE {repo}

Recorded == RunwayRemembers /\ verdict # "none"

Record(v) == verdict' = IF RunwayRemembers THEN v ELSE verdict

RunwayMerges ==
    /\ runwayInbox
    /\ runwayInbox' = FALSE
    /\ IF Recorded
         THEN /\ PublishAnswer("primary", verdict)
              /\ UNCHANGED <<repo, verdict>>
         ELSE \E r \in MergeOutcome :
                /\ repo' = r
                /\ Record(r)
                /\ PublishAnswer("primary", r)
    /\ UNCHANGED <<batch, landMsg, runwayDLQ, answersDLQ, faults>>

\* Retries exhausted, possibly after the push landed but before the verdict
\* was recorded or the answer published.
RunwayDeadLetters ==
    /\ runwayInbox
    /\ ~Recorded
    /\ faults < MaxFaults
    /\ runwayInbox' = FALSE
    /\ runwayDLQ' = TRUE
    /\ repo' \in IF repo = "unmerged" THEN {"unmerged", "merged"} ELSE {repo}
    /\ faults' = faults + 1
    /\ UNCHANGED <<batch, landMsg, verdict, answers, answersDLQ, answered>>

DLQVerdict ==
    IF Recorded THEN verdict
    ELSE IF RunwayDLQ = "CheckBranch" /\ repo = "merged" THEN "merged"
    ELSE "rejected"

RunwayReconcilesDLQ ==
    /\ runwayDLQ
    /\ runwayDLQ' = FALSE
    /\ Record(DLQVerdict)
    /\ PublishAnswer("dlq", DLQVerdict)
    /\ UNCHANGED <<batch, landMsg, runwayInbox, repo, answersDLQ, faults>>

(* landsignal *)

LandsignalApplies(a) ==
    /\ answers' = answers \ {a}
    /\ batch' = IF batch \in Terminal THEN batch
                ELSE IF a.verdict = "merged" THEN "succeeded" ELSE "failed"
    /\ UNCHANGED <<verdict, landMsg, runwayInbox, runwayDLQ, repo, answersDLQ, answered, faults>>

\* e.g. the batch CAS keeps failing on a storage fault.
LandsignalDeadLetters(a) ==
    /\ faults < MaxFaults
    /\ answers' = answers \ {a}
    /\ answersDLQ' = answersDLQ \cup {a}
    /\ faults' = faults + 1
    /\ UNCHANGED <<verdict, batch, landMsg, runwayInbox, runwayDLQ, repo, answered>>

(* Orchestrator DLQ reconcilers: the step both share. *)

FailUnlessTerminal == IF batch \in Terminal THEN batch ELSE "failed"

ReconcileBatch ==
    CASE DLQPolicy = "FailAlways" ->
            /\ batch' = FailUnlessTerminal
            /\ UNCHANGED runwayInbox
      [] DLQPolicy = "SkipLanding" ->
            /\ batch' = IF batch = "landing" THEN batch ELSE FailUnlessTerminal
            /\ UNCHANGED runwayInbox
      [] DLQPolicy = "ResendMerge" ->
            IF batch = "landing"
              THEN /\ runwayInbox' = TRUE
                   /\ UNCHANGED batch
              ELSE /\ batch' = FailUnlessTerminal
                   /\ UNCHANGED runwayInbox

ReconcileLandDLQ ==
    /\ landMsg = "dlq"
    /\ landMsg' = "acked"
    /\ ReconcileBatch
    /\ UNCHANGED <<verdict, runwayDLQ, repo, answers, answersDLQ, answered, faults>>

ReconcileSignalDLQ(a) ==
    /\ answersDLQ' = answersDLQ \ {a}
    /\ ReconcileBatch
    /\ UNCHANGED <<verdict, landMsg, runwayDLQ, repo, answers, answered, faults>>

---------------------------------------------------------------------------

Next ==
    \/ DecideLand
    \/ LandPublishes
    \/ LandDeadLetters
    \/ RunwayMerges
    \/ RunwayDeadLetters
    \/ RunwayReconcilesDLQ
    \/ \E a \in answers : LandsignalApplies(a) \/ LandsignalDeadLetters(a)
    \/ ReconcileLandDLQ
    \/ \E a \in answersDLQ : ReconcileSignalDLQ(a)

\* Faults are never forced; every recovery step eventually runs while it
\* stays possible.
Fairness ==
    /\ WF_vars(LandPublishes)
    /\ WF_vars(RunwayMerges)
    /\ WF_vars(RunwayReconcilesDLQ)
    /\ WF_vars(\E a \in answers : LandsignalApplies(a))
    /\ WF_vars(ReconcileLandDLQ)
    /\ WF_vars(\E a \in answersDLQ : ReconcileSignalDLQ(a))

Spec == Init /\ [][Next]_vars /\ Fairness

---------------------------------------------------------------------------
(* Properties *)

\* What SubmitQueue reports never contradicts the branch.
FailedMeansNotMerged == batch = "failed" => repo # "merged"
SucceededMeansMerged == batch = "succeeded" => repo = "merged"

\* A terminal batch keeps its outcome.
TerminalIsFinal == [][(batch \in Terminal) => (batch' = batch)]_vars

\* A batch that starts landing eventually reaches a terminal state.
LandingSettles == (batch = "landing") ~> (batch \in Terminal)

=============================================================================
