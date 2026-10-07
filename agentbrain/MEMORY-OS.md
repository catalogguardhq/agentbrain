# AgentBrain v4.1 — Thread-Scoped Memory Operating System

AgentBrain has four memory scopes. Never collapse them together.

1. **Project shared memory** — durable facts, requirements, decisions, lessons and project identity.
   Every thread may read it. It is knowledge, not a personal task queue.
2. **Role memory** — open-ended user-defined role conventions and role-targeted handoffs/tasks. Roles are not limited to a built-in list.
3. **Thread private memory** — this exact conversation's objective, progress, next actions, blockers and history.
   Only this thread owns these directives.
4. **Handoffs** — explicit cross-thread assignments. A different thread's private `next_actions` never become
   this thread's work unless the user explicitly says so or an open handoff/task targets this thread/role.

## Non-negotiable routing rule

A thread MUST NOT execute another thread's private objective or next action merely because it can read it.
Other-thread memory is read-only context. Cross-thread work enters this thread only through:
- the current user's explicit instruction;
- a handoff addressed to this exact thread;
- a handoff/task addressed to this thread's established role; or
- an explicit project-shared task that this thread is instructed to claim.

## Authority order

1. Current explicit user instruction in this thread.
2. Verified workspace/artifact state and test results.
3. This thread's owned task/handoff state.
4. Active project requirements and decisions.
5. Other-thread memory for awareness only.
6. Agent proposals/inferences.

Keep analyzer proposals separate from builder implementation claims. Preserve provenance on shared memories.
Do not store secrets or hidden chain-of-thought.
