# Local PR-Agent advisory review

This opt-in workflow adds a local-model review comment to eligible pull requests in `fizzlepoof/WXDispatch`. It does not approve, merge, deploy or edit PR source code. Existing CI and human review remain authoritative.

## Boundaries

- Runs for same-repository, non-draft PRs on opened, reopened, ready-for-review and synchronize events. Fork PRs are excluded from this initial configuration.
- Fetches only the pinned public upstream tool, never checks out or executes PR code.
- Uses explicit local Qwen with no fallback models. No hosted model is configured.
- Uses stock PR-Agent source `e673a02076ce09a4726c3d0c3cc85780e59ca52b` (upstream v0.50.0; its internal package version is 0.49.0), a frozen lockfile and a per-job Python environment.
- Publishes an advisory comment only: automatic descriptions, code improvements, approvals, reactions and review labels are disabled.
- Does not change existing required checks, branch protections or deployment workflows.

## Operator prerequisites

1. An authorized repository-scoped or explicitly authorized runner with labels `self-hosted`, `linux`, `x64`, `pr-agent-local`.
2. Python 3.11+ at `/usr/bin/python3`, the standard user-local uv executable at `$HOME/.local/bin/uv`, and outbound access to GitHub and dependency registries.
3. A direct reachable private OpenAI-compatible endpoint advertising `vllm-sr/qwen3.8-flash-next-iq3_s`. The current compatibility-key configuration assumes that endpoint accepts the non-secret compatibility key shown in the workflow. Do not publish a private endpoint or real credentials in source.
4. Runner-owned work, temporary environment and cache storage with adequate headroom. Shared-organization runners must not expose personal-agent homes or administrative credentials to jobs. Do not reuse another repository's registered runner merely by copying its labels.

## Activation

After the workflow is reviewed and merged into `main`, configure repository Actions variables:

- `PR_AGENT_LOCAL_API_BASE`: the directly reachable private API base, including `/v1`.
- `PR_AGENT_PILOT_ENABLED`: `true` only after runner and endpoint qualification.

With either setting absent, or the enable flag not exactly `true`, the job does not run. An Actions admin must arrange runner access; source-write or maintain access alone does not establish that authority. Verify one disposable unmerged canary with an actual local completion and GitHub comment readback. Close the canary without merging it.

## Operation and limits

Guidance focuses on introduced defects, actual callers/defaults, evidence-backed protocol claims and distinct root causes. Thinking and bounded large-PR chunking are enabled. Successful execution, complete reported coverage or an empty findings list does not certify correctness. Hypothetical failure-mode analysis can remain noisy. Review findings against source and tests before acting.

## Disable and rollback

Set/remove `PR_AGENT_PILOT_ENABLED` so it is not `true` to prevent future review jobs; cancel an already-running review separately if needed. Revert the focused workflow/docs change through the normal review path to remove the integration. Runner deregistration/service removal is a separate operator action. No application or database rollback is involved.
