Start with `docs/index.md` for a human overview or `docs/SKILL.md` for the
agent-focused index, then follow the referenced docs as needed.

**Debugging metrics/UI issues?** See `docs/metrics-streaming-debug.md` for the
complete phase transition flow (PREPARING → WARMUP → MEASUREMENT) and why QPS
may show 0 during certain phases.

## Agent Behavior: Mandatory Subagent Usage

**CRITICAL: Context preservation is a high priority for this project.**

### Subagent Usage Requirements

**ALWAYS use subagents for:**
1. **Codebase exploration** - Any search, analysis, or investigation of existing code
2. **File discovery** - Finding patterns, implementations, or references across the codebase
3. **Architecture analysis** - Understanding system design, data flow, or component relationships
4. **Long research tasks** - Any investigation requiring reading 3+ files
5. **Documentation exploration** - Searching through docs/ for information

**Use the Explore subagent proactively:**
- When asked "how does X work?"
- When asked "where is Y implemented?"
- When asked "find all instances of Z"
- Before answering questions about code structure or patterns
- When investigating bugs or unexpected behavior

**Use the /investigate custom subagent for:**
- Project-specific code analysis requiring domain knowledge
- Multi-step investigations combining code + docs
- Summarizing implementation patterns specific to this project

EXCEPTION: If subagents are not available (ie in Cursor), notify user they are not available but do not prompt to continue without them, just proceed.

### Context Management Goals

- Main conversation should focus on decisions, direction, and implementation
- Exploration work should happen in isolated subagent contexts
- Subagent responses should be concise summaries (≤500 tokens)
- Never pollute main context with raw search results or verbose file exploration

### How to Invoke

**Automatic (preferred):** Simply ask questions; agent should delegate automatically

**Explicit (if automatic fails):**
- `/explore [task]` - Use built-in Explore subagent
- `/investigate [task]` - Use custom investigation subagent
- Mention "use subagent" or "delegate to subagent" in requests