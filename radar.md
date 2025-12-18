# RADAR
## Reconnaissance-Driven Architecture

**Version:** 0.4
**Date:** 2025-03-08

RADAR is an engineering framework for reducing uncertainty early, accelerating learning, and designing systems that can be operated, supported, secured, and paid for in the real world.

RADAR is not a delivery methodology.
It is an engineering stance.

---

## Foundational Stance: Simple Is the Number One Feature

Simplicity is the constraint that governs all RADAR decisions.

It is not a phase and not a trade-off.

We prefer:
- fewer components over clever abstractions
- obvious designs over flexible ones
- boring technology over novel stacks
- deletion over addition

Every extra moving part is a future incident, cost, or support burden.

If a system is hard to explain, it is already failing.

---

## The RADAR Principles

### 1. Reconnaissance Over Assumptions

We do not design systems in the abstract.

We deliberately build thin, even ugly, probes across **every critical part of the system** as early as possible:
- infrastructure
- data
- deployment
- observability
- security
- failure modes

If something matters later, it is touched now.

Unproven ideas are liabilities.
Working probes are assets.

---

### 2. Learning Is the Primary Deliverable

Progress is measured in **uncertainty removed**, not stories completed.

We favour:
- building over debating
- experiments over documents
- execution over explanation

Architecture emerges from contact with reality, not from diagrams.

---

### 3. Accelerate Feedback Ruthlessly

Fast feedback beats elegant design.

We design for iteration speed before feature depth:
- automated tests before manual QA
- CI/CD before complex services
- local dev parity before scale
- synthetic data before real data access

If it’s slow to change, it’s already wrong.

---

### 4. Risk Is Paid Early or With Interest Later

Technical hotspots are identified deliberately, not discovered accidentally in production.

For each hotspot we design:
- early probes
- mitigators
- kill switches
- containment boundaries

Failure is expected.
Blast radius is optional.

---

### 5. Operations, Security, and Support Are First-Class

Systems are designed for the people who must run them.

We engage:
- security at design time
- testing during construction
- support during construction

Every system answers:
- Who is paged?
- What do they see?
- What can they safely do at 3am?

If it cannot be supported, it is not finished.

---

### 6. FinOps Is an Architectural Concern

Cost is a design input, not a report.

We prefer:
- scale-to-zero over idle capacity
- predictable cost over theoretical efficiency
- simple services over expensive magic

A system nobody uses should cost almost nothing.

---

### 7. Milestones as Reconnaissance Checkpoints

Milestones are not promises to defend.
They are checkpoints to validate learning.

We use milestones to ask:
- What uncertainty should be gone by now?
- What risks should be retired?
- What assumptions have we invalidated?

Learning faster than planned is success.

---

### 8. Developer Experience Is Productive Capacity

The system includes the development experience.

We optimise for:
- fast local setup
- obvious defaults
- minimal cognitive load
- easy rollback and teardown

If engineers fight the system, everyone loses.

---

### 9. Boring by Default, Exploratory by Design

We default to boring, proven technology.

When we explore novel or unfamiliar tech, we do so explicitly and early:
- to learn its limits
- to validate or reject it quickly
- not to justify adopting it

Reconnaissance exists to discover what should be avoided.

---

### 10. RADAR Is Continuous

RADAR is not a phase.

Reconnaissance never stops.
Risk moves.
Systems evolve.

We keep scanning.

---

## When to Apply RADAR

RADAR is most effective when:
- building greenfield systems
- introducing new platforms or architectures
- operating in high-risk domains (scale, data sensitivity, regulation)
- integrating multiple teams or external dependencies

RADAR may be overkill for:
- trivial changes
- low-risk, well-understood systems
- cosmetic or copy-only work

Apply RADAR proportionally, not dogmatically.

## Why We Do This
RADAR is discipline in service of craft.
It exists to protect engineering time, attention, and judgement.

We clear away complexity, uncertainty, and firefighting so the actual work stays interesting - building systems that solve real problems, doing what engineers do best.

---

## RADAR Project Kickoff Template

This document is completed **before significant build begins**.
Discomfort here prevents incidents later.

---

### 1. Project Snapshot

**Project Name:**
**Owner:**
**Tech Lead:**
**Support Owner:**
**Security Contact:**

**One-sentence purpose:**
> What problem does this solve, for whom, and why now?

If this cannot be stated simply, stop.

---

### 2. Simplicity Check (Mandatory)

- What could we remove and still succeed?
- What are we intentionally *not* building?
- What is the simplest version that delivers learning?
- Where are we most tempted to over-engineer?

If simplicity has no defender, complexity will win.

---

### 3. Reconnaissance Phasing & Definitions

**Definitions**
- **Planned:** Identified but not yet explored
- **Prototyped:** Thin, disposable implementation to remove uncertainty (often called a “spike”)
- **Working:** Production-grade enough to build upon
- **Ignored:** Explicitly out of scope, with justification

**Phase Expectations**

**Must be prototyped before first milestone**
- deployment pipeline
- logging
- authN / authZ approach
- basic data model shape

**Must be prototyped before production**
- alerts
- rollback
- cost guardrails
- failure scenarios

**Must be working before launch**
- everything else deemed in scope

---

### 4. Recon Checklist — Build One of Everything

For each item: Planned / Prototyped / Working / Ignored (with justification)

#### Core System
- Interface or API skeleton
- Data model (even if wrong)
- Persistence layer
- Configuration & secrets handling

#### Platform & Ops
- Logging
- Metrics
- Alerts
- Health checks
- Rollback mechanism

#### Security
- AuthN/AuthZ approach
- Secret management
- Threat model sketch
- Dependency scanning

#### Cost & Scale
- Cost drivers identified
- Scale-to-zero path
- Upper cost guardrails
- Load assumptions stated

---

### 5. Test Strategy (Mandatory)

Testing is a design decision, not an afterthought.

Describe the intended approach for:
- **Unit testing**
- **Integration testing**
- **Contract testing**
- **Failure / chaos testing**

Answer explicitly:
- What is **not** being tested yet?
- Why is that acceptable *for now*?
- What would trigger expanding test coverage?

If this section is vague, risk is being deferred.

---

### 6. External Dependencies & Constraints

- External systems or teams depended on:
- Expected reliability and SLAs:
- Regulatory or compliance constraints:
- Data classification (e.g. PII, market-sensitive, internal):
- Approval or review gates required:

These are first-class risk inputs.

---

### 7. Known Hotspots & Mitigators

| Hotspot | Why it’s risky | Early probe | Mitigator |
|-------|---------------|------------|-----------|
| | | | |

If you cannot name hotspots, you have not looked.

---

### 8. Team Capability & Gaps

- Known skill gaps related to hotspots:
- How gaps will be addressed:
- pairing
- training
- external support
- scope reduction
- Risks explicitly being accepted:

Reconnaissance includes honest self-assessment.

---

### 9. Developer Experience Design

- Time to first successful local run: ___ minutes
- One-command setup? (Y/N)
- One-command teardown? (Y/N)
- Local vs prod parity gaps:
- Debugging story:
- Common failure modes visible locally?

Slow setup guarantees slow delivery.

---

### 10. Support: Day in the Life

**Who is on call?**
**What wakes them up?**
**What do they see first?**

Describe:
- top three likely alerts
- dashboards they will open
- actions they are allowed to take safely
- escalation path

If this section is vague, the system is hostile.

---

### 11. FinOps Reality Check

- Expected steady-state cost:
- Zero-usage cost:
- Primary cost levers:
- Cost alarms defined? (Y/N)
- Who gets notified?

If no one owns cost, cost will own you.

---

### 12. Milestones as Learning Gates

| Milestone | Target Date | Uncertainty to remove by this point |
|---------|-------------|--------------------------------------|
| | | |

Milestones without learning objectives are theatre.

---

### 13. Kill Switches & Exit Strategy

- How do we disable this system safely?
- How do we roll back fast?
- How do we delete it entirely if it fails?

Everything should be deletable. Including this project.

---

### 14. RADAR Sign-Off

This is shared understanding, not approval theatre.

- Tech Lead:
- Product Owner:
- Support Owner:
- Security:

Date:

---

## Final Note

Skipping reconnaissance does not save time.
It merely defers payment with interest.

Simple systems fail less, cost less, and move faster.

That is the point.