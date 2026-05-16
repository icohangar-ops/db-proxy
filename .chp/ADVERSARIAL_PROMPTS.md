# Adversarial Challenge Templates — db-proxy

## Phase 0: Foundation Challenge
When a new decision enters CHP, the adversary MUST address:
1. Why is the proposed direction wrong? (vulnerability_strike)
2. What is the system not seeing? (invalidation_conditions)
3. What is the false consensus risk?

## Domain-Specific Challenges (Tools / Utilities)
1. What edge cases in the input data could produce misleading tool output?
2. Is the tool's output being used for decisions beyond its intended scope?
3. What happens if the tool's dependencies (APIs, data sources) become unavailable?
4. Are there accessibility or usability gaps that could lead to misinterpretation?
5. What is the maintenance burden and technical debt risk for this tool?

## Round 3: Implementation Drift Check
1. Does the implementation match the locked spec acceptance criteria?
2. Are operational handoffs and owner capacity accounted for?
3. Is evidence quality sufficient for the decision domain?

## Council Spawn Triggers
When confidence <85% on high-stakes decisions:
- Attacker Model 1: Challenge foundational assumptions
- Attacker Model 2: Challenge operational feasibility
- Synthesizer: Resolve contradictions and produce final recommendation
