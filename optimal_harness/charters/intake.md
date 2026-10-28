# INTAKE — lifecycle audit, no design work

Read only the specification. The output ROOT must be {"slots": {...}}.
Do not put the nine keys at the root; nest ALL of them inside the "slots" object.
The slots object has exactly these nine keys:
`invocation`, `input_existence`, `input_access`, `input_emptiness`,
`malformed_entries`, `ties`, `aggregation_edges`, `output_shape_order`,
`exit_behavior`. Each slot is either
`{"status":"citation","quote":"exact substring from spec","because":"reason it resolves this slot"}`
or `{"status":"gap","detail":"precisely what is undefined"}`.
No suggestions, extrapolation, or brainstorming. Missing requirements are GAPS.
Write the audit artifact, append evidence, mark the handoff `ready`, and finish.
An unresolved gap halts execution for human/coordinator clarification.

Complete JSON skeleton (replace every placeholder with source evidence; never
copy placeholders as evidence). Use action=write with this object as data:
{"slots":{
  "invocation":{"status":"citation","quote":"<exact spec substring>","because":"<reason>"},
  "input_existence":{"status":"citation","quote":"<exact spec substring>","because":"<reason>"},
  "input_access":{"status":"citation","quote":"<exact spec substring>","because":"<reason>"},
  "input_emptiness":{"status":"citation","quote":"<exact spec substring>","because":"<reason>"},
  "malformed_entries":{"status":"citation","quote":"<exact spec substring>","because":"<reason>"},
  "ties":{"status":"citation","quote":"<exact spec substring>","because":"<reason>"},
  "aggregation_edges":{"status":"citation","quote":"<exact spec substring>","because":"<reason>"},
  "output_shape_order":{"status":"citation","quote":"<exact spec substring>","because":"<reason>"},
  "exit_behavior":{"status":"citation","quote":"<exact spec substring>","because":"<reason>"}
}}
If a slot is undefined, replace its entire value with
{"status":"gap","detail":"<precise missing requirement>"}. Never invent a quote.
Format errors receive deterministic tool feedback; correct the whole document.
