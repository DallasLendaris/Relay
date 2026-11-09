# Spanish summaries: native-speaker review still required

The Spanish text in `backend/src/summaries/templates.ts` and the Spanish push wording in
`backend/src/delivery/send-jobs.ts` were written by us, not by a professional translator or a
native speaker. A native-speaker review (ideally someone who works in emergency communications,
and a look at NWS Spanish products and Cal OES Spanish guidance) is still required before the
text is used for real alerts.

Corrections already applied (audit SAFE-9, SAFE-10):

| Before               | After                       | Why                                           |
| -------------------- | --------------------------- | --------------------------------------------- |
| Declaración          | Comunicado                  | Tier word for statements.                     |
| inundación repentina | inundaciones repentinas     | NWS Spanish uses the plural for flash floods. |
| Fíjese cómo están    | Revise cómo están           | More natural, and not a regional idiom.       |
| 12 a. m. / 12 p. m.  | la medianoche / el mediodía | Avoids the ambiguity of 12 a. m. and 12 p. m. |
| (no zone)            | "PT" after the time         | The time zone is always stated.               |

Things the reviewer should look at in particular:

- "aviso / vigilancia / advertencia" for Warning / Watch / Advisory, and the Spanish names for
  the new events (tsunami, extreme wind, dust storm, shelter in place, beach hazards).
- "PT" versus "hora del Pacífico", and the "a partir de" / "Habrá" wording used when an alert has
  not started yet.
- The push body for Spanish: `Resumen de Relay (no oficial): ... Fuente: NWS`, and the fallback
  `Alerta oficial del NWS (solo en inglés): ...` used when there is no Spanish summary.
- Reading level (we aim for 3rd to 4th grade) and the use of "usted".

Until the review is done, treat the Spanish summaries as a draft. The official English text is
always the source of truth, and the push says so.
