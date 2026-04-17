# tinydag Design

tinydag is an embedded-first, Rust-native DAG orchestrator designed to run
inside a larger system rather than be the system. This document captures the
current design thinking. As the project matures, major decisions will move
to a formal RFC process.

---

## Chapters

1. [Vision](design/01-vision.md) -- why tinydag exists
2. [Concepts](design/02-concepts.md) -- the mental model
3. [Pipeline Authoring](design/03-pipeline-authoring.md) -- the user facing model
4. [Operator Model](design/04-operator-model.md) -- how operators work
5. [Observability](design/05-observability.md) -- how to peek inside the box
6. [Internals](design/06-internals.md) -- the guts
7. [Roadmap](design/07-roadmap.md) -- how will we make it happen
7. [Open Questions](design/08-open-questions.md) -- what we need to think more about

---

## Contributing

This project is in early design. The best way to contribute right now is to
open an issue with feedback on the design decisions in any of the chapters
above. As the project matures, major changes will go through a formal RFC
process.
