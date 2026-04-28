We are developing an agent orchestrator, for autonomous work. The architecture should be flexible enough that if I want to add layers or move things around it should not be a big lift. To start the basic architecture I am
  thinking is an orchestrator LLM / agent which oversees work and spawns off workers. It would also handle user requests ("start working on X"). The workers may include planner agents, which would get context on the task
  and state of things, and return to the orchestrator for approval / rejection. Then workers will perform the actual work of development.

  Requirements:
  Flexible, but simple architecture
  Minimalist as possible while still being effective
  Way to plugin to customize functionality and architecture
  Statefulness of the orchestrator and a mechanism for the workers to request info from the orchestrator
  Ability to run workers on other vms, or on the same machine (docker, subprocesses)
  Ability to work with codex, claude code (headless modes)
  User visibility into the state of work
  Steerability by the user
