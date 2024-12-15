# Orca is a orchestrator built with Manager-Worker architecture pattern.

	The purpose is to orchestrate docker containers (aka Tasks), assigned with Manager,
	implementing various scheduler algorithms to choose suitable worker.
	Stack:
	- Golang
	- Docker SDK API for managing containers
	- BoltDB for persistent storage
	- Cobra cli framework
	- Chi library for routing

Project built according to Tim Boring book "Build an Orchestrator in Go from scratch"