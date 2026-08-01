# Contributing to Home Energy Tracker

Thank you for your interest in contributing.

## Getting Started

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/your-feature-name`
3. Make your changes
4. Commit using [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat:` new feature
   - `fix:` bug fix
   - `perf:` performance improvement
   - `docs:` documentation only
   - `refactor:` code change without feature/fix
5. Push and open a Pull Request against `main`

## Prerequisites

- Java 21
- Docker & Docker Compose
- Maven (or use `./mvnw` in each service)

## Running Locally

```bash
docker compose up -d          # start infra
cd ingestion-service && ./mvnw spring-boot:run
# repeat for other services
```

## Code Style

- Follow existing package structure: `com.leetjourney.<service_name>`
- Use Lombok where applicable (`@Slf4j`, `@Builder`, `@Data`)
- Keep controllers thin — business logic belongs in services
- New Kafka events go in the shared `kafka/event/` package inside the relevant service

## Reporting Issues

Open a GitHub Issue with:
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or screenshots

## License

By contributing, you agree your contributions will be licensed under the [MIT License](LICENSE).
