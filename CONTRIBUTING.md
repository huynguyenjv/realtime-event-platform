# Contributing to Realtime Event Platform

First off, thank you for considering contributing to this project! 🎉

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Commit Messages](#commit-messages)

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to uphold this code. Please be respectful and constructive in all interactions.

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Set up the development environment
4. Create a new branch for your feature/fix
5. Make your changes
6. Submit a pull request

## Development Setup

### Prerequisites

- Go 1.26+
- Python 3.11+
- Docker & Docker Compose
- Make

### Local Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/realtime-event-platform.git
cd realtime-event-platform

# Add upstream remote
git remote add upstream https://github.com/huynguyenjv/realtime-event-platform.git

# Install dependencies
make deps

# Start infrastructure
make infra-up

# Run tests
make test
```

## How to Contribute

### Reporting Bugs

Before creating bug reports, please check existing issues. When creating a bug report, include:

- A clear and descriptive title
- Steps to reproduce the behavior
- Expected behavior vs actual behavior
- Environment details (OS, Go version, etc.)
- Relevant logs or error messages

### Suggesting Features

Feature requests are welcome! Please provide:

- A clear description of the feature
- The problem it solves
- Possible implementation approach
- Any relevant examples

### Code Contributions

1. **Pick an issue** - Look for issues labeled `good first issue` or `help wanted`
2. **Discuss** - Comment on the issue to let others know you're working on it
3. **Implement** - Write your code following our coding standards
4. **Test** - Ensure all tests pass and add new tests if needed
5. **Document** - Update documentation if required
6. **Submit** - Create a pull request

## Pull Request Process

1. Update the README.md or documentation with details of changes if applicable
2. Ensure all tests pass (`make test`)
3. Run the linter (`make lint`)
4. Update the CHANGELOG.md if applicable
5. The PR will be merged once you have approval from a maintainer

### PR Title Format

```
type(scope): description

Examples:
feat(collector): add batch event ingestion
fix(analytics): resolve memory leak in stream processor
docs(readme): update installation instructions
refactor(kafka): simplify consumer configuration
test(auth): add JWT validation tests
```

## Coding Standards

### Go

- Follow [Effective Go](https://golang.org/doc/effective_go)
- Use `gofmt` for formatting
- Run `golangci-lint` before committing
- Write tests for new functionality
- Keep functions small and focused
- Use meaningful variable names

```go
// Good
func (s *Service) ProcessEvent(ctx context.Context, event *Event) error {
    // ...
}

// Avoid
func (s *Service) PE(c context.Context, e *Event) error {
    // ...
}
```

### Python

- Follow PEP 8
- Use type hints
- Run `black` for formatting
- Run `pylint` or `flake8` for linting

### General

- Write clear, self-documenting code
- Add comments for complex logic
- Keep commits atomic and focused
- Don't commit generated files

## Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks
- `perf`: Performance improvements

### Examples

```
feat(prediction): add LSTM model for time-series forecasting

- Implement LSTM architecture with PyTorch
- Add model training pipeline
- Include evaluation metrics

Closes #123
```

```
fix(websocket): handle connection timeout gracefully

Previously, connections would hang indefinitely when the server
was unresponsive. This change adds a configurable timeout with
proper cleanup.

Fixes #456
```

## Questions?

Feel free to open an issue for any questions or reach out to the maintainers.

Thank you for contributing! 🚀
