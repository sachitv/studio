# Project Structure

## Monorepo Organization

This is a Yarn workspaces monorepo with the following top-level structure:

```
├── packages/           # Core library packages
├── desktop/           # Electron desktop application
├── web/              # Web application
├── benchmark/        # Performance benchmarking
├── e2e/             # End-to-end tests
├── ci/              # CI/CD scripts and utilities
├── patches/         # Package patches
└── resources/       # Static resources
```

## Key Packages (`packages/`)

### Core Packages

- **`@lichtblick/suite-base`** - Main application components and logic
- **`@lichtblick/suite`** - Public API and core interfaces
- **`@lichtblick/suite-desktop`** - Desktop-specific components
- **`@lichtblick/suite-web`** - Web-specific components

### Utility Packages

- **`@lichtblick/hooks`** - Shared React hooks
- **`@lichtblick/log`** - Logging utilities
- **`@lichtblick/theme`** - UI theming system
- **`@lichtblick/den`** - Utility functions
- **`@lichtblick/message-path`** - Message path parsing
- **`@lichtblick/mcap-support`** - MCAP file format support
- **`@lichtblick/comlink-transfer-handlers`** - Web worker communication

### Type Definitions

- **`packages/@types/`** - Custom TypeScript type definitions

## Application Structure

### Desktop App (`desktop/`)

```
desktop/
├── main/           # Electron main process
├── preload/        # Electron preload scripts
├── renderer/       # Electron renderer process
└── quicklook/      # macOS QuickLook plugin
```

### Web App (`web/`)

```
web/
├── src/           # Web-specific source code
└── integration-test/  # Web integration tests
```

## Configuration Files

### Build & Development

- **`webpack.config.ts`** - Webpack configurations (per app)
- **`tsconfig.json`** - TypeScript project references
- **`babel.config.json`** - Babel transformation config
- **`jest.config.json`** - Jest testing configuration

### Code Quality

- **`.eslintrc.yaml`** - ESLint rules and configuration
- **`.prettierrc.yaml`** - Prettier formatting rules
- **`.eslintrc.ci.yaml`** - CI-specific linting rules

### Package Management

- **`package.json`** - Root package with workspace definitions
- **`.yarnrc.yml`** - Yarn configuration
- **`yarn.lock`** - Dependency lock file

## Naming Conventions

### Packages

- All internal packages use `@lichtblick/` namespace
- Workspace dependencies use `workspace:*` pattern
- Private packages marked with `"private": true`

### Files & Directories

- **Components**: PascalCase (e.g., `MyComponent.tsx`)
- **Utilities**: camelCase (e.g., `myUtility.ts`)
- **Tests**: `*.test.ts` or `*.test.tsx`
- **Stories**: `*.stories.tsx`
- **Styles**: `*.style.ts` (when using tss-react)

### Import Patterns

- Internal imports use `@lichtblick/` namespace
- Relative imports for same-package files
- Absolute imports for cross-package dependencies

## Development Workflow

### Adding New Features

1. Create components in `packages/suite-base/src/`
2. Add tests alongside components (`*.test.tsx`)
3. Create Storybook stories for UI components (`*.stories.tsx`)
4. Export public APIs through `packages/suite/src/`

### Package Dependencies

- Use `workspace:*` for internal dependencies
- Keep external dependencies in sync across packages
- Use `devDependencies` for build-time tools
- Use `dependencies` for runtime requirements

### Testing Structure

- Unit tests: Co-located with source files
- Integration tests: `web/integration-test/`
- E2E tests: `e2e/tests/`
- Each package has its own `jest.config.json`

## Build Outputs

- **Desktop**: `dist/` directory (platform-specific installers)
- **Web**: `web/.webpack/` directory (static assets)
- **Packages**: Individual `dist/` directories after TypeScript compilation
