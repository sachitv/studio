# Technology Stack

## Core Technologies

- **Language**: TypeScript 5.3.3
- **Frontend Framework**: React 18.3.1 with JSX
- **Build System**: Webpack 5.101.0
- **Package Manager**: Yarn 3.6.3 (via Corepack)
- **Desktop Framework**: Electron 37.2.6
- **Testing**: Jest 30.0.5 with Playwright for E2E

## Key Libraries & Frameworks

- **UI Components**: Material-UI (@mui/material 5.13.5)
- **Styling**: TSS-React 4.8.6 (preferred over @emotion/styled for performance)
- **State Management**: Zustand 4.5.7, Reselect 5.1.1
- **3D Graphics**: Three.js (patched version 0.156.1)
- **Charts**: Chart.js 4.4.8, Recharts 2.15.3
- **Data Processing**: Various robotics libraries (@lichtblick/ros\*, @mcap/core)
- **Code Editor**: Monaco Editor 0.52.2

## Development Tools

- **Linting**: ESLint 8.57 with custom @lichtblick rules
- **Formatting**: Prettier 3.3.2
- **Type Checking**: TypeScript with strict configuration
- **Storybook**: 9.1.1 for component development

## Common Commands

### Development

```bash
# Install dependencies
yarn install

# Web development
yarn web:serve              # Start web dev server (http://localhost:8080)
yarn web:build:dev          # Development build
yarn web:build:prod         # Production build

# Desktop development
yarn desktop:serve          # Start webpack dev server
yarn desktop:start          # Launch Electron app
yarn desktop:build:dev      # Development build
yarn desktop:build:prod     # Production build

# Package desktop apps
yarn package:win            # Windows package
yarn package:darwin         # macOS package
yarn package:linux          # Linux package
```

### Testing & Quality

```bash
yarn test                   # Run Jest tests
yarn test:coverage          # Run tests with coverage
yarn test:watch             # Watch mode
yarn test:e2e:web           # Web E2E tests
yarn test:e2e:desktop       # Desktop E2E tests

yarn lint                   # Run ESLint with auto-fix
yarn lint:ci                # CI linting (no auto-fix)
yarn license-check          # Check license compliance
```

### Utilities

```bash
yarn clean                  # Clean build artifacts
yarn storybook              # Start Storybook dev server
yarn build:packages         # Build all TypeScript packages
```

## Build Configuration

- **Webpack**: Separate configs for web, desktop, and benchmark
- **TypeScript**: Monorepo setup with project references
- **Babel**: Used for JSX transformation and preset configurations
- **ESBuild**: Used via esbuild-loader for faster builds

## Performance Guidelines

- Avoid `sx` prop from Material-UI (use tss-react instead)
- Avoid `@emotion/styled` and `@mui/Box` (performance implications)
- Prefer `undefined` over `null` (project convention)
- Use workspace dependencies with `workspace:*` pattern
