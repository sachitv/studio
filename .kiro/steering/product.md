# Product Overview

Lichtblick is an integrated visualization and diagnosis tool for robotics, available both as a web application and desktop app (Linux, Windows, macOS).

## Key Features

- Robotics data visualization and analysis
- Real-time data streaming and playback
- Multi-platform support (browser and native desktop)
- Open-source with MPL-2.0 license
- Originally forked from Foxglove Studio

## Target Users

- Robotics engineers and developers
- Data analysts working with robotics data
- Teams needing collaborative robotics debugging tools

## Distribution

- **Web**: Browser-based application at http://localhost:8080
- **Desktop**: Native Electron applications for Windows, macOS, and Linux
- **Docker**: Available as containerized web application
- **Source**: Open-source repository for custom builds

## Architecture

- **Frontend**: React-based UI with TypeScript
- **Desktop**: Electron wrapper around web components
- **Data Processing**: Supports various robotics data formats (ROS, MCAP, etc.)
- **Visualization**: 3D rendering, charts, and real-time data displays
