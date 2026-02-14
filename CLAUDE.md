# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**mqtt5-web-backend** is a server-side MQTT 5.0 pub/sub library for real-time communication over WebSocket/TCP streams. It acts as a custom MQTT broker with built-in session persistence, QoS 0/1 support, topic-based authorization, and an autoloading module system.

## Commands

- **Run**: `npm start` (runs `node src/index.js`)
- **Dev with auto-reload**: `npm run serve` (uses nodemon)
- **Run tests**: `npm test` (runs `mocha test/ --exit --bail`)
- Tests require `.env` with `API_RSA_KEY` and `API_RSA_PUB_KEY` pointing to RSA key files (used for JWT auth in tests). The included `dev.private`/`dev.public` files serve this purpose.

## Architecture

### Core Components (`src/`)

- **`index.js`** - `server` constructor. Creates a broker instance that exposes: `connection()`, `subscription()`, `publication()`, `fetcher()`, `scheduler()`, `autoloadModules()`, `sendUpdateBroadcast()`, `publishExternal()`. Uses `mqemitter` for internal pub/sub and `qlobber` for MQTT wildcard topic matching.

- **`client.js`** - `connection` constructor. Handles a single MQTT 5.0 client connection over a stream. Manages the full client lifecycle: CONNECT/auth, SUBSCRIBE with fetcher loading, PUBLISH with publication routing, PUBACK for QoS 1, UNSUBSCRIBE, PINGREQ/PINGRESP, session restore, and disconnect/cleanup. Prevents duplicate connections for the same clientId via `globalEvents`.

- **`session.js`** - In-memory session persistence layer wrapping `aedes-persistence`. Manages session TTL, subscription storage, offline message queuing (outgoing), retained messages, and online/offline state tracking.

- **`const.js`** - MQTT 5.0 reason code constants (SUCCESS, NOT_AUTHORIZED, PAYLOAD_INVALID, etc.).

- **`utils/packet.js`** - Helper functions for building publication response packets: `failPkt()`, `responsePkt()` (broadcast to all), `prvResponsePkt()` (private to client).

- **`typedefs.js`** - JSDoc type definitions used across the codebase.

### Module System

The broker auto-discovers modules via `autoloadModules()` using globby patterns (default: `modules/*/index.js`, `modules/*.js`). Each module exports a function receiving the server instance, then registers subscriptions, fetchers, and publications. See `example/modules/test.js` for the pattern.

### Key Concepts

- **Subscriptions** define topic access control via `access(params, authMeta)`. Registered with `server.subscription(topicPattern, { access })`. Uses `url-pattern` for route-style matching (`:id` params).
- **Fetchers** provide initial data when a client subscribes to a topic. Registered with `server.fetcher(mqttWildcard, { load })`. Uses qlobber MQTT wildcards (`+`, `#`).
- **Publications** handle client-to-broker publishes. Registered with `server.publication(topicPattern, { access, publish })`. The `publish` callback returns `{ all, client, bridge, puback }` to control message routing.
- **Schedulers** run periodic tasks via `setInterval` that can publish packets.
- **ClientCallbacks** are per-client topic listeners configured at connection time, built from `authMeta`.

### Auth Flow

Authentication uses JWT (RS256) passed as the MQTT password. The `auth_func` option verifies the token and returns `authMeta` (or `false`). An optional `metadata_func` enriches `authMeta` after auth. Only MQTT protocol version 5 is accepted.

### Optional Aedes Bridge

When `aedes_handle` is provided in options, subscriptions marked with `bridge: true` cross-subscribe to an external Aedes broker instance, enabling bridging between this broker and Aedes.
