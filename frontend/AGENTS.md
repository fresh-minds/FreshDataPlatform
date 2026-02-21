# Frontend Agent Instructions

Scope: `frontend/` (React + Vite launchpad).

## Must Do
- Keep changes limited to source/config files (`src/`, `package.json`, `vite.config.js`, Dockerfiles).
- Run and report relevant checks after changes:
  - `npm run lint`
  - `npm run build`
- Do not edit generated/vendor folders (`node_modules/`, `dist/`, `.vite/`).

## Documentation Requirements
- If routes, service links, env usage, or operator workflows change, update:
  - `README.md`
  - `DEVELOPMENT.md`
  - `ARCHITECTURE.md` (if UI/component flows change)

## Change Rules
- Prefer minimal, consistent React patterns already used in `src/`.
- Keep Docker and local Vite behavior aligned when changing runtime URLs or ports.
