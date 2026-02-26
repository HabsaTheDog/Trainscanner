# Purpose of this File

This document outlines common mistakes, established patterns, and points of confusion that AI agents may encounter while working on this project. 

If you ever encounter something surprising, undocumented, or counterintuitive, please alert the developer so this file can be updated. **Do not modify this file yourself unless explicitly instructed to do so.**

## Project Evolution

This codebase is in a state of rapid development and iterative refinement. As an AI agent, you are encouraged to prioritize long-term maintainability and architectural integrity. 

If you identify opportunities for structural improvements, schema refactoring, or tech stack optimizations, please propose them clearly. Do not feel constrained by existing patterns if a more robust, scalable, or efficient solution is available.

## Terminal Commands & Environment Pitfalls

When executing commands in the terminal, be aware of the following recurring environment issues:

*   **Interactive Prompts Hang:** Commands that prompt for user input (like `npx` asking for permission to install a package) will hang indefinitely and block execution. Always strictly use non-interactive flags (e.g., `npx --yes`, `npm install --no-fund --no-audit`, etc.).
*   **Stuck NPM Commands:** Occasionally, `npm` package installations or wrapper commands may hang entirely due to cache locks or registry timeouts in this environment. If a command runs unusually long with no output, cancel it, try clearing the cache (`npm cache clean --force`), or run the underlying tools (e.g., `biome`, `tsc`) directly instead of through NPM scripts.
