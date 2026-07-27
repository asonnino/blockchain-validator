# blockchain-validator

[![build status][ci-badge]][ci-link]
[![rustc](https://img.shields.io/badge/rustc-1.97+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

A validator node composing the [mysticeti](https://github.com/asonnino/mysticeti)
consensus replica with an execution engine and a checkpoint engine. Mysticeti
deliberately scopes out application execution; this repo adds it on top.

[ci-badge]: https://img.shields.io/github/actions/workflow/status/asonnino/blockchain-validator/code.yaml?branch=main&logo=github&style=flat-square
[ci-link]: https://github.com/asonnino/blockchain-validator/actions
