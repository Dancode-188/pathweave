# Contributing

Thanks for wanting to help. Here's what you need to know before you start.

## Open an issue first

Even for small changes. It takes five minutes and saves everyone time -- if something is
already being worked on, or if you're approaching a problem from an angle we've already
considered and ruled out, better to know that before writing code. Just open an issue,
describe what you want to do and why, and we'll go from there.

## Setup

You need a recent stable Rust toolchain. The `rust-toolchain.toml` in the root pins the
version so `rustup` will handle it automatically.

```bash
git clone https://github.com/dancode-188/pathweave
cd pathweave
cargo build
```

## Before you submit

Your changes need to pass all of these:

```bash
cargo test                        # all tests pass
cargo clippy -- -D warnings       # no warnings
cargo fmt --all --check           # formatting is consistent
cargo audit                       # no known vulnerabilities in deps
```

If `cargo audit` isn't installed: `cargo install cargo-audit`.

CI runs all of these on every pull request. If it fails in CI, it won't be merged.

## Submitting a pull request

Branch off `main`. Keep changes focused -- one thing per PR. The description should
explain what changed and why, not just what the diff says. If there's a tradeoff you made
or something you considered and decided against, that belongs in the description.

PRs that add or change security-relevant code (crypto, transport handling, key management)
need extra context about the security reasoning. Not a lot -- just enough that someone
reviewing can understand the decision.

## Code style

No comments that describe what the code does -- the code does that. Comments explain why:
a non-obvious constraint, a workaround for a specific issue, something that would surprise
a reader. If a comment could be removed without confusing anyone, remove it.

Error types use `thiserror`, not `anyhow`. No `unwrap()` in library code. If something
shouldn't panic, return a `Result`.

## Security issues

Don't open a public issue for vulnerabilities. See [SECURITY.md](SECURITY.md).

## Questions

Open a discussion on GitHub. Or an issue if it's specific enough to be one.
