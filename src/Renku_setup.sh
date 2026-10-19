#!/usr/bin/env bash
# ================================================================
# Install Claude Code in a Renku session.
#
# Assumes the conda env from environment.yml (incl. nodejs) is
# already active, as it is by default in Renku sessions.
#
# Usage:
#   bash ~/work/forhyx/src/Renku_setup.sh
#   source ~/.bashrc   # or open a new terminal
#   claude
# ================================================================

set -euo pipefail

# The conda env is read-only, so install into a user-writable prefix in the
# persistent work directory.
export NPM_CONFIG_PREFIX="/home/renku/work/.npm-global"
export PATH="$NPM_CONFIG_PREFIX/bin:$PATH"

# A script can't change its parent shell's PATH, so persist it for new shells.
LINE="export PATH=\"$NPM_CONFIG_PREFIX/bin:\$PATH\""
grep -qxF "$LINE" ~/.bashrc 2>/dev/null || echo "$LINE" >> ~/.bashrc

if command -v claude >/dev/null 2>&1; then
    echo "Claude Code already installed: $(claude --version)"
    exit 0
fi

echo "Installing Claude Code..."
mkdir -p "$NPM_CONFIG_PREFIX"
npm install -g @anthropic-ai/claude-code

echo "Done. Run: source ~/.bashrc  (or open a new terminal), then: claude"
