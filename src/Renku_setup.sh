#!/usr/bin/env bash
# ================================================================
# Install Claude Code in a Renku session.
#
# Assumes the conda env from environment.yml (incl. nodejs) is
# already active, as it is by default in Renku sessions.
#
# Usage:
#   bash Renku_setup.sh
#   claude
# ================================================================

set -euo pipefail

if command -v claude >/dev/null 2>&1; then
    echo "Claude Code already installed: $(claude --version)"
    exit 0
fi

# npm's global prefix is the active conda env, so `claude` lands in $CONDA_PREFIX/bin.
echo "Installing Claude Code..."
npm install -g @anthropic-ai/claude-code

echo "Done. Run: claude"
