# Shell Configuration

## Using direnv (Recommended)

```bash
brew install direnv

# Add to ~/.zshrc or ~/.bashrc
eval "$(direnv hook zsh)"  # or bash, fish, etc.

# In the project directory
direnv allow
```

## Make Target Auto-Completion (zsh)

Add to `~/.zshrc` for tab-completion of Makefile targets with descriptions:

```bash
autoload -Uz compinit
compinit

function _make_targets() {
  local -a targets
  local makefile_cache=".make_targets_cache"

  if [[ -f Makefile ]]; then
    if [[ ! -f $makefile_cache ]] || [[ Makefile -nt $makefile_cache ]]; then
      awk -F':.*?## ' '/^[a-zA-Z0-9_-]+:.*?## / {printf "%s:%s\n", $1, $2}' Makefile > $makefile_cache
    fi
    targets=(${(f)"$(<$makefile_cache)"})
    if [[ -s $makefile_cache ]] && grep -q ':' $makefile_cache 2>/dev/null; then
      _describe 'make targets' targets
    else
      awk -F: '/^[a-zA-Z0-9_-]+:/ {print $1}' Makefile > $makefile_cache
      targets=(${(f)"$(<$makefile_cache)"})
      _describe 'make targets' targets
    fi
  fi
}

compdef _make_targets make
```

The completion cache (`.make_targets_cache`) is gitignored and automatically regenerates when the Makefile changes.
