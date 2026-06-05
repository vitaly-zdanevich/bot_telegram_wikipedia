#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_ROOT="$(CDPATH='' cd -- "$SCRIPT_DIR/.." && pwd)"
TFVARS_FILE="${TFVARS_FILE:-$PROJECT_ROOT/infra/terraform.tfvars}"

usage() {
  printf '%s\n' \
    "Usage:" \
    "  scripts/favorite-category.sh list" \
    "  scripts/favorite-category.sh add <language> <category title>" \
    "  scripts/favorite-category.sh add <language>:<category title>" \
    "  scripts/favorite-category.sh remove <language> <category title>" \
    "  scripts/favorite-category.sh remove <language>:<category title>" \
    "" \
    "Examples:" \
    "  scripts/favorite-category.sh add en Physics" \
    "  scripts/favorite-category.sh add en Category:Physics" \
    "  scripts/favorite-category.sh remove en Physics" \
    "" \
    "Set TFVARS_FILE=/path/to/terraform.tfvars to edit another file."
}

die() {
  echo "favorite-category.sh: $*" >&2
  exit 1
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

favorite_value() {
  if [[ ! -f "$TFVARS_FILE" ]]; then
    printf ''
    return
  fi

  local line
  line="$(grep -E '^[[:space:]]*favorite_categories[[:space:]]*=' "$TFVARS_FILE" | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    printf ''
    return
  fi

  if [[ "$line" =~ ^[[:space:]]*favorite_categories[[:space:]]*=[[:space:]]*\"(.*)\"[[:space:]]*$ ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return
  fi

  die "favorite_categories must be a single quoted string in $TFVARS_FILE"
}

entry_from_args() {
  if [[ $# -eq 1 ]]; then
    trim "$1"
    return
  fi

  if [[ $# -ge 2 ]]; then
    local language="$1"
    shift
    local title="$*"
    language="$(trim "$language")"
    title="$(trim "$title")"
    [[ -n "$language" ]] || die "language is empty"
    [[ -n "$title" ]] || die "category title is empty"
    printf '%s:%s' "$language" "$title"
    return
  fi

  die "category is required"
}

read_entries() {
  local value="$1"
  value="${value//,/|}"
  value="${value//$'\n'/|}"

  local entry
  local -a entries
  IFS='|' read -r -a entries <<< "$value"
  for entry in "${entries[@]}"; do
    entry="$(trim "$entry")"
    [[ -n "$entry" ]] && printf '%s\n' "$entry"
  done
}

entry_key() {
  local entry="$1"
  printf '%s' "$entry" | tr '[:upper:]' '[:lower:]'
}

write_value() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  local sed_value="$value"
  sed_value="${sed_value//&/\\&}"
  sed_value="${sed_value//|/\\|}"

  mkdir -p "$(dirname -- "$TFVARS_FILE")"
  if [[ -f "$TFVARS_FILE" ]] && grep -Eq '^[[:space:]]*favorite_categories[[:space:]]*=' "$TFVARS_FILE"; then
    sed -i -E "s|^[[:space:]]*favorite_categories[[:space:]]*=.*$|favorite_categories = \"$sed_value\"|" "$TFVARS_FILE"
  else
    local needs_newline=false
    if [[ -f "$TFVARS_FILE" && -s "$TFVARS_FILE" ]]; then
      needs_newline=true
    fi
    {
      $needs_newline && printf '\n'
      printf 'favorite_categories = "%s"\n' "$value"
    } >> "$TFVARS_FILE"
  fi
}

list_entries() {
  local value
  value="$(favorite_value)"
  if [[ -z "$value" ]]; then
    echo "No favorite categories configured."
    return
  fi
  read_entries "$value"
}

add_entry() {
  local new_entry="$1"
  [[ -n "$new_entry" ]] || die "category is empty"

  local value existing key new_key
  value="$(favorite_value)"
  new_key="$(entry_key "$new_entry")"
  local updated=()
  while IFS= read -r existing; do
    key="$(entry_key "$existing")"
    if [[ "$key" == "$new_key" ]]; then
      echo "Already present: $existing"
      return
    fi
    updated+=("$existing")
  done < <(read_entries "$value")

  updated+=("$new_entry")
  local joined
  joined="$(IFS='|'; echo "${updated[*]}")"
  write_value "$joined"
  echo "Added: $new_entry"
}

remove_entry() {
  local target_entry="$1"
  [[ -n "$target_entry" ]] || die "category is empty"

  local value existing key remove_key removed=false
  value="$(favorite_value)"
  remove_key="$(entry_key "$target_entry")"
  local updated=()
  while IFS= read -r existing; do
    key="$(entry_key "$existing")"
    if [[ "$key" == "$remove_key" ]]; then
      removed=true
      continue
    fi
    updated+=("$existing")
  done < <(read_entries "$value")

  $removed || die "not found: $target_entry"
  local joined
  joined="$(IFS='|'; echo "${updated[*]}")"
  write_value "$joined"
  echo "Removed: $target_entry"
}

command="${1:-}"
case "$command" in
  list)
    [[ $# -eq 1 ]] || die "list does not accept arguments"
    list_entries
    ;;
  add)
    shift
    add_entry "$(entry_from_args "$@")"
    ;;
  remove|rm)
    shift
    remove_entry "$(entry_from_args "$@")"
    ;;
  -h|--help|help|"")
    usage
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
