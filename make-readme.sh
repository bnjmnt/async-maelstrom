#!/usr/bin/env bash
echo "# Async Maelstrom" > README.md
grep '//!' src/lib.rs | sed 's/\/\/\![\ ]*//' >> README.md
