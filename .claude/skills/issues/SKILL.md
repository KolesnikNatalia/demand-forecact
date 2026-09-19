---
name: issues
description: Создает github issues и milestones из файла плана. Использую, когда есть готовый план с этапами, и нужно создать бэклог на github.
---

# Issues Generator

Прочитай план из файла: $ARGUMENTS

Для каждого этапа создай milestone и issues в GitHub, используя gh CLI.

## Порядок действий

1. Прочитай файл плана
2. Для каждого этапа создай milestone:
   `gh api repos/:owner/:repo/milestones -f title="Этап N: название"`

3. Для каждой задачи в этапе создай Issue:
   `gh issue create --title "..." --body "..." --label "..." --milestone "..."`

## Формат Issue

**Title**: текст задачи из плана (без[])
**Body**: описани задачи