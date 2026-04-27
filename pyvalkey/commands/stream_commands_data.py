from typing import Any

import yaml

XAUTOCLAIM_REPLY_SCHEMA: dict[str, Any] = yaml.safe_load("""
anyOf:
- description: Claimed stream entries (with data, if `JUSTID` was not given).
  type: array
  minItems: 3
  maxItems: 3
  items:
  - description: Cursor for next call.
    type: string
    pattern: '[0-9]+-[0-9]+'
  - type: array
    uniqueItems: true
    items:
      type: array
      minItems: 2
      maxItems: 2
      items:
      - description: Entry ID
        type: string
        pattern: '[0-9]+-[0-9]+'
      - description: Data
        type: array
        items:
          type: string
  - description: >-
      Entry IDs which no longer exist in the stream, and were deleted from the PEL in which they were found.
    type: array
    items:
      type: string
      pattern: '[0-9]+-[0-9]+'
- description: Claimed stream entries (without data, if `JUSTID` was given).
  type: array
  minItems: 3
  maxItems: 3
  items:
  - description: Cursor for next call.
    type: string
    pattern: '[0-9]+-[0-9]+'
  - type: array
    uniqueItems: true
    items:
      type: string
      pattern: '[0-9]+-[0-9]+'
  - description: >-
      Entry IDs which no longer exist in the stream, and were deleted from the PEL in which they were found.
    type: array
    items:
      type: string
      pattern: '[0-9]+-[0-9]+'
""")

XINFO_STREAM_REPLY_SCHEMA: dict[str, Any] = yaml.safe_load("""
oneOf:
- description: Summary form, in case `FULL` was not given.
  type: object
  additionalProperties: false
  properties:
    length:
      description: The number of entries in the stream (see `XLEN`).
      type: integer
    radix-tree-keys:
      description: The number of keys in the underlying radix data structure.
      type: integer
    radix-tree-nodes:
      description: The number of nodes in the underlying radix data structure.
      type: integer
    last-generated-id:
      description: The ID of the least-recently entry that was added to the stream.
      type: string
      pattern: '[0-9]+-[0-9]+'
    max-deleted-entry-id:
      description: The maximal entry ID that was deleted from the stream.
      type: string
      pattern: '[0-9]+-[0-9]+'
    recorded-first-entry-id:
      description: Cached copy of the first entry ID.
      type: string
      pattern: '[0-9]+-[0-9]+'
    entries-added:
      description: The count of all entries added to the stream during its lifetime.
      type: integer
    groups:
      description: The number of consumer groups defined for the stream.
      type: integer
    first-entry:
      description: The first entry of the stream.
      oneOf:
      - type: 'null'
      - type: array
        minItems: 2
        maxItems: 2
        items:
        - description: Entry ID.
          type: string
          pattern: '[0-9]+-[0-9]+'
        - description: Data.
          type: array
          items:
            type: string
    last-entry:
      description: The last entry of the stream.
      oneOf:
      - type: 'null'
      - type: array
        minItems: 2
        maxItems: 2
        items:
        - description: Entry ID.
          type: string
          pattern: '[0-9]+-[0-9]+'
        - description: Data.
          type: array
          items:
            type: string
- description: Extended form, in case `FULL` was given.
  type: object
  additionalProperties: false
  properties:
    length:
      description: The number of entries in the stream (see `XLEN`).
      type: integer
    radix-tree-keys:
      description: The number of keys in the underlying radix data structure.
      type: integer
    radix-tree-nodes:
      description: The number of nodes in the underlying radix data structure.
      type: integer
    last-generated-id:
      description: The ID of the least-recently entry that was added to the stream.
      type: string
      pattern: '[0-9]+-[0-9]+'
    max-deleted-entry-id:
      description: The maximal entry ID that was deleted from the stream.
      type: string
      pattern: '[0-9]+-[0-9]+'
    recorded-first-entry-id:
      description: Cached copy of the first entry ID.
      type: string
      pattern: '[0-9]+-[0-9]+'
    entries-added:
      description: The count of all entries added to the stream during its lifetime.
      type: integer
    entries:
      description: All the entries of the stream.
      type: array
      uniqueItems: true
      items:
        type: array
        minItems: 2
        maxItems: 2
        items:
        - description: Entry ID.
          type: string
          pattern: '[0-9]+-[0-9]+'
        - description: Data.
          type: array
          items:
            type: string
    groups:
      type: array
      items:
        type: object
        additionalProperties: false
        properties:
          name:
            description: Group name.
            type: string
          last-delivered-id:
            description: Last entry ID that was delivered to a consumer.
            type: string
            pattern: '[0-9]+-[0-9]+'
          entries-read:
            description: Total number of entries ever read by consumers in the group.
            oneOf:
            - type: 'null'
            - type: integer
          lag:
            description: Number of entries left to be consumed from the stream.
            oneOf:
            - type: 'null'
            - type: integer
          pel-count:
            description: Total number of unacknowledged entries.
            type: integer
          pending:
            description: Data about all of the unacknowledged entries.
            type: array
            items:
              type: array
              minItems: 4
              maxItems: 4
              items:
              - description: Entry ID.
                type: string
                pattern: '[0-9]+-[0-9]+'
              - description: Consumer name.
                type: string
              - description: Delivery timestamp.
                type: integer
              - description: Delivery count.
                type: integer
          consumers:
            description: Data about all of the consumers of the group.
            type: array
            items:
              type: object
              additionalProperties: false
              properties:
                active-time:
                  type: integer
                  description: Last time this consumer was active (successful reading/claiming).
                  minimum: 0
                name:
                  description: Consumer name.
                  type: string
                seen-time:
                  description: Timestamp of the last interaction attempt of the consumer.
                  type: integer
                  minimum: 0
                pel-count:
                  description: Number of unacknowledged entries that belong to the consumer.
                  type: integer
                pending:
                  description: Data about the unacknowledged entries.
                  type: array
                  items:
                    type: array
                    minItems: 3
                    maxItems: 3
                    items:
                    - description: Entry ID.
                      type: string
                      pattern: '[0-9]+-[0-9]+'
                    - description: Delivery timestamp.
                      type: integer
                    - description: Delivery count.
                      type: integer
""")
