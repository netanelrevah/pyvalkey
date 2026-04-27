# Code Style

## General
- No comments or docstrings — ever. Don't restate what code does; extract instead.
- No nested `if` — use fast returns.
- DRY: extract to variables/functions/constants when it aids readability.
- Use `is`/direct evaluation instead of `== True`/`== False`.
- Line length: 120 characters.
- Target Python 3.11+.

## Imports
- Always start with `from __future__ import annotations`.
- All imports must be at the top of the file. No imports inside functions or conditionals, except `TYPE_CHECKING` blocks.
- Move imports only needed for type annotations into a `TYPE_CHECKING` block:
  ```python
  from typing import TYPE_CHECKING
  if TYPE_CHECKING:
      from collections.abc import Callable
  ```

## Type Annotations
- Annotate every function parameter and return type.
- Use `|` union syntax, not `Union[...]`.
- Use lowercase generics: `dict[str, T]`, `list[T]`, `set[str]` — never `Dict`, `List`, `Set`.
- Use `Literal[...]` for parameters constrained to a finite set of values:
  ```python
  def configure(type_: Literal["string", "integer", "password"]) -> None: ...
  ```
- Use `Self` as the return type for method-chaining / fluent builder APIs.
- Declare class-level mappings with `ClassVar`:
  ```python
  MAPPING: ClassVar[dict[str, SomeTranslator]] = {}
  ```
- Declare type aliases at module level. Use a `Type` suffix for class-type aliases; no suffix required for value/union aliases:
  ```python
  FooTranslatorType = type["FooTranslator"]
  ValueType = int | bytes | str | None
  ```
- For multiline union aliases, break before each `|`:
  ```python
  ValueType = (
      int
      | bytes
      | str
      | None
  )
  ```
- Use `Generic[T]` with a named `TypeVar` (suffix `TypeVar`) to parameterize base classes so subclasses carry concrete types without `assert`:
  ```python
  FooTypeVar = TypeVar("FooTypeVar", bound=FooBase)

  class Translator(Generic[FooTypeVar]):
      def translate(self, item: FooTypeVar, ...) -> str | None: ...

  class ConcreteTranslator(Translator[ConcreteFoo]):
      def translate(self, item: ConcreteFoo, ...) -> str | None: ...
  ```
- Prefer bounded TypeVars (`bound=BaseClass`) over unconstrained ones.
- Covariant TypeVars use a `_co` suffix:
  ```python
  InstanceType_co = TypeVar("InstanceType_co", bound=Instance, covariant=True)
  ```

## Indentation and Structure
- Maximum indentation depth: 3–4 levels. Deeply nested pyramids must be extracted.
- Extract complex branches into named classmethods or functions — not module-level helpers if the logic belongs to one class.
- Prefer classmethods over staticmethods for class-internal helpers (use `@classmethod` with `cls`).
- A method should read as a pipeline: each step is a call, not an inline block.
- Use early `continue` in loops to avoid nesting, same as early `return` in functions.

## Class Design
- **Registry/decorator pattern** for pluggable dispatch: implement `register` as a classmethod that returns a decorator, then apply it to each concrete subclass:
  ```python
  class Base:
      MAPPING: ClassVar[dict[str, Base]] = {}

      @classmethod
      def register(cls, key: str) -> Callable[[BaseType], BaseType]:
          def decorator(translator_cls: BaseType) -> BaseType:
              cls.MAPPING[key] = translator_cls()
              return translator_cls
          return decorator

  @Base.register("foo")
  class FooImpl(Base): ...
  ```
- Use `raise NotImplementedError` in abstract base methods (no `@abstractmethod` needed).
- Use `Protocol` for interface contracts; protocol methods end with `...`.
- Use `@dataclass` for value-carrying classes; mutable defaults via `field(default_factory=...)`, private fields prefixed with `_`.
- Use `__post_init__` for initialization that runs after construction — cross-references, computed fields, self-registration:
  ```python
  @dataclass
  class Clause:
      context: QueryContext

      def __post_init__(self) -> None:
          self.context.where_clause = self
  ```
- Use `@dataclass(eq=False)` when the class overrides `__eq__` for DSL semantics (e.g., `col == value` returns an expression object) to prevent conflict with the auto-generated equality:
  ```python
  @dataclass(eq=False)
  class TableColumn(Expression): ...
  ```

## Return Patterns
- Return `None` to signal failure; callers check `if x is None`.
- Use early `return None` to keep the happy path at the bottom — never `elif` after a `return`.
- Use the inline ternary form for short results:
  ```python
  return None if args is None else f"len({args[0]})"
  ```
- Never use `assert isinstance` for type narrowing in production code — use `Generic[T]` on the base class instead so subclasses declare their concrete type directly.

## Naming
- Classes: `PascalCase`. Functions/variables: `snake_case`. Module-private: `_underscore_prefix`.
- No abbreviations — use full words: `substitution` not `sub`, `escape` not `esc`, `argument` not `arg`, `expression` not `expr`.
- `match` statements preferred over chained `if/elif` for dispatch on string/type keys.
- Append `_` suffix to names that conflict with Python keywords: `as_`, `is_`, `type_`, `from_`.
- No `__all__` — use `_prefix` for private symbols; everything else is public by default.

## Conditions
- Extract non-obvious boolean conditions into named local variables:
  ```python
  is_end_of_text = i >= len(raw)
  if is_end_of_text or cls._is_whitespace(raw, i):
  ```
- Extract repeated or non-obvious predicates into named classmethods:
  ```python
  @classmethod
  def _is_var_name_char(cls, c: str) -> bool:
      return c.isalnum() or c in ":_"
  ```

## Linting
- Prefer `# noqa: RULE` inline suppression over adding rules to `ruff.lint.ignore` in `pyproject.toml`. Inline suppressions are scoped and self-documenting.
