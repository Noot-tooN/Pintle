# Naming decision — Pintle

**FINAL NAME: Pintle.** The repository has been created and cloned by the maintainer. Pintle is a mechanical pivot/connecting pin—a short technical name compatible with a broad coordination toolkit. Names below are historical brainstorming only; do not choose one or rename the project without an explicit request.

## Naming directions

| Candidate | Story / tone | Preliminary caution |
|---|---|---|
| **Weyve** | Pronounced like “wave”; weave independent threads into one fabric, movement across systems | Invented spelling; search did not establish domain/trademark/package availability |
| **Tessiq** | Tessellation + precise building blocks; terse, technical, infrastructure-like | Invented, may be harder to pronounce; availability unverified |
| **Conflux** | Separate flows meeting at one place; broad, strong engineering feel | Existing common term and likely brand/project collisions: verify carefully |
| **Trestle** | A reliable supporting structure beneath other software | Existing infrastructure/engineering uses, availability uncertain |
| **Knotra** | Nodes and connections tied together, slightly playful | Already used by unrelated IT/services/community projects; **not cleared** |
| **Weft** | The thread that binds a weave; concise, genuine metaphor for coordination | Existing software named Weft; not a clear choice |
| **Skein** | Interlaced threads; distinctive systems vibe | Multiple software projects use Skein |
| **Wovra** | Woven + short abstract suffix; neutral/invented | Existing name uses in textile/store context; not cleared |
| **Latchkey** | Immediate lock/safety imagery, memorable | Locks-focused and likely broad collisions |
| **Tandem** | Independent workers operating together | Very widely used in other software/businesses |
| **Threadwell** | Code threads functioning together, friendly and memorable | Existing browser extension named Threadwell |
| **Ubiquity** | Everywhere, available across systems | Existing tech projects and close to Ubiquiti networking brand |

**Historical note:** Alternative names were explored and rejected or set aside. Use Pintle in new code.

## Before committing a name

1. Search GitHub exact repositories and organizations; crates.io, npm, PyPI and Go import-path expectations.
2. Check domain and social handles without assuming a search-result absence means availability.
3. Check trademark conflicts for software/infrastructure classes in intended jurisdictions.
4. Say it aloud, ensure unambiguous spelling and pronounceability.
5. Name must work as `cargo` package prefix, Go module, CLI and possible `api.<brand>.dev` endpoint.

Use `pintle.v1` as the provisional Protobuf package name for P0b; do not publish an immutable v1 wire contract until protocol tests are complete. No trademark clearance is implied.

### Name research references (samples)

- Existing Weft software: https://github.com/WeaveMindAI/weft
- Existing Heddle CLI (another weaving-related name): https://heddle.sh/docs
- Existing Skein software: https://github.com/Ragin-LundF/smart-skein
- Existing Threadwell app: https://threadwell.app/guide.html
- Existing Wovra usage: https://wovra.site/policies/terms-of-service
- Existing Knotra usage: https://play.google.com/store/apps/details?id=com.knotra.mobile
