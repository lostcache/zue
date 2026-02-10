# Product Guidelines

## Documentation & Communication
- **Tone:** Technical and precise. Documentation and code comments must focus on accuracy, detailed implementation notes, and performance characteristics.
- **Clarity:** While technical, explanations should be clear and structured to help contributors understand the underlying distributed systems concepts.

## Development Principles
- **Performance First:** Every change must be evaluated for its impact on latency and throughput. The storage engine's core value is its speed.
- **Safety & Correctness:** Data integrity and memory safety are paramount. In complex replication and concurrency logic, correctness must never be sacrificed for minor performance gains.
- **Simplicity:** Favor straightforward, idiomatic Zig code. Avoid unnecessary abstractions that make the system harder to audit or maintain.

## Resilience & Error Handling
- **Explicit Handling:** Utilize Zig's explicit error handling (`try`, `catch`). Avoid panics in production code paths to ensure the system remains stable and predictable.
- **Graceful Degradation:** When non-critical failures occur (e.g., a single follower lag), the system should attempt to continue operating in a reduced but safe capacity.

## Visual Identity & Branding
- **Aesthetic:** Minimalist and professional.
- **CLI & Docs:** Use clean typography, simple ASCII art for architectural diagrams, and a consistent, understated color palette for terminal output to maintain a technical and focused environment.
