[BRO NOTE] Helpful Links / Pointers

Repos
- Backend prototype (LisaEnginePrototype): https://github.com/unity-hallie/LISA (python backend folder: `LisaEnginePrototype`)

Backend Files (when you’re ready — no rush)
- `LisaEnginePrototype/services/canvas_record_factory.py` — teaching scaffold with normalize_* stubs and derivation hook.
- `LisaEnginePrototype/models/json_data_haver_mixin.py` — JSON storage/versioning; [BRO NOTE] comments on safe usage.
- `LisaEnginePrototype/services/compliance_service.py` — metrics logic that will read raw + derived fields.
- `LisaEnginePrototype/api_server.py` — endpoints; ignore demo loaders for now.

Your Entry Points
- `helpers/derivations.py` — keep rules here. Later, we’ll mirror them in the factory.
- `matt_mini_test.py` — applies rules and emits audit columns.

