from space_smoke import scenario
from space_smoke.template_delivery import TemplateDeliveryFixture

@scenario(
    "native.template_delivery_exact_bytes",
    api_surfaces=("native",),
    required_tools=("ssh",),
    timeout_minutes=45,
    ignored="explicit application experiment configuration and authorized hosts required",
    rerun_environment=("SPACE_SMOKE_TEMPLATE_DELIVERY_CONFIG",),
)
def verify(bench: TemplateDeliveryFixture) -> None:
    """Flat and chunk delivery preserve builder bytes, immutable cached reuse, and writable guest clones."""
    bench.prove_native_gate()
