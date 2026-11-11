from __future__ import annotations

import unittest
from unittest import mock

from space_smoke.native import NativeConfig, NativeSandboxFactory
from space_smoke.scenarios.native_aws_secret_network_credential_injection import (
    aws_secret_network_credential_injection,
)


class NativeCredentialInjectionTest(unittest.TestCase):
    def test_adapter_uses_flat_reference_and_patch_contracts(self) -> None:
        factory = NativeSandboxFactory(
            NativeConfig(
                api_url="http://127.0.0.1:8080",
                data_plane_url="http://127.0.0.1:8081",
                token="token",
                template="seed:latest",
            )
        )
        policy = {
            "version": "v1",
            "egress": {"default_action": "deny"},
            "tls_interception": "on",
        }
        with (
            mock.patch.object(
                factory,
                "_json_request",
                side_effect=[
                    {"credential_reference_id": "reference-id"},
                    {"operation_id": "operation-id"},
                    {},
                ],
            ) as request,
            mock.patch.object(factory, "_wait_operation") as wait_operation,
        ):
            self.assertEqual(
                factory.register_credential_reference(
                    "arn:aws:secretsmanager:us-west-2:123456789012:secret:fixture"
                ),
                "reference-id",
            )
            factory.set_network_policy("sbx/id", policy)
            factory.retire_credential_reference("reference/id")

        registration = request.call_args_list[0].kwargs["body"]
        self.assertEqual(
            set(registration), {"request_id", "provider", "secret_arn"}
        )
        self.assertEqual(registration["provider"], "aws_secrets_manager")
        self.assertNotIn("locator", registration)
        self.assertNotIn("provider_instance_id", registration)
        self.assertEqual(
            request.call_args_list[1].kwargs["body"],
            {"network_policy": {"mode": "set", "policy": policy}},
        )
        self.assertEqual(
            request.call_args_list[1].args[:2],
            ("PATCH", "/v1/sandboxes/sbx%2Fid"),
        )
        self.assertEqual(
            request.call_args_list[2].args[:2],
            ("DELETE", "/v1/credential-references/reference%2Fid"),
        )
        wait_operation.assert_called_once_with("operation-id")
        self.assertEqual(
            aws_secret_network_credential_injection.api_surfaces, frozenset({"native"})
        )
        self.assertEqual(aws_secret_network_credential_injection.tier, "manual")
        self.assertTrue(aws_secret_network_credential_injection.manual_reason)


if __name__ == "__main__":
    unittest.main()
