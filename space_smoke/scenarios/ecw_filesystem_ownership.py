"""ECW filesystem identity and parent-ownership contract."""

import space_smoke as smoke

WORKDIR = "/home/user/ecw-filesystem-ownership"
DEFAULT_OWNED_PATH = f"{WORKDIR}/default-user.txt"
ROOT_OWNED_PATH = f"{WORKDIR}/owned-by-root.txt"
USER_OWNED_PATH = f"{WORKDIR}/owned-by-user.txt"
WORKSPACE_FILE_PATH = "/home/user/workspace/projects/example/data/probe.txt"
WORKSPACE_DIRECTORY_PATH = "/home/user/workspace/projects/output"
WORKSPACE_TREE = (
    "/home/user/workspace",
    "/home/user/workspace/projects",
    "/home/user/workspace/projects/example",
    "/home/user/workspace/projects/example/data",
    WORKSPACE_FILE_PATH,
    WORKSPACE_DIRECTORY_PATH,
)


@smoke.scenario("ecw.filesystem_ownership")
def filesystem_ownership(test: smoke.Ecw) -> None:
    """Files written as root, explicit user, and default user retain their owner; parents
    auto-created for a user-owned workspace file remain writable by that user.
    """

    user_uid = test.identity.uid("user")

    with test.phase("write files as root and user"):
        test.sandbox.files.make_dir(WORKDIR)
        test.sandbox.files.write(DEFAULT_OWNED_PATH, "owned by default user")
        test.sandbox.files.write(ROOT_OWNED_PATH, "owned by root", user="root")
        test.sandbox.files.write(USER_OWNED_PATH, "owned by user", user="user")
        owners = test.identity.file_owner_uids((DEFAULT_OWNED_PATH, ROOT_OWNED_PATH, USER_OWNED_PATH))
        test.verify(
            owners
            == {
                DEFAULT_OWNED_PATH: user_uid,
                ROOT_OWNED_PATH: 0,
                USER_OWNED_PATH: user_uid,
            },
            "Files API writes did not retain their requested owners",
            expected_write_owners={ROOT_OWNED_PATH: 0, USER_OWNED_PATH: user_uid},
            observed_write_owners=owners,
        )

    with test.phase("auto-create a user-owned workspace tree"):
        test.sandbox.files.write(WORKSPACE_FILE_PATH, "ok", user="user")
        test.sandbox.files.make_dir(WORKSPACE_DIRECTORY_PATH, user="user")
        owners = test.identity.file_owner_uids(WORKSPACE_TREE)
        unexpected_owners = {
            path: uid for path, uid in owners.items() if uid != user_uid
        }
        test.verify(
            set(owners) == set(WORKSPACE_TREE) and not unexpected_owners,
            "Files API did not leave the workspace tree owned by the sandbox user",
            expected_uid=user_uid,
            observed_tree_owners=owners,
            unexpected_owners=unexpected_owners,
        )
