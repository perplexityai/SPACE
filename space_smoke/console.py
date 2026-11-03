"""The admin console (`ui/`), driven through a browser by the `local.console_*` scenarios.

Each scenario signs in to the bench's own console as a real user, acts through
the pages an operator uses, and judges the outcome twice: by what the page
shows and by what the admin API answers afterwards. The API is the oracle;
the console only renders it.

A browser that cannot launch is a capability the bench lacks, not a failure:
the suite runner asks `browser_unavailable` once and skips every scenario that
declares a browser, visibly, naming why. A scenario run by name without one is
refused the same way, as a configuration error.

Every page a scenario opens records the browser's console errors, uncaught
page errors, failed requests and HTTP error answers. `BASELINE` names the ones
the console produces by design, each with its reason; a scenario allows the
failures its own probes cause (a lookup of a sandbox that does not exist
answers 404) through `allow`; `expect_no_new_browser_errors` fails on
anything else.

The scenarios create their own organizations, users and foreign-org
sandboxes, under names unique to the run, and remove them in cleanup: the
sandboxes first, then the users, then the organizations, each refusing to
delete anything it did not create.
"""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

from space_smoke.capabilities import ConsoleControl
from space_smoke.runtime import ResourceScope, SmokeConfigurationError

if TYPE_CHECKING:
    from playwright.sync_api import Browser, Locator, Page

    from space_smoke.fixtures import KindEcw

BROWSER_CHROMIUM = "chromium"
BROWSERS = frozenset({BROWSER_CHROMIUM})

# The installation the skip reason points at: Playwright's own Chromium build,
# with the system libraries it links against.
CHROMIUM_INSTALL = "uv run --frozen --python 3.11 python -m playwright install --with-deps chromium"

# How long one page interaction may take before it counts as not happening.
# The console polls its sandbox views every 4 s; this covers two polls and a
# slow first render of a cold Next.js server.
PAGE_TIMEOUT_MS = 20_000
# How long a lifecycle change the console asked for may take to show in the
# admin API. Suspend uploads a snapshot to the bench's object store.
LIFECYCLE_TIMEOUT_SECONDS = 240.0
# How long a cancelled confirmation is watched for a request it should never send.
CANCEL_SETTLE_SECONDS = 3.0

# What the console shows when the page it was asked for did not resolve: a
# detail page's not-found or load-failure state, Next.js's own 404 page, and
# the notice that turns away a signed-in non-operator.
UNRESOLVED_PAGE = re.compile(
    r"\b(?:Sandbox|Organization|User|Node) not found\b"
    r"|Could not load the (?:sandbox|organization|user|node)\b"
    r"|This page could not be found"
    r"|This console is for cluster operators"
)
NOT_FOR_YOU = "This console is for cluster operators"


def browser_unavailable(browser: str) -> str | None:
    """Why this environment cannot launch `browser` headless, or None when it can.

    The probe launches the browser rather than looking for its files: a build
    that is present but cannot load its system libraries skips the same way.
    """

    if browser not in BROWSERS:
        return f"unknown browser {browser!r}"
    try:
        from playwright.sync_api import Error, sync_playwright
    except ImportError:
        return "Playwright for Python is not importable in this environment"
    try:
        with sync_playwright() as playwright:
            playwright.chromium.launch(headless=True).close()
    except Error as error:
        return _launch_failure(error)
    return None


def _launch_failure(error: Exception) -> str:
    message = str(error)
    if "Executable doesn't exist" in message:
        return f"Playwright's Chromium is not installed ({CHROMIUM_INSTALL})"
    if "error while loading shared libraries" in message:
        return f"Playwright's Chromium cannot load its system libraries ({CHROMIUM_INSTALL})"
    first = next((line for line in message.splitlines() if line.strip()), message)
    return f"Playwright's Chromium did not launch: {first.strip()[:300]}"


@dataclass(frozen=True)
class BrowserEvent:
    """One error the browser reported while a scenario's page was open."""

    # console (a console.error message), pageerror (an uncaught exception),
    # requestfailed (no response at all) or http (a response of 400 or more).
    kind: str
    text: str
    url: str
    page: str

    def describe(self) -> str:
        where = f" {self.url}" if self.url else ""
        return f"{self.kind}: {self.text}{where} (on {self.page})"


@dataclass(frozen=True)
class Allowed:
    """A browser error that is expected, and why."""

    kind: str
    # Searched in the event's text and URL together.
    pattern: str
    reason: str

    def matches(self, event: BrowserEvent) -> bool:
        return event.kind == self.kind and re.search(self.pattern, f"{event.text} {event.url}") is not None


# The errors the console produces by design, each observed on a Kind run of
# these scenarios. Anything else a page reports fails the scenario that
# opened it.
BASELINE: tuple[Allowed, ...] = (
    Allowed(
        "requestfailed",
        r"^net::ERR_ABORTED ",
        "a navigation cancels the previous page's reads still in flight",
    ),
    Allowed(
        "http",
        r"^404 /api/v1/sandboxes/sbx_[0-9a-f-]+/snapshots(?:\?|$)",
        "snapshot listing is a hidden native operation: unless the organization enables it, the API "
        "answers 404 'route not found', which the sandbox page reads as snapshots being off",
    ),
    Allowed(
        "console",
        r"^Failed to load resource: the server responded with a status of 404 .*/api/v1/sandboxes/sbx_[0-9a-f-]+/snapshots(?:\?|$)",
        "Chromium logs that same hidden-operation 404",
    ),
)


@dataclass(frozen=True)
class ConsoleAccount:
    """A console user the scenario created, and the password it signs in with."""

    user_id: str
    org_id: str
    email: str
    password: str
    role: str


@dataclass(frozen=True)
class ConsoleTenant:
    """An organization the scenario created, other than the one managing the cluster."""

    org_id: str
    slug: str
    name: str


class ConsoleSession:
    """One signed-in browser context and its page."""

    def __init__(self, console: ConsoleAssertions, page: Page, label: str) -> None:
        self._console = console
        self.page = page
        self.label = label
        # State-changing requests the page sent to the API, oldest first.
        self.writes: list[str] = []
        page.on("request", self._record_write)

    def _record_write(self, request: Any) -> None:
        path = urlsplit(request.url).path
        if request.method != "GET" and (path.startswith("/admin/") or path.startswith("/api/")):
            self.writes.append(f"{request.method} {path}")

    def open(self, path: str) -> None:
        self.page.goto(self._console.url + path, wait_until="domcontentloaded")

    @property
    def path(self) -> str:
        return urlsplit(self.page.url).path

    def text(self) -> str:
        try:
            return self.page.locator("body").inner_text(timeout=5_000)
        except Exception:  # noqa: BLE001 - evidence only
            return ""


class ConsoleAssertions:
    """Drive the admin console and judge it against the admin API."""

    def __init__(self, fixture: KindEcw, control: ConsoleControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self.url = control.console_url().rstrip("/")
        self._run = uuid.uuid4().hex[:12]
        self._browser: Browser | None = None
        self._operator: ConsoleSession | None = None
        self.events: list[BrowserEvent] = []
        self._allowed: list[Allowed] = list(BASELINE)
        self._names = 0

    # --- browser ----------------------------------------------------------

    def _launch(self) -> Browser:
        if self._browser is not None:
            return self._browser
        try:
            from playwright.sync_api import Error, sync_playwright
        except ImportError:
            raise SmokeConfigurationError(
                "the console scenarios need a browser: Playwright for Python is not importable"
            ) from None
        playwright = sync_playwright().start()
        self._scope.defer("stop Playwright", playwright.stop)
        try:
            browser = playwright.chromium.launch(headless=True)
        except Error as error:
            raise SmokeConfigurationError(
                f"the console scenarios need a browser: {_launch_failure(error)}"
            ) from None
        self._browser = browser
        self._scope.defer("close the console browser", browser.close)
        return browser

    def _new_session(self, label: str) -> ConsoleSession:
        context = self._launch().new_context(viewport={"width": 1600, "height": 1000})
        context.set_default_timeout(PAGE_TIMEOUT_MS)
        self._scope.defer(f"close the {label} browser context", context.close)
        page = context.new_page()
        where = lambda: urlsplit(page.url).path or page.url  # noqa: E731 - evaluated per event

        def on_console(message: Any) -> None:
            if message.type == "error":
                location = message.location or {}
                self.events.append(BrowserEvent("console", message.text, str(location.get("url") or ""), where()))

        def on_page_error(error: Any) -> None:
            self.events.append(BrowserEvent("pageerror", str(error), "", where()))

        def on_request_failed(request: Any) -> None:
            self.events.append(BrowserEvent("requestfailed", str(request.failure or ""), _path(request.url), where()))

        def on_response(response: Any) -> None:
            if response.status >= 400:
                self.events.append(BrowserEvent("http", str(response.status), _path(response.url), where()))

        page.on("console", on_console)
        page.on("pageerror", on_page_error)
        page.on("requestfailed", on_request_failed)
        page.on("response", on_response)
        return ConsoleSession(self, page, label)

    def allow(self, kind: str, pattern: str, reason: str) -> None:
        """Expect a browser error this scenario's own probe causes."""

        self._allowed.append(Allowed(kind, pattern, reason))

    def allow_missing(self, path: str, reason: str) -> None:
        """Expect the 404 a read of `path` answers, and Chromium's log line for it."""

        escaped = re.escape(path)
        self.allow("http", rf"^404 {escaped}(?:\?|$)", reason)
        self.allow(
            "console",
            rf"^Failed to load resource: the server responded with a status of 404 .*{escaped}(?:\?|$)",
            reason,
        )

    def unexpected_events(self) -> list[BrowserEvent]:
        return [event for event in self.events if not any(allowed.matches(event) for allowed in self._allowed)]

    def _allowed_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for event in self.events:
            for allowed in self._allowed:
                if allowed.matches(event):
                    counts[allowed.reason] = counts.get(allowed.reason, 0) + 1
                    break
        return counts

    def _evidence(self, session: ConsoleSession | None = None) -> Callable[[], Mapping[str, object]]:
        def collect() -> Mapping[str, object]:
            evidence: dict[str, object] = {
                "console_unexpected_browser_errors": [event.describe() for event in self.unexpected_events()][:20],
            }
            if session is not None:
                evidence["console_page"] = session.page.url
                evidence["console_page_text"] = session.text()[:1_500]
                evidence["console_page_writes"] = list(session.writes)
            return evidence

        return collect

    # --- accounts -----------------------------------------------------------

    def sign_in_operator(self) -> ConsoleSession:
        """The bench's seeded operator, signed in through the login page."""

        if self._operator is None:
            email, password = self._control.console_operator_login()
            self._operator = self._sign_in(email, password, "operator")
        return self._operator

    def sign_in(self, account: ConsoleAccount) -> ConsoleSession:
        return self._sign_in(account.email, account.password, account.email)

    def _sign_in(self, email: str, password: str, label: str) -> ConsoleSession:
        session = self._new_session(label)
        page = session.page
        session.open("/login")
        page.get_by_label("Email").fill(email)
        page.get_by_label("Password").fill(password)
        page.get_by_role("button", name="Log in").click()
        try:
            page.wait_for_url(re.compile(r"/dashboard(?:$|[/?])"))
            page.get_by_text("Log out").first.wait_for()
        except Exception:  # noqa: BLE001 - judged below with the page as evidence
            pass
        self._fixture.verify(
            session.path.startswith("/dashboard"),
            "the console's login page did not sign the user in",
            console_user=label,
            failure_evidence=self._evidence(session),
        )
        return session

    def managing_org_id(self) -> str:
        cluster = self._api("GET", "/auth/v1/cluster")
        org_id = cluster.get("managing_org_id") if isinstance(cluster, dict) else None
        if not isinstance(org_id, str) or not org_id:
            raise SmokeConfigurationError("the cluster record names no managing organization")
        return org_id

    def create_tenant(self) -> ConsoleTenant:
        """A run-owned organization other than the managing one; deleted in cleanup."""

        self._names += 1
        slug = f"console-smoke-{self._run}-{self._names}"
        tenant_id: list[str] = []

        def delete() -> None:
            org_id = tenant_id[0] if tenant_id else self._org_id_by_slug(slug)
            if org_id is None:
                return
            users = self._api("GET", f"/admin/api/v1/orgs/{org_id}/users")
            for user in users if isinstance(users, list) else []:
                email = user.get("email") if isinstance(user, dict) else None
                if not isinstance(email, str) or not email.startswith(f"{slug}-"):
                    raise SmokeConfigurationError(f"refusing to delete {slug}: it holds a user this run did not create")
            self._delete(f"/admin/api/v1/orgs/{org_id}")

        self._scope.defer(f"delete console organization {slug}", delete, critical=True)
        created = self._api("POST", "/admin/api/v1/orgs", {"name": slug, "slug": slug}, expect=(200, 201))
        org_id = created.get("id") if isinstance(created, dict) else None
        if not isinstance(org_id, str):
            raise SmokeConfigurationError("organization creation answered no id")
        tenant_id.append(org_id)
        self._fixture.record(f"console_tenant_{self._names}", {"org_id": org_id, "slug": slug})
        return ConsoleTenant(org_id=org_id, slug=slug, name=slug)

    def create_account(self, org_id: str, role: str, *, slug: str | None = None) -> ConsoleAccount:
        """A run-owned user of `org_id` with a password; deleted in cleanup."""

        self._names += 1
        prefix = slug or f"console-smoke-{self._run}"
        email = f"{prefix}-{self._names}-{role.replace('_', '-')}@local"
        password = uuid.uuid4().hex + "Aa1!"
        user_id: list[str] = []

        def delete() -> None:
            identifier = user_id[0] if user_id else self._user_id_by_email(org_id, email)
            if identifier is not None:
                self._delete(f"/admin/api/v1/users/{identifier}")

        self._scope.defer(f"delete console user {email}", delete, critical=True)
        created = self._api(
            "POST",
            f"/admin/api/v1/orgs/{org_id}/users",
            {"email": email, "password": password, "display_name": email.split("@", 1)[0], "role": role},
            expect=(200, 201),
        )
        identifier = created.get("id") if isinstance(created, dict) else None
        if not isinstance(identifier, str):
            raise SmokeConfigurationError("user creation answered no id")
        user_id.append(identifier)
        return ConsoleAccount(user_id=identifier, org_id=org_id, email=email, password=password, role=role)

    def create_foreign_sandbox(self, tenant: ConsoleTenant) -> str:
        """A started sandbox owned by `tenant`, created through the admin API.

        The tenant needs a user to own it. The sandbox is force-deleted, its
        deletion awaited and its node-local rows reclaimed, before that user
        and the tenant are deleted.
        """

        template = self._fixture.sandbox_template_name()
        public_id: list[str] = []
        key = str(uuid.uuid4())
        body = {"org_id": tenant.org_id, "sandbox": {"template": template, "metadata": {"space_smoke_run": self._run}}}

        def delete() -> None:
            if not public_id:
                # Replaying the create's key answers the sandbox it created, if any.
                replay = self._control.console_api("POST", "/admin/api/v1/sandboxes", body, idempotency_key=key)
                if replay.status not in (200, 201, 202) or not isinstance(replay.body, dict):
                    return
                public_id.append(str(replay.body["sandbox_id"]))
            self._force_delete(public_id[0])

        self._scope.defer(f"delete the {tenant.slug} sandbox", delete, critical=True)
        answer = self._control.console_api("POST", "/admin/api/v1/sandboxes", body, idempotency_key=key)
        if answer.status != 202 or not isinstance(answer.body, dict) or not answer.body.get("sandbox_id"):
            raise SmokeConfigurationError(f"admin sandbox create answered HTTP {answer.status}")
        sandbox_id = str(answer.body["sandbox_id"])
        public_id.append(sandbox_id)
        self._fixture.record("console_foreign_sandbox_id", sandbox_id)
        self._await_status(sandbox_id, "started")
        return sandbox_id

    # --- the admin API: reads, and the cleanup writes ---------------------

    def _api(self, method: str, path: str, body: Mapping[str, object] | None = None, *, expect: tuple[int, ...] = (200,)) -> Any:
        answer = self._control.console_api(method, path, body)
        if answer.status not in expect:
            raise SmokeConfigurationError(f"{method} {path.split('?', 1)[0]} answered HTTP {answer.status}")
        return answer.body

    def _delete(self, path: str) -> None:
        self._api("DELETE", path, expect=(200, 204, 404))

    def _org_id_by_slug(self, slug: str) -> str | None:
        orgs = self._api("GET", "/admin/api/v1/orgs")
        matches = [org["id"] for org in orgs if isinstance(org, dict) and org.get("slug") == slug]
        if len(matches) > 1:
            raise SmokeConfigurationError(f"more than one organization has the run's slug {slug}")
        return matches[0] if matches else None

    def _user_id_by_email(self, org_id: str, email: str) -> str | None:
        answer = self._control.console_api("GET", f"/admin/api/v1/orgs/{org_id}/users")
        if answer.status == 404 or not isinstance(answer.body, list):
            return None
        matches = [user["id"] for user in answer.body if isinstance(user, dict) and user.get("email") == email]
        return matches[0] if matches else None

    def admin_sandbox(self, sandbox_id: str) -> dict[str, Any] | None:
        """The admin API's sandbox record, or None once it answers 404."""

        answer = self._control.console_api("GET", f"/admin/api/v1/sandboxes/{public_sandbox_id(sandbox_id)}")
        if answer.status == 404:
            return None
        if answer.status != 200 or not isinstance(answer.body, dict):
            raise SmokeConfigurationError(f"admin sandbox read answered HTTP {answer.status}")
        return answer.body

    def _await_status(self, sandbox_id: str, status: str, *, timeout: float = LIFECYCLE_TIMEOUT_SECONDS) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        observed: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            observed = self.admin_sandbox(sandbox_id)
            if observed is not None and observed.get("status") == status:
                return observed
            if observed is not None and observed.get("status") == "failed":
                break
            time.sleep(1)
        self._fixture.verify(
            False,
            f"the sandbox did not reach {status} in the admin API",
            sandbox_id=sandbox_id,
            observed_status=(observed or {}).get("status"),
            observed_failure_reason=(observed or {}).get("failure_reason"),
        )
        raise AssertionError("unreachable")

    def _await_gone(self, sandbox_id: str, *, timeout: float = LIFECYCLE_TIMEOUT_SECONDS) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            observed = self.admin_sandbox(sandbox_id)
            if observed is None or observed.get("status") == "deleted":
                return True
            time.sleep(1)
        return False

    def _force_delete(self, sandbox_id: str) -> None:
        if self.admin_sandbox(sandbox_id) is not None:
            answer = self._control.console_api(
                "POST",
                f"/admin/api/v1/sandboxes/{public_sandbox_id(sandbox_id)}:force-delete",
                idempotency_key=str(uuid.uuid4()),
            )
            if answer.status not in (200, 202, 404):
                raise SmokeConfigurationError(f"admin force-delete answered HTTP {answer.status}")
            if not self._await_gone(sandbox_id):
                raise SmokeConfigurationError(f"sandbox {sandbox_id} was not deleted")
        # As the fixture does for its own sandboxes: a suspended sandbox's
        # local rows would otherwise outlive the scenario on its worker.
        self._control.reclaim_local_sandbox_everywhere(sandbox_id.removeprefix("sbx_"))

    # --- page reads ----------------------------------------------------------

    def _visible(self, locator: Locator, timeout: float = PAGE_TIMEOUT_MS) -> bool:
        try:
            locator.wait_for(state="visible", timeout=timeout)
            return True
        except Exception:  # noqa: BLE001 - a missing element is the verdict
            return False

    def _hidden(self, locator: Locator, timeout: float = PAGE_TIMEOUT_MS) -> bool:
        try:
            locator.wait_for(state="hidden", timeout=timeout)
            return True
        except Exception:  # noqa: BLE001 - a lingering element is the verdict
            return False

    def _description(self, session: ConsoleSession, label: str) -> Locator:
        """The value cell of a Descriptions item, by its label."""

        return session.page.locator(
            f"xpath=//th[contains(@class,'ant-descriptions-item-label')][normalize-space()={_xpath_literal(label)}]"
            "/following-sibling::td[1]"
            f" | //span[contains(@class,'ant-descriptions-item-label')][normalize-space()={_xpath_literal(label)}]"
            "/following-sibling::span[1]"
        ).first

    def expect_page_resolved(self, session: ConsoleSession, path: str, *, shows: str, what: str) -> None:
        """The page at `path` rendered `shows` and none of the unresolved states."""

        page = session.page
        try:
            page.wait_for_url(re.compile(rf"{re.escape(path)}(?:$|\?)"))
        except Exception:  # noqa: BLE001 - judged below
            pass
        # The content, not the sidebar, which names the signed-in user's organization.
        shown = self._visible(page.locator(".ant-layout-content").get_by_text(shows, exact=True).first)
        unresolved = UNRESOLVED_PAGE.search(session.text())
        self._fixture.verify(
            session.path == path and shown and unresolved is None,
            f"{what} did not resolve in the console",
            console_expected_path=path,
            console_path=session.path,
            console_expected_text=shows,
            console_expected_text_shown=shown,
            console_unresolved_state=unresolved.group(0) if unresolved else None,
            failure_evidence=self._evidence(session),
        )

    # --- lookup ---------------------------------------------------------------

    def sandbox_id_forms(self, sandbox_id: str) -> dict[str, str]:
        """Every form of the sandbox's identity the console accepts, by name."""

        bare = str(uuid.UUID(sandbox_id.removeprefix("sbx_")))
        pods = [pod.pod_name for pod in self._control.sandbox_pods(bare) if not pod.deleting]
        if len(pods) != 1:
            raise SmokeConfigurationError(f"expected one live pod for sandbox {bare}, found {len(pods)}")
        forms = {
            "public id": f"sbx_{bare}",
            "bare UUID": bare,
            "undashed UUID": bare.replace("-", ""),
            "pod name": pods[0],
        }
        self._fixture.record("console_lookup_forms", forms)
        return forms

    def expect_detail_route_accepts(self, session: ConsoleSession, form: str, value: str, public_id: str) -> None:
        """`/dashboard/sandboxes/<value>` lands on the sandbox's canonical page."""

        session.open(f"/dashboard/sandboxes/{quote(value, safe='')}")
        self.expect_page_resolved(
            session, f"/dashboard/sandboxes/{public_id}", shows=public_id, what=f"the detail route for the {form}"
        )

    def expect_search_finds(self, session: ConsoleSession, form: str, value: str, public_id: str) -> None:
        """The sandbox list's search box narrows to exactly this sandbox."""

        page = session.page
        if session.path != "/dashboard/sandboxes":
            session.open("/dashboard/sandboxes")
        search = page.get_by_placeholder("ID, pod name or name")
        search.fill(value)
        rows = page.locator(".ant-table-tbody tr.ant-table-row")
        target = f"/dashboard/sandboxes/{public_id}"
        hrefs: list[str | None] = []
        deadline = time.monotonic() + PAGE_TIMEOUT_MS / 1000
        while time.monotonic() < deadline:
            hrefs = [row.locator("a").first.get_attribute("href") for row in rows.all()]
            if hrefs == [target]:
                break
            time.sleep(0.5)
        self._fixture.verify(
            hrefs == [target],
            f"the sandbox list's search did not find exactly the sandbox by its {form}",
            console_search=value,
            console_search_rows=hrefs,
            failure_evidence=self._evidence(session),
        )
        search.fill("")

    def expect_unknown_id_not_found(self, session: ConsoleSession) -> None:
        """Control: a well-formed ID of no sandbox is not found, by both routes."""

        missing = f"sbx_{uuid.uuid4()}"
        self.allow_missing(f"/admin/api/v1/sandboxes/{missing}", "the control looks up a sandbox that does not exist")
        session.open(f"/dashboard/sandboxes/{missing}")
        shown = self._visible(session.page.get_by_text("Sandbox not found", exact=True).first)
        self._fixture.verify(
            shown,
            "the detail route did not report a sandbox ID that names no sandbox as not found",
            console_missing_id=missing,
            failure_evidence=self._evidence(session),
        )
        session.open("/dashboard/sandboxes")
        session.page.get_by_placeholder("ID, pod name or name").fill(missing)
        warned = self._visible(session.page.get_by_text(f"{missing}:", exact=False).first)
        rows = session.page.locator(".ant-table-tbody tr.ant-table-row").count()
        self._fixture.verify(
            warned and rows == 0,
            "the sandbox list's search did not report a sandbox ID that names no sandbox",
            console_missing_id=missing,
            console_search_rows=rows,
            failure_evidence=self._evidence(session),
        )

    # --- detail completeness ---------------------------------------------------

    def expect_detail_complete(self, session: ConsoleSession, sandbox_id: str, state: str) -> None:
        """The detail page shows what the admin API knows of the sandbox in `state`.

        `state` is the admin API's lifecycle state: a running sandbox is `started`.

        The header carries its ID and state; the Details card its template,
        node (or `unassigned` without one), organization by name and owner by
        email, each linking to its page; the Controls card enables exactly the
        lifecycle moves valid from that state; the Operations card lists every
        operation the admin API records against it.
        """

        public_id = public_sandbox_id(sandbox_id)
        record = self.admin_sandbox(public_id)
        self._fixture.verify(record is not None, "the admin API no longer has the sandbox", sandbox_id=public_id)
        assert record is not None
        self._fixture.verify(
            record.get("status") == state,
            f"the sandbox is not {state} in the admin API",
            sandbox_id=public_id,
            observed_status=record.get("status"),
        )
        org = self._api("GET", f"/admin/api/v1/orgs/{record['org_id']}")
        owner = self._api("GET", f"/admin/api/v1/users/{record['user_id']}")
        node = record.get("active_node") or None
        session.open(f"/dashboard/sandboxes/{public_id}")
        self.expect_page_resolved(session, f"/dashboard/sandboxes/{public_id}", shows=public_id, what=f"the {state} sandbox's page")
        page = session.page
        header_state = self._visible(page.locator(".ant-tag", has_text=re.compile(rf"^{re.escape(state)}$")).first)
        expected_fields = {
            "Template": str(record.get("template") or ""),
            "Org": str(org["name"]),
            "Owner": str(owner["email"]),
            "Node": str(node.get("name") or node.get("address") or node["id"][:8]) if node else "unassigned",
        }
        shown_fields: dict[str, str] = {}
        for label, expected in expected_fields.items():
            cell = self._description(session, label)
            deadline = time.monotonic() + PAGE_TIMEOUT_MS / 1000
            text = ""
            while time.monotonic() < deadline:
                text = cell.inner_text() if cell.count() else ""
                if expected in text:
                    break
                time.sleep(0.5)
            shown_fields[label] = text
        links = {
            "Org": self._description(session, "Org").locator("a").first.get_attribute("href"),
            "Owner": self._description(session, "Owner").locator("a").first.get_attribute("href"),
        }
        complete = all(expected in shown_fields[label] for label, expected in expected_fields.items())
        self._fixture.verify(
            header_state and complete
            and links["Org"] == f"/dashboard/orgs/{record['org_id']}"
            and links["Owner"] == f"/dashboard/users/{record['user_id']}",
            f"the {state} sandbox's page does not show what the admin API records",
            console_header_state_shown=header_state,
            console_expected_fields=expected_fields,
            console_shown_fields=shown_fields,
            console_links=links,
            failure_evidence=self._evidence(session),
        )
        controls = page.locator(".ant-card", has=page.locator(".ant-card-head-title", has_text=re.compile(r"^Controls$")))
        enabled = {
            label: _button(controls, label).is_enabled()
            for label in LIFECYCLE_MOVES
        }
        expected_moves = {label: label in LIFECYCLE_MOVES_FROM.get(state, ()) for label in LIFECYCLE_MOVES}
        self._fixture.verify(
            enabled == expected_moves,
            f"the {state} sandbox's page offers lifecycle moves that state does not have",
            console_enabled_moves=enabled,
            console_expected_moves=expected_moves,
            failure_evidence=self._evidence(session),
        )
        self.expect_operations_listed(session, public_id, record.get("org_id"))

    def expect_operations_listed(self, session: ConsoleSession, public_id: str, org_id: object) -> list[str]:
        """The page's Operations card lists every operation the admin API records for the sandbox."""

        query = f"target_type=sandbox&target_id={public_id}&limit=50"
        if isinstance(org_id, str):
            query += f"&org_id={org_id}"
        recorded = self._api("GET", f"/admin/api/v1/operations?{query}")
        kinds = sorted({str(op["kind"]) for op in recorded if isinstance(op, dict)}) if isinstance(recorded, list) else []
        card = session.page.locator(".ant-card", has=session.page.locator(".ant-card-head-title", has_text="Operations"))
        shown: list[str] = []
        empty = False
        deadline = time.monotonic() + PAGE_TIMEOUT_MS / 1000
        while time.monotonic() < deadline:
            shown = sorted(set(card.locator("td code").all_inner_texts()))
            empty = card.get_by_text("No operations recorded", exact=True).count() > 0
            if (kinds and all(kind in shown for kind in kinds)) or (not kinds and empty):
                break
            time.sleep(0.5)
        self._fixture.verify(
            all(kind in shown for kind in kinds) if kinds else empty,
            "the sandbox's page does not list the operations the admin API records for it",
            console_recorded_operation_kinds=kinds,
            console_shown_operation_kinds=shown,
            console_operations_empty_state=empty,
            failure_evidence=self._evidence(session),
        )
        return kinds

    def force_action(self, sandbox_id: str, action: str, state: str) -> None:
        """An operator's admin force action, awaited until the sandbox is in `state`.

        Unlike the ECW calls, it records an operation against the sandbox,
        which its console page must then list.
        """

        public_id = public_sandbox_id(sandbox_id)
        answer = self._control.console_api(
            "POST",
            f"/admin/api/v1/sandboxes/{public_id}:force-{action}",
            idempotency_key=str(uuid.uuid4()),
        )
        if answer.status not in (200, 202):
            raise SmokeConfigurationError(f"admin force-{action} answered HTTP {answer.status}")
        self._await_status(public_id, state)

    def expect_deleted_detail(self, session: ConsoleSession, sandbox_id: str) -> None:
        """A deleted sandbox's page says so and still lists the operations recorded against it."""

        public_id = public_sandbox_id(sandbox_id)
        self._fixture.verify(
            self._await_gone(public_id),
            "the deleted sandbox is still live in the admin API",
            sandbox_id=public_id,
        )
        record = self.admin_sandbox(public_id)
        if record is None:
            self.allow_missing(f"/admin/api/v1/sandboxes/{public_id}", "the page reads the deleted sandbox's row, which is gone")
            session.open(f"/dashboard/sandboxes/{public_id}")
            gone = self._visible(session.page.get_by_text("Sandbox not found", exact=True).first)
            history = self._visible(session.page.get_by_text("lifecycle operations are still recorded", exact=False).first)
            self._fixture.verify(
                gone and history,
                "the deleted sandbox's page does not say it is gone and keep its history",
                console_gone_shown=gone,
                console_history_shown=history,
                failure_evidence=self._evidence(session),
            )
            kinds = self.expect_operations_listed(session, public_id, None)
        else:
            session.open(f"/dashboard/sandboxes/{public_id}")
            self.expect_page_resolved(session, f"/dashboard/sandboxes/{public_id}", shows=public_id, what="the deleted sandbox's page")
            shown = self._visible(session.page.locator(".ant-tag", has_text=re.compile(r"^deleted$")).first)
            self._fixture.verify(shown, "the deleted sandbox's page does not show it deleted", failure_evidence=self._evidence(session))
            kinds = self.expect_operations_listed(session, public_id, record.get("org_id"))
        self._fixture.verify(
            bool(kinds),
            "the admin API records no operation for the deleted sandbox, so its page has no history to show",
            console_recorded_operation_kinds=kinds,
        )

    # --- cross-org action ---------------------------------------------------------

    def expect_cross_org_suspend(self, session: ConsoleSession, sandbox_id: str, tenant: ConsoleTenant, owner: ConsoleAccount) -> None:
        """The operator suspends another organization's sandbox from its page.

        The admin API then has it suspended and still owned by that
        organization and user, and the page shows the new state.
        """

        public_id = public_sandbox_id(sandbox_id)
        session.open(f"/dashboard/sandboxes/{public_id}")
        self.expect_page_resolved(session, f"/dashboard/sandboxes/{public_id}", shows=public_id, what="the foreign sandbox's page")
        page = session.page
        org_shown = self._visible(self._description(session, "Org").get_by_text(tenant.name, exact=True))
        self._fixture.verify(
            org_shown,
            "the foreign sandbox's page does not name its organization",
            console_expected_org=tenant.name,
            failure_evidence=self._evidence(session),
        )
        controls = page.locator(".ant-card", has=page.locator(".ant-card-head-title", has_text=re.compile(r"^Controls$")))
        _button(controls, "Suspend").click()
        record = self._await_status(public_id, "suspended")
        self._fixture.verify(
            record.get("org_id") == tenant.org_id and record.get("user_id") == owner.user_id,
            "suspending another organization's sandbox changed who owns it",
            observed_org_id=record.get("org_id"),
            observed_user_id=record.get("user_id"),
            expected_org_id=tenant.org_id,
            expected_user_id=owner.user_id,
        )
        shown = self._visible(page.locator(".ant-tag", has_text=re.compile(r"^suspended$")).first)
        self._fixture.verify(
            shown,
            "the foreign sandbox's page does not show it suspended",
            failure_evidence=self._evidence(session),
        )

    # --- confirmations ---------------------------------------------------------------

    @contextmanager
    def _cancelled_confirmation(self, session: ConsoleSession, what: str, dialog: Locator, cancel: Locator) -> Iterator[None]:
        """Open a confirmation (in the body), cancel it, and require it sent nothing."""

        before = len(session.writes)
        yield
        appeared = self._visible(dialog)
        self._fixture.verify(
            appeared,
            f"{what} took effect without asking for confirmation",
            failure_evidence=self._evidence(session),
        )
        cancel.click()
        closed = self._hidden(dialog)
        time.sleep(CANCEL_SETTLE_SECONDS)
        sent = session.writes[before:]
        self._fixture.verify(
            closed and not sent,
            f"cancelling the confirmation of {what} did not leave it undone",
            console_writes_after_cancel=sent,
            failure_evidence=self._evidence(session),
        )

    def expect_delete_requires_confirmation(self, session: ConsoleSession, sandbox_id: str) -> None:
        """Delete asks first; cancelled, nothing is sent and the sandbox stays; confirmed, it is deleted."""

        public_id = public_sandbox_id(sandbox_id)
        session.open(f"/dashboard/sandboxes/{public_id}")
        self.expect_page_resolved(session, f"/dashboard/sandboxes/{public_id}", shows=public_id, what="the sandbox's page")
        page = session.page
        controls = page.locator(".ant-card", has=page.locator(".ant-card-head-title", has_text=re.compile(r"^Controls$")))
        delete = _button(controls, "Delete")
        dialog = page.locator(".ant-popconfirm:visible", has_text="Delete this sandbox?")
        with self._cancelled_confirmation(
            session, "deleting the sandbox", dialog, _button(dialog, "Cancel")
        ):
            delete.click()
        record = self.admin_sandbox(public_id)
        self._fixture.verify(
            record is not None and record.get("status") == "started",
            "the sandbox changed although its delete was cancelled",
            observed_status=(record or {}).get("status"),
        )
        before = len(session.writes)
        delete.click()
        self._fixture.verify(self._visible(dialog), "delete did not ask for confirmation again", failure_evidence=self._evidence(session))
        _button(dialog, "OK").click()
        self._fixture.verify(
            self._await_gone(public_id),
            "the confirmed delete did not delete the sandbox",
            console_writes=session.writes[before:],
            failure_evidence=self._evidence(session),
        )

    def expect_role_change_requires_confirmation(self, session: ConsoleSession, account: ConsoleAccount, role: str) -> None:
        """A role change asks first; cancelled, the role is unchanged; confirmed, it is `role`."""

        session.open(f"/dashboard/users/{account.user_id}")
        self.expect_page_resolved(session, f"/dashboard/users/{account.user_id}", shows=account.email, what="the user's page")
        page = session.page
        label = ROLE_LABELS[role]
        select = self._description(session, "Role").locator(".ant-select")
        dialog = page.locator(".ant-modal-confirm:visible", has_text=f"Make {account.email} {label}?")

        def choose() -> None:
            select.click()
            page.locator(".ant-select-dropdown:visible .ant-select-item-option", has_text=re.compile(rf"^{re.escape(label)}$")).click()

        with self._cancelled_confirmation(
            session, "changing the user's role", dialog, _button(dialog, "Cancel")
        ):
            choose()
        current = self._api("GET", f"/admin/api/v1/users/{account.user_id}")
        self._fixture.verify(
            current.get("role") == account.role,
            "the user's role changed although the change was cancelled",
            observed_role=current.get("role"),
        )
        before = len(session.writes)
        choose()
        self._fixture.verify(self._visible(dialog), "the role change did not ask for confirmation again", failure_evidence=self._evidence(session))
        _button(dialog, "Change role").click()
        deadline = time.monotonic() + PAGE_TIMEOUT_MS / 1000
        while time.monotonic() < deadline:
            current = self._api("GET", f"/admin/api/v1/users/{account.user_id}")
            if current.get("role") == role:
                break
            time.sleep(0.5)
        self._fixture.verify(
            current.get("role") == role,
            "the confirmed role change did not change the role",
            observed_role=current.get("role"),
            console_writes=session.writes[before:],
            failure_evidence=self._evidence(session),
        )

    # --- role gating ---------------------------------------------------------------

    def expect_turned_away(self, session: ConsoleSession, sandbox_id: str) -> None:
        """A non-operator sees the not-for-you notice and no navigation, on the
        dashboard and on the sandbox's page, which does not show the sandbox."""

        hidden = public_sandbox_id(sandbox_id)
        for path in ("/dashboard", f"/dashboard/sandboxes/{hidden}"):
            session.open(path)
            page = session.page
            notice = self._visible(page.get_by_text(NOT_FOR_YOU, exact=True))
            menu = page.locator(".ant-menu-item").count()
            leaked = hidden in session.text()
            self._fixture.verify(
                notice and menu == 0 and not leaked,
                "the console did not turn a non-operator away",
                console_user=session.label,
                console_path=path,
                console_notice_shown=notice,
                console_menu_items=menu,
                console_leaked=leaked,
                failure_evidence=self._evidence(session),
            )

    def expect_admitted(self, session: ConsoleSession, sandbox_id: str) -> None:
        """The operator gets the navigation and the sandbox's page, and no not-for-you notice."""

        public_id = public_sandbox_id(sandbox_id)
        deep_link = f"/dashboard/sandboxes/{public_id}"
        session.open(deep_link)
        self.expect_page_resolved(session, deep_link, shows=public_id, what="the operator's deep link")
        menu = session.page.locator(".ant-menu-item").count()
        self._fixture.verify(
            menu == len(CONSOLE_MENU),
            "the operator does not get the console's navigation",
            console_menu_items=menu,
            failure_evidence=self._evidence(session),
        )

    # --- link integrity ---------------------------------------------------------------

    def follow(self, session: ConsoleSession, link: Locator, path: str, *, shows: str, what: str) -> None:
        """Click `link`, which must point at `path`, and require the page to resolve."""

        href = link.get_attribute("href") if self._visible(link) else None
        self._fixture.verify(
            href == path,
            f"{what} does not link where it should",
            console_link_href=href,
            console_expected_href=path,
            failure_evidence=self._evidence(session),
        )
        link.click()
        self.expect_page_resolved(session, path, shows=shows, what=what)

    def _linked(self, sandbox_id: str) -> tuple[str, dict[str, Any]]:
        public_id = public_sandbox_id(sandbox_id)
        record = self.admin_sandbox(public_id)
        if record is None:
            raise SmokeConfigurationError("the admin API no longer has the sandbox")
        return public_id, record

    def follow_list_to_detail(self, session: ConsoleSession, sandbox_id: str) -> None:
        """The sandbox list's link opens the sandbox's page."""

        public_id, _ = self._linked(sandbox_id)
        detail = f"/dashboard/sandboxes/{public_id}"
        session.open("/dashboard/sandboxes")
        session.page.get_by_placeholder("ID, pod name or name").fill(public_id)
        link = session.page.locator(f'.ant-table-tbody a[href="{detail}"]').first
        self.follow(session, link, detail, shows=public_id, what="the list's sandbox link")

    def follow_organization_links(self, session: ConsoleSession, sandbox_id: str) -> None:
        """The sandbox's organization link opens that organization, which links back."""

        public_id, record = self._linked(sandbox_id)
        detail = f"/dashboard/sandboxes/{public_id}"
        org = self._api("GET", f"/admin/api/v1/orgs/{record['org_id']}")
        org_page = f"/dashboard/orgs/{record['org_id']}"
        session.open(detail)
        self.expect_page_resolved(session, detail, shows=public_id, what="the sandbox's page")
        link = self._description(session, "Org").locator("a").first
        self.follow(session, link, org_page, shows=str(org["name"]), what="the sandbox's organization link")
        back = session.page.locator(f'.ant-card a[href="{detail}"]').first
        self.follow(session, back, detail, shows=public_id, what="the organization's sandbox link")

    def follow_owner_links(self, session: ConsoleSession, sandbox_id: str) -> None:
        """The sandbox's owner link opens that user, whose organization and sandbox links resolve."""

        public_id, record = self._linked(sandbox_id)
        detail = f"/dashboard/sandboxes/{public_id}"
        org = self._api("GET", f"/admin/api/v1/orgs/{record['org_id']}")
        owner = self._api("GET", f"/admin/api/v1/users/{record['user_id']}")
        user_page = f"/dashboard/users/{record['user_id']}"
        org_page = f"/dashboard/orgs/{record['org_id']}"
        session.open(detail)
        self.expect_page_resolved(session, detail, shows=public_id, what="the sandbox's page")
        link = self._description(session, "Owner").locator("a").first
        self.follow(session, link, user_page, shows=str(owner["email"]), what="the sandbox's owner link")
        org_link = self._description(session, "Organization").locator("a").first
        self.follow(session, org_link, org_page, shows=str(org["name"]), what="the owner's organization link")
        session.open(user_page)
        back = session.page.locator(f'.ant-card a[href="{detail}"]').first
        self.follow(session, back, detail, shows=public_id, what="the owner's sandbox link")

    def follow_node_links(self, session: ConsoleSession, sandbox_id: str) -> None:
        """The nodes list links to the sandbox's node, whose page links back to the sandbox."""

        public_id, record = self._linked(sandbox_id)
        detail = f"/dashboard/sandboxes/{public_id}"
        node = record.get("active_node")
        if not isinstance(node, dict) or not node.get("id"):
            raise SmokeConfigurationError("the running sandbox has no active node to follow")
        node_page = f"/dashboard/nodes/{node['id']}"
        node_name = str(node.get("name") or node.get("address") or node["id"])
        session.open("/dashboard/nodes")
        link = session.page.locator(f'a[href="{node_page}"]').first
        self.follow(session, link, node_page, shows=node_name, what="the nodes list's link to the sandbox's node")
        back = session.page.locator(f'.ant-card a[href="{detail}"]').first
        self.follow(session, back, detail, shows=public_id, what="the node's sandbox link")

    def expect_template_listed(self, session: ConsoleSession, sandbox_id: str) -> None:
        """The templates page lists the sandbox's template; main has no template page to link to."""

        _, record = self._linked(sandbox_id)
        template = str(record.get("template") or "")
        session.open("/dashboard/templates")
        listed = self._visible(session.page.get_by_text(template, exact=True).first)
        self._fixture.verify(
            bool(template) and listed,
            "the templates page does not list the sandbox's template",
            console_template=template,
            failure_evidence=self._evidence(session),
        )

    # --- browser errors ---------------------------------------------------------------

    def expect_no_new_browser_errors(self) -> None:
        """No page reported an error outside the baseline and this scenario's own probes."""

        unexpected = self.unexpected_events()
        self._fixture.record("console_browser_errors_allowed", self._allowed_counts())
        self._fixture.verify(
            not unexpected,
            "the console reported browser errors outside its baseline",
            console_unexpected_browser_errors=[event.describe() for event in unexpected][:50],
        )


# The lifecycle buttons on a sandbox's page, and the ones each state offers:
# SPACE's lifecycle vocabulary (stop/start, pause/resume, suspend/restore).
# Wake accepts any valid start, resume or restore path.
LIFECYCLE_MOVES = ("Start", "Wake", "Pause", "Resume", "Suspend", "Restore", "Stop")
LIFECYCLE_MOVES_FROM: dict[str, tuple[str, ...]] = {
    "started": ("Pause", "Suspend", "Stop"),
    "suspended": ("Wake", "Restore"),
}

ROLE_LABELS = {"member": "Member", "organization_admin": "Organization Admin"}

# The console's navigation, as an operator sees it.
CONSOLE_MENU = (
    "Overview", "Organizations", "Users", "API Keys", "Sandboxes",
    "Credential Rules", "Templates", "Nodes", "Operations", "Audit log",
)


def _button(scope: Locator, label: str) -> Locator:
    """The button whose text is `label`; its accessible name also carries its icon's."""

    return scope.locator("button", has_text=re.compile(rf"^\s*{re.escape(label)}\s*$")).first


def public_sandbox_id(sandbox_id: str) -> str:
    return f"sbx_{uuid.UUID(sandbox_id.removeprefix('sbx_'))}"


def _path(url: str) -> str:
    parts = urlsplit(url)
    return parts.path + (f"?{parts.query}" if parts.query else "")


def _xpath_literal(text: str) -> str:
    if "'" not in text:
        return f"'{text}'"
    return 'concat(' + ', "\'", '.join(f"'{part}'" for part in text.split("'")) + ')'
