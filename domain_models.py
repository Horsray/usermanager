"""Domain models and helpers for user inventory and ledger records.

This module centralises the business rules that were previously spread across
multiple view functions.  It introduces explicit data structures for account
life-cycle management and transaction recording so that the rest of the
application can operate on a consistent contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
import uuid


# ---- Constants -----------------------------------------------------------------

AccountStatus = str
SaleType = Optional[str]
Role = str
TransactionDirection = str


ACCOUNT_STATUS_AGENT_STOCK: AccountStatus = "agent_stock"
ACCOUNT_STATUS_DISTRIBUTOR_STOCK: AccountStatus = "distributor_stock"
ACCOUNT_STATUS_SOLD: AccountStatus = "sold"

SALE_TYPE_DIRECT: str = "direct_sale"
SALE_TYPE_DISTRIBUTION: str = "distribution_sale"

ROLE_ADMIN: Role = "admin"
ROLE_AGENT: Role = "agent"
ROLE_DISTRIBUTOR: Role = "distributor"
ROLE_CUSTOMER: Role = "customer"

TRANSACTION_ADMIN_TO_AGENT: str = "admin_to_agent_wholesale"
TRANSACTION_AGENT_PURCHASE: str = "agent_purchase"
TRANSACTION_AGENT_DIRECT_SALE: str = "agent_direct_sale"
TRANSACTION_AGENT_TO_DISTRIBUTOR: str = "agent_to_distributor_wholesale"
TRANSACTION_DISTRIBUTOR_SALE: str = "distributor_sale"
TRANSACTION_ASSIGNMENT: str = "distribution_assignment"
TRANSACTION_LEGACY: str = "legacy"


# ---- Data classes ---------------------------------------------------------------

@dataclass
class AccountState:
    """Represents the state of an inventory account."""

    owner: Optional[str] = None
    manager: Optional[str] = None
    status: AccountStatus = ACCOUNT_STATUS_AGENT_STOCK
    sale_type: SaleType = None
    sold_at: Optional[str] = None

    def ensure_defaults(self) -> None:
        if not self.manager:
            self.manager = self.owner
        if self.status not in {
            ACCOUNT_STATUS_AGENT_STOCK,
            ACCOUNT_STATUS_DISTRIBUTOR_STOCK,
            ACCOUNT_STATUS_SOLD,
        }:
            self.status = ACCOUNT_STATUS_AGENT_STOCK
        if self.sale_type not in {SALE_TYPE_DIRECT, SALE_TYPE_DISTRIBUTION, None}:
            self.sale_type = None


@dataclass
class LedgerEntry:
    """Normalised ledger record."""

    id: str
    time: str
    actor: str
    actor_role: Role
    transaction_type: str
    direction: TransactionDirection
    amount: float
    quantity: int
    product: Optional[str] = None
    account_username: Optional[str] = None
    counterparty: Optional[str] = None
    sale_type: SaleType = None
    metadata: Dict[str, object] = field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: Dict[str, object]) -> "LedgerEntry":
        """Create a normalised ledger entry from legacy data."""

        def _get_amount() -> float:
            if "amount" in raw:
                try:
                    return float(raw.get("amount", 0) or 0)
                except (TypeError, ValueError):
                    return 0.0
            revenue = raw.get("revenue")
            price = raw.get("price")
            count = raw.get("count")
            try:
                if revenue is not None:
                    return float(revenue or 0)
            except (TypeError, ValueError):
                pass
            try:
                price_val = float(price or 0)
                count_val = int(count or 0) or 1
                return price_val * count_val
            except (TypeError, ValueError):
                return 0.0

        def _get_quantity() -> int:
            try:
                return int(raw.get("quantity", raw.get("count", 0)) or 0)
            except (TypeError, ValueError):
                return 0

        def _get_direction() -> TransactionDirection:
            direction = raw.get("direction")
            if direction in {"in", "out"}:
                return direction
            transaction_type = raw.get("transaction_type")
            if transaction_type == TRANSACTION_AGENT_PURCHASE:
                return "out"
            return "in"

        def _get_sale_type() -> SaleType:
            sale_type = raw.get("sale_type")
            if sale_type in {SALE_TYPE_DIRECT, SALE_TYPE_DISTRIBUTION, None}:
                return sale_type
            if sale_type in {"总部直销", "direct"}:
                return SALE_TYPE_DIRECT
            if sale_type in {"分销售出", "distribution"}:
                return SALE_TYPE_DISTRIBUTION
            return None

        actor = raw.get("actor") or raw.get("admin") or ""
        role = raw.get("actor_role") or raw.get("role") or ROLE_ADMIN
        time = raw.get("time") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = cls(
            id=str(raw.get("id") or uuid.uuid4()),
            time=str(time),
            actor=str(actor),
            actor_role=str(role),
            transaction_type=str(raw.get("transaction_type") or TRANSACTION_LEGACY),
            direction=_get_direction(),
            amount=_get_amount(),
            quantity=_get_quantity() or 0,
            product=raw.get("product"),
            account_username=raw.get("account_username") or raw.get("username"),
            counterparty=raw.get("counterparty") or raw.get("agent") or raw.get("distributor"),
            sale_type=_get_sale_type(),
            metadata=dict(raw.get("metadata", {})),
        )

        # keep backward compatible fields
        entry.metadata.setdefault("legacy_admin", raw.get("admin"))
        entry.metadata.setdefault("legacy_role", raw.get("role"))
        entry.metadata.setdefault("legacy_count", raw.get("count"))
        entry.metadata.setdefault("legacy_revenue", raw.get("revenue"))
        entry.metadata.setdefault("legacy_price", raw.get("price"))
        entry.metadata.setdefault("legacy_user_id", raw.get("user_id"))
        entry.metadata.setdefault("legacy_direction", raw.get("direction"))
        return entry

    def to_dict(self) -> Dict[str, object]:
        """Dump entry to dict while keeping legacy fields for compatibility."""

        price = 0.0
        if self.quantity:
            price = self.amount / self.quantity

        data: Dict[str, object] = {
            "id": self.id,
            "time": self.time,
            "actor": self.actor,
            "actor_role": self.actor_role,
            "transaction_type": self.transaction_type,
            "direction": self.direction,
            "amount": round(self.amount, 2),
            "quantity": self.quantity,
            "product": self.product,
            "account_username": self.account_username,
            "counterparty": self.counterparty,
            "sale_type": self.sale_type,
            "metadata": self.metadata,
            # legacy mirror fields ------------------------------------------
            "admin": self.actor,
            "role": self.actor_role,
            "price": round(price, 2),
            "count": self.quantity,
            "revenue": round(self.amount, 2),
        }
        if self.counterparty:
            if self.actor_role == ROLE_AGENT:
                data.setdefault("agent", self.actor)
            if self.actor_role == ROLE_ADMIN:
                data.setdefault("agent", self.counterparty)
        return data


# ---- User normalisation helpers -------------------------------------------------

def _is_inventory_record(data: Dict[str, object]) -> bool:
    if data.get("owner"):
        return True
    if any(
        key in data
        for key in (
            "forsale",
            "distribution_tag",
            "distributor_forsale",
            "assigned_distributor",
        )
    ):
        return True
    if data.get("source") in {"agent", "batch", "distribution", "admin_transfer"}:
        return True
    return False


def _normalise_roles(data: Dict[str, object]) -> None:
    roles = set(data.get("roles") or [])
    if data.get("is_admin"):
        roles.add(ROLE_ADMIN)
    if data.get("is_agent"):
        roles.add(ROLE_AGENT)
    if data.get("is_distributor"):
        roles.add(ROLE_DISTRIBUTOR)
    if not roles:
        roles.add(ROLE_CUSTOMER)

    data["roles"] = sorted(roles)
    data["is_admin"] = ROLE_ADMIN in roles
    data["is_agent"] = ROLE_AGENT in roles
    data["is_distributor"] = ROLE_DISTRIBUTOR in roles


def ensure_accounting(data: Dict[str, object]) -> AccountState:
    """Ensure that the record carries an AccountState (inventory only)."""

    state_data = data.setdefault("accounting", {})
    state = AccountState(
        owner=state_data.get("owner") or data.get("owner"),
        manager=state_data.get("manager")
        or data.get("manager")
        or data.get("assigned_distributor")
        or data.get("owner"),
        status=state_data.get("status") or data.get("status"),
        sale_type=state_data.get("sale_type") or data.get("sale_type"),
        sold_at=state_data.get("sold_at") or data.get("sold_at"),
    )

    state.ensure_defaults()

    if state.status is None:
        state.status = ACCOUNT_STATUS_AGENT_STOCK

    if not state.sale_type:
        sale_type = data.get("sale_type")
        if sale_type in {SALE_TYPE_DIRECT, "总部直销", "direct"}:
            state.sale_type = SALE_TYPE_DIRECT
        elif sale_type in {SALE_TYPE_DISTRIBUTION, "分销售出", "distribution"}:
            state.sale_type = SALE_TYPE_DISTRIBUTION

    if not state.status:
        if data.get("distributor_forsale"):
            state.status = ACCOUNT_STATUS_DISTRIBUTOR_STOCK
        elif data.get("forsale") in (False, 0, "false", "False"):
            state.status = ACCOUNT_STATUS_SOLD
        else:
            state.status = ACCOUNT_STATUS_AGENT_STOCK

    state.ensure_defaults()
    state_data.update(
        {
            "owner": state.owner,
            "manager": state.manager,
            "status": state.status,
            "sale_type": state.sale_type,
            "sold_at": state.sold_at,
        }
    )
    _sync_legacy_flags(data, state)
    return state


def _sync_legacy_flags(data: Dict[str, object], state: AccountState) -> None:
    """Mirror the new account state back to legacy fields for compatibility."""

    if ROLE_AGENT in data.get("roles", []) or ROLE_ADMIN in data.get("roles", []) or ROLE_DISTRIBUTOR in data.get("roles", []):
        # business accounts do not carry inventory status
        return

    data["owner"] = state.owner
    data["manager"] = state.manager
    if state.status == ACCOUNT_STATUS_AGENT_STOCK:
        data["forsale"] = True
        data["distribution_tag"] = False
        data["distributor_forsale"] = False
        data["distributor_sold"] = False
        data.pop("assigned_distributor", None)
    elif state.status == ACCOUNT_STATUS_DISTRIBUTOR_STOCK:
        data["forsale"] = False
        data["distribution_tag"] = True
        data["distributor_forsale"] = True
        data["distributor_sold"] = False
        data["assigned_distributor"] = state.manager
    else:
        data["forsale"] = False
        data["distribution_tag"] = bool(state.manager and state.manager != state.owner)
        data["distributor_forsale"] = False
        data["distributor_sold"] = state.sale_type == SALE_TYPE_DISTRIBUTION
        if state.manager and state.manager != state.owner:
            data["assigned_distributor"] = state.manager
        elif "assigned_distributor" in data:
            data.pop("assigned_distributor", None)

    if state.sale_type == SALE_TYPE_DIRECT:
        data["sale_type"] = "总部直销"
    elif state.sale_type == SALE_TYPE_DISTRIBUTION:
        data["sale_type"] = "分销售出"
    else:
        data.pop("sale_type", None)

    if state.sold_at:
        data["sold_at"] = state.sold_at


def update_account_state(
    data: Dict[str, object],
    *,
    owner: Optional[str] = None,
    manager: Optional[str] = None,
    status: Optional[AccountStatus] = None,
    sale_type: SaleType = None,
    sold_at: Optional[str] = None,
) -> AccountState:
    """Update the inventory state for the given record and mirror legacy fields."""

    if not _is_inventory_record(data):
        # no-op for non-inventory records
        _normalise_roles(data)
        return AccountState()

    state = ensure_accounting(data)
    if owner is not None:
        state.owner = owner
    if manager is not None:
        state.manager = manager
    if status is not None:
        state.status = status
    if sale_type is not None:
        state.sale_type = sale_type
    if sold_at is not None:
        state.sold_at = sold_at

    state.ensure_defaults()
    data.setdefault("accounting", {})
    data["accounting"].update(
        {
            "owner": state.owner,
            "manager": state.manager,
            "status": state.status,
            "sale_type": state.sale_type,
            "sold_at": state.sold_at,
        }
    )
    _sync_legacy_flags(data, state)
    return state


def normalize_user_record(username: str, data: Dict[str, object]) -> Dict[str, object]:
    """Normalise a single user record."""

    _normalise_roles(data)
    if _is_inventory_record(data):
        ensure_accounting(data)
    else:
        data.pop("accounting", None)
    return data


def normalize_user_store(users: Dict[str, Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    for username, record in list(users.items()):
        users[username] = normalize_user_record(username, record)
    return users


def iter_agent_inventory(users: Dict[str, Dict[str, object]], agent: str) -> Iterable[Tuple[str, Dict[str, object]]]:
    for username, record in users.items():
        state = record.get("accounting")
        if not state:
            continue
        if state.get("owner") == agent and state.get("status") == ACCOUNT_STATUS_AGENT_STOCK:
            yield username, record


def iter_distributor_inventory(users: Dict[str, Dict[str, object]], distributor: str) -> Iterable[Tuple[str, Dict[str, object]]]:
    for username, record in users.items():
        state = record.get("accounting")
        if not state:
            continue
        if state.get("owner") == distributor and state.get("status") == ACCOUNT_STATUS_DISTRIBUTOR_STOCK:
            yield username, record


def iter_managed_accounts(users: Dict[str, Dict[str, object]], manager: str) -> Iterable[Tuple[str, Dict[str, object]]]:
    for username, record in users.items():
        state = record.get("accounting")
        if not state:
            continue
        if state.get("manager") == manager and state.get("status") == ACCOUNT_STATUS_SOLD:
            yield username, record


# ---- Ledger helpers -------------------------------------------------------------

def normalize_ledger_records(records: List[Dict[str, object]]) -> List[Dict[str, object]]:
    normalised: List[Dict[str, object]] = []
    for raw in records:
        entry = LedgerEntry.from_raw(raw)
        normalised.append(entry.to_dict())
    return normalised


def build_ledger_entry(
    *,
    transaction_type: str,
    actor: str,
    actor_role: Role,
    amount: float,
    quantity: int = 1,
    product: Optional[str] = None,
    account_username: Optional[str] = None,
    counterparty: Optional[str] = None,
    sale_type: SaleType = None,
    direction: TransactionDirection = "in",
    occurred_at: Optional[str] = None,
    metadata: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    entry = LedgerEntry(
        id=str(uuid.uuid4()),
        time=occurred_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        actor=actor,
        actor_role=actor_role,
        transaction_type=transaction_type,
        direction=direction,
        amount=float(amount or 0),
        quantity=int(quantity or 0),
        product=product,
        account_username=account_username,
        counterparty=counterparty,
        sale_type=sale_type,
        metadata=metadata or {},
    )
    return entry.to_dict()


def record_transaction(records: List[Dict[str, object]], **kwargs) -> Dict[str, object]:
    entry = build_ledger_entry(**kwargs)
    records.append(entry)
    return entry


__all__ = [
    "ACCOUNT_STATUS_AGENT_STOCK",
    "ACCOUNT_STATUS_DISTRIBUTOR_STOCK",
    "ACCOUNT_STATUS_SOLD",
    "SALE_TYPE_DIRECT",
    "SALE_TYPE_DISTRIBUTION",
    "ROLE_ADMIN",
    "ROLE_AGENT",
    "ROLE_DISTRIBUTOR",
    "ROLE_CUSTOMER",
    "TRANSACTION_ADMIN_TO_AGENT",
    "TRANSACTION_AGENT_PURCHASE",
    "TRANSACTION_AGENT_DIRECT_SALE",
    "TRANSACTION_AGENT_TO_DISTRIBUTOR",
    "TRANSACTION_DISTRIBUTOR_SALE",
    "TRANSACTION_ASSIGNMENT",
    "TRANSACTION_LEGACY",
    "AccountState",
    "LedgerEntry",
    "ensure_accounting",
    "update_account_state",
    "normalize_user_record",
    "normalize_user_store",
    "iter_agent_inventory",
    "iter_distributor_inventory",
    "iter_managed_accounts",
    "normalize_ledger_records",
    "build_ledger_entry",
    "record_transaction",
]

