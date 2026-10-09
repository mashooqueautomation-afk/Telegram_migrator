"""Pure eligibility rules (no Telegram, no DB) so they are trivially unit-testable.

Honesty rule: ELIGIBLE means "no blocking condition was found in the data Telegram
gave us". It is NOT a promise the invite will succeed: Telegram never reveals
privacy settings in advance. Anything we cannot establish is UNKNOWN.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.domain import Eligibility, MemberState
from app.telegram.members import RawMember

ELIGIBLE_REASON = ("No blocking condition found. Telegram does not reveal a user's privacy settings in advance, "
                   "so an invitation may still be refused.")


@dataclass(frozen=True)
class DestinationSnapshot:
    member_ids: frozenset[int] = field(default_factory=frozenset)
    banned_ids: frozenset[int] = field(default_factory=frozenset)
    membership_complete: bool = False   # did we see the destination's FULL member list?
    banned_known: bool = False          # did we manage to read the destination's banned list?


@dataclass(frozen=True)
class Classification:
    eligibility: Eligibility
    reason: str


def classify(member: RawMember, dest: DestinationSnapshot) -> Classification:
    if member.telegram_user_id in dest.member_ids:
        return Classification(Eligibility.ALREADY_IN_DESTINATION, "Already a member of the destination.")
    if member.is_deleted:
        return Classification(Eligibility.DELETED_ACCOUNT, "Deleted Telegram account; cannot be invited.")
    if member.is_bot:
        return Classification(Eligibility.BOT, "Bots cannot be invited as members.")
    if member.is_restricted:
        return Classification(Eligibility.RESTRICTED, "Telegram has restricted this account.")
    if member.telegram_user_id in dest.banned_ids:
        return Classification(Eligibility.RESTRICTED, "Banned or restricted in the destination; an admin must lift that first.")
    if not member.has_usable_hash:
        return Classification(Eligibility.UNKNOWN, "Telegram returned only partial data for this user.")
    if not dest.membership_complete:
        return Classification(Eligibility.UNKNOWN,
                              "The destination's member list is incomplete, so we can't confirm this user "
                              "isn't already a member.")
    return Classification(Eligibility.ELIGIBLE, ELIGIBLE_REASON)


def initial_state_for(eligibility: Eligibility) -> MemberState:
    if eligibility is Eligibility.ELIGIBLE:
        return MemberState.ELIGIBLE
    if eligibility is Eligibility.UNKNOWN:
        return MemberState.DISCOVERED
    return MemberState.SKIPPED
