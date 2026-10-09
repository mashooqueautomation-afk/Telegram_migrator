"""Neutral enums shared by the DB models, services and the Telegram layer."""
from enum import StrEnum


class ChannelKind(StrEnum):
    CHANNEL = "CHANNEL"        # broadcast channel
    SUPERGROUP = "SUPERGROUP"  # megagroup
    GROUP = "GROUP"            # legacy basic group


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class AnalysisState(StrEnum):
    NOT_ANALYZED = "NOT_ANALYZED"
    ANALYZING = "ANALYZING"
    COMPLETED = "COMPLETED"  # every listing Telegram gave us was complete
    PARTIAL = "PARTIAL"      # truncated / rate-limited: results are incomplete
    FAILED = "FAILED"


class MemberState(StrEnum):
    DISCOVERED = "DISCOVERED"
    ELIGIBLE = "ELIGIBLE"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    ALREADY_MEMBER = "ALREADY_MEMBER"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
    RATE_LIMITED = "RATE_LIMITED"


class Eligibility(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    ALREADY_IN_DESTINATION = "ALREADY_IN_DESTINATION"
    PRIVACY_RESTRICTED = "PRIVACY_RESTRICTED"  # only ever set after Telegram refuses an invite (Stage 2)
    DELETED_ACCOUNT = "DELETED_ACCOUNT"
    BOT = "BOT"
    RESTRICTED = "RESTRICTED"
    UNKNOWN = "UNKNOWN"


class AccountStatus(StrEnum):
    ACTIVE = "ACTIVE"
    NEEDS_REAUTH = "NEEDS_REAUTH"


class LoginState(StrEnum):
    AWAITING_CODE = "AWAITING_CODE"
    AWAITING_PASSWORD = "AWAITING_PASSWORD"


class PauseReason(StrEnum):
    USER = "USER"                # an administrator asked for it; never auto-resumes
    RATE_LIMIT = "RATE_LIMIT"    # Telegram FloodWait / account cooldown; auto-resumes after the cooldown
    ERRORS = "ERRORS"            # circuit breaker: too many consecutive transient failures


class InviteOutcome(StrEnum):
    """Classified result of one member's invitation attempt (mapping lives in app/telegram/errors.py)."""

    SUCCESS = "SUCCESS"                          # invite sent AND membership confirmed by Telegram
    ALREADY_MEMBER = "ALREADY_MEMBER"
    PRIVACY_RESTRICTED = "PRIVACY_RESTRICTED"    # incl. USER_NOT_MUTUAL_CONTACT
    INVALID_USER = "INVALID_USER"
    USER_RESTRICTED = "USER_RESTRICTED"          # banned/kicked in destination, in too many channels, blocked us
    ADMIN_REQUIRED = "ADMIN_REQUIRED"            # fatal for the job
    CHANNEL_RESTRICTED = "CHANNEL_RESTRICTED"    # fatal: private / invalid / full destination
    ACCOUNT_RESTRICTED = "ACCOUNT_RESTRICTED"    # fatal: Telegram restricted OUR account (PEER_FLOOD, spam ban)
    SESSION_INVALID = "SESSION_INVALID"          # fatal: login revoked/expired
    FLOOD_WAIT = "FLOOD_WAIT"                    # pauses the job until the cooldown elapses
    TEMPORARY_ERROR = "TEMPORARY_ERROR"          # network / server hiccup: retried with exponential backoff
    NOT_CONFIRMED = "NOT_CONFIRMED"              # request accepted but Telegram does not list the user as a member
    UNKNOWN_ERROR = "UNKNOWN_ERROR"              # never auto-retried; an admin can retry manually
