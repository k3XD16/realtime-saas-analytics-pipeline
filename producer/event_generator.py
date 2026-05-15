import uuid
import random
from datetime import datetime
from typing import Dict, Any, Optional
from faker import Faker

fake = Faker()

class EventGenerator:
    """Generates fake SaaS events based on specified weights and schemas."""

    EVENT_TYPES = ["page_view", "signup", "login", "feature_used", "upgrade", "churn"]
    WEIGHTS     = [0.50,        0.08,     0.20,    0.15,           0.05,      0.02]

    PLANS    = ["free_trial", "pro", "enterprise"]
    DEVICES  = ["mobile", "desktop", "tablet"]
    BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
    OS_LIST  = ["Windows", "macOS", "iOS", "Android"]
    COUNTRIES = ["IN", "US", "GB", "DE", "SG"]
    CITIES = {
        "IN": ["Chennai", "Mumbai", "Bangalore", "Delhi"],
        "US": ["New York", "San Francisco", "Austin", "Seattle"],
        "GB": ["London", "Manchester", "Birmingham"],
        "DE": ["Berlin", "Munich", "Hamburg"],
        "SG": ["Singapore City"]
    }

    # ✅ FIX 1 — Active session pool: reuse session_ids across multiple events
    # Simulates real users who trigger 3–8 events per session
    _active_sessions: Dict[str, Dict] = {}
    _SESSION_POOL_SIZE = 200       # Max concurrent active sessions
    _SESSION_REUSE_PROB = 0.75     # 75% chance to reuse existing session

    @classmethod
    def _get_or_create_session(cls) -> Dict[str, str]:
        """
        Returns an existing active session (75% chance) or creates a new one.
        This ensures multiple events share the same session_id — mimicking
        real user behaviour where 1 session = 3–8 page interactions.
        """
        # Reuse existing session if pool has entries and probability hits
        if cls._active_sessions and random.random() < cls._SESSION_REUSE_PROB:
            session_key = random.choice(list(cls._active_sessions.keys()))
            return cls._active_sessions[session_key]

        # Create a new session
        new_session = {
            "session_id":   f"sess_{uuid.uuid4()}",
            "user_id":      f"u_{random.randint(10000, 99999)}",
            "anonymous_id": f"anon_{uuid.uuid4()}",
            "device":       random.choice(cls.DEVICES),
            "country":      random.choice(cls.COUNTRIES),
        }

        # Add to pool, evict oldest if pool is full
        if len(cls._active_sessions) >= cls._SESSION_POOL_SIZE:
            evict_key = next(iter(cls._active_sessions))
            del cls._active_sessions[evict_key]

        cls._active_sessions[new_session["session_id"]] = new_session
        return new_session

    @classmethod
    def generate_event(cls, event_type: Optional[str] = None) -> Dict[str, Any]:
        """Generates a single SaaS event with realistic session reuse."""
        if not event_type:
            event_type = random.choices(cls.EVENT_TYPES, weights=cls.WEIGHTS, k=1)[0]

        # ✅ FIX 2 — Reuse session context instead of generating new IDs every time
        session = cls._get_or_create_session()

        event_id     = f"evt_{uuid.uuid4()}"
        timestamp    = datetime.utcnow().isoformat() + "Z"  # historical_seed overrides this

        properties   = cls._generate_properties(event_type, session["country"])

        return {
            "event_id":     event_id,
            "event_type":   event_type,
            "timestamp":    timestamp,
            "session_id":   session["session_id"],    # ✅ shared across events
            "user_id":      session["user_id"] if event_type != "page_view" else None,
            "anonymous_id": session["anonymous_id"],  # ✅ consistent per session
            "properties":   properties
        }

    @classmethod
    def _generate_properties(cls, event_type: str, country: str = None) -> Dict[str, Any]:
        """Generates event-specific properties."""
        if not country:
            country = random.choice(cls.COUNTRIES)
        city = random.choice(cls.CITIES[country])

        props = {
            "page": None, "referrer": None, "utm_source": None, "utm_medium": None,
            "utm_campaign": None, "device": None, "browser": None, "os": None,
            "country": None, "city": None, "plan": None, "signup_method": None,
            "referral_source": None, "login_method": None, "session_number": None,
            "feature_name": None, "duration_seconds": None, "actions_taken": None,
            "from_plan": None, "to_plan": None, "mrr_usd": None, "payment_method": None,
            "billing_cycle": None, "reason": None, "days_active": None, "mrr_lost_usd": None
        }

        if event_type == "page_view":
            props.update({
                "page":         random.choice(["/home", "/pricing", "/features", "/docs", "/blog"]),
                "referrer":     random.choice(["google.com", "github.com", "twitter.com", "direct"]),
                "utm_source":   random.choice(["google", "newsletter", "social", "organic"]),
                "utm_medium":   random.choice(["cpc", "email", "referral"]),
                "utm_campaign": "spring_launch",
                "device":       random.choice(cls.DEVICES),
                "browser":      random.choice(cls.BROWSERS),
                "os":           random.choice(cls.OS_LIST),
                "country":      country,
                "city":         city
            })

        elif event_type == "signup":
            props.update({
                "plan":            "free_trial",
                "signup_method":   random.choice(["google_oauth", "email", "github"]),
                "device":          random.choice(cls.DEVICES[:2]),
                "country":         country,
                "referral_source": random.choice(["organic", "paid", "referral"])
            })

        elif event_type == "login":
            props.update({
                "login_method":   random.choice(["google_oauth", "email"]),
                "device":         random.choice(cls.DEVICES),
                "country":        country,
                "session_number": random.randint(1, 100)
            })

        elif event_type == "feature_used":
            props.update({
                "feature_name":    random.choice(["dashboard", "reports", "export", "api", "settings", "integrations"]),
                "duration_seconds": random.randint(10, 3600),
                "actions_taken":   random.randint(1, 50),
                "plan":            random.choice(cls.PLANS)
            })

        elif event_type == "upgrade":
            to_plan = random.choice(cls.PLANS[1:])
            mrr     = 49.00 if to_plan == "pro" else 199.00
            props.update({
                "from_plan":      "free_trial",
                "to_plan":        to_plan,
                "mrr_usd":        float(mrr),
                "payment_method": random.choice(["card", "paypal"]),
                "billing_cycle":  random.choice(["monthly", "annual"])
            })

        elif event_type == "churn":
            props.update({
                "plan":          random.choice(cls.PLANS[1:]),
                "reason":        random.choice(["too_expensive", "missing_features", "switching_product", "no_longer_needed"]),
                "days_active":   random.randint(1, 365),
                "mrr_lost_usd":  49.00
            })

        return props